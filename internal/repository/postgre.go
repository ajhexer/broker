package repository

import (
	"broker/pkg/broker"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"sync"
	"time"
)

const (
	PG_HOST     = "localhost"
	PG_PORT     = "5432"
	PG_USER     = "postgres"
	PG_PASSWORD = "postgres"
	PG_NAME     = "broker"
)

type PostgresDatabase struct {
	client *sql.DB
}

func (db *PostgresDatabase) Save(msg broker.Message, subject string) error {
	_, err := db.client.Exec(`INSERT INTO messages(id, subject, body, expiration_date) VALUES ($1, $2, $3, $4)`, msg.Id, subject, msg.Body, msg.Expiration)
	return err
}

func (db *PostgresDatabase) FetchMessage(id int, subject string) (broker.Message, error) {
	var expiration int64
	var body string

	err := db.client.QueryRow(`SELECT body, expiration_date FROM messages WHERE id=$1 AND subject=$2`, id, subject).Scan(&body, &expiration)

	if err != nil {
		return broker.Message{}, err
	}
	return broker.Message{
		Body:       body,
		Expiration: time.Duration(expiration),
	}, nil
}

func (db *PostgresDatabase) DeleteMessage(id int, subject string) error {
	_, err := db.client.Exec(`DELETE FROM messages WHERE id=$1 AND subject=$2`, id, subject)
	return err
}

func (db *PostgresDatabase) DropAll() error {
	_, err := db.client.Exec(`TRUNCATE messages`)
	return err
}

func (db *PostgresDatabase) createTable() error {
	table := `
	CREATE TABLE IF NOT EXISTS messages (
		id serial,
		subject varchar(255) not null,
		body varchar(255) ,
		expiration_date bigint not null,
		primary key(id, subject)
	);`

	_, err := db.client.Exec(table)
	if err != nil {
		return err
	}

	return nil
}

func (db *PostgresDatabase) createIndex() error {
	command := `CREATE INDEX IF NOT EXISTS idx_id_subject on messages (id,subject)`

	_, err := db.client.Exec(command)
	if err != nil {
		return err
	}

	return nil

}

func NewPostgres(databaseUrl string) (*PostgresDatabase, error) {
	var once sync.Once
	var connectionError error
	postgresDatabase := new(PostgresDatabase)
	once.Do(func() {
		connString := fmt.Sprintf("host=%s port=%s user=%s password=%s sslmode=disable",
			PG_HOST, PG_PORT, PG_USER, PG_PASSWORD)
		client, err := sql.Open("postgres", connString)

		if err != nil {
			connectionError = err
			return
		}
		err = client.Ping()
		if err != nil {
			_, err = client.Exec("create database " + PG_NAME)
			if err != nil {
				log.Fatal(err)
			}
		}
		client.SetMaxOpenConns(100)
		client.SetMaxIdleConns(50)
		client.SetConnMaxIdleTime(time.Second * 10)

		*postgresDatabase = PostgresDatabase{
			client: client,
		}
		err = postgresDatabase.createTable()
		if err != nil {
			connectionError = err
			return
		}

	})
	return postgresDatabase, connectionError
}
