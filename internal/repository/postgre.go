package repository

import (
	"broker/pkg/broker"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"strings"
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
	batchHandler *BatchHandler
}
type BatchHandler struct {
	deleteMessages []string
	insertMessages []BatchLog
	client         *sql.DB
	deleteLock     sync.RWMutex
	insertLock     sync.RWMutex
	ticker         *time.Ticker
}
type BatchLog struct {
	msg     broker.Message
	subject string
}

func (b *BatchHandler) batchInsert() error {
	if len(b.insertMessages) != 0 {
		query := `INSERT INTO messages(id, subject, body, expiration_date) VALUES `
		b.insertLock.RLock()
		for _, bl := range b.insertMessages {
			query += fmt.Sprintf("(%d, '%s', '%s', %v),", bl.msg.Id, bl.subject, bl.msg.Body, int64(bl.msg.Expiration))
			log.Println(bl.msg.Id)
		}
		b.insertLock.RUnlock()
		query = query[:len(query)-1] + ";"
		b.insertMessages = b.insertMessages[:0]

		_, err := b.client.Exec(query)
		if err != nil {
			fmt.Println(err)
		}
		return err
	}
	return nil
}
func (b *BatchHandler) batchDelete() error {
	b.deleteLock.RLock()
	query := `DELETE FROM messages WHERE ` + strings.Join(b.deleteMessages, " or ") + ";"
	b.deleteLock.RUnlock()
	b.deleteMessages = b.deleteMessages[:0]

	_, err := b.client.Exec(query)
	if err != nil {
		fmt.Println(err)
	}
	return err
}
func (b *BatchHandler) insert(msg broker.Message, subject string) error {

	b.insertLock.Lock()
	b.insertMessages = append(b.insertMessages, BatchLog{msg: msg, subject: subject})
	b.insertLock.Unlock()
	return nil
}

func (b *BatchHandler) delete(id int, subject string) error {

	b.deleteLock.Lock()
	b.deleteMessages = append(b.deleteMessages, fmt.Sprintf("(id,subject)=(%d,'%s')", id, subject))
	b.deleteLock.Unlock()
	return nil
}

func (b *BatchHandler) start() {
	for range b.ticker.C {
		go func() {
			b.insertLock.RLock()
			if len(b.insertMessages) != 0 {
				err := b.batchInsert()
				if err != nil {
					log.Println(err)
				}
			}
			b.insertLock.RUnlock()
		}()
		go func() {
			b.deleteLock.RLock()
			if len(b.deleteMessages) != 0 {
				err := b.batchDelete()
				if err != nil {
					log.Println(err)
				}
			}
			b.deleteLock.RUnlock()
		}()
	}

}

func (db *PostgresDatabase) Save(msg broker.Message, subject string) error {
	return db.batchHandler.insert(msg, subject)
}

func (db *PostgresDatabase) FetchMessage(id int, subject string) (broker.Message, error) {
	var expiration int64
	var body string

	err := db.batchHandler.client.QueryRow(`SELECT body, expiration_date FROM messages WHERE id=$1 AND subject=$2`, id, subject).Scan(&body, &expiration)

	if err != nil {
		return broker.Message{}, err
	}
	return broker.Message{
		Body:       body,
		Expiration: time.Duration(expiration),
	}, nil
}

func (db *PostgresDatabase) DeleteMessage(id int, subject string) error {
	return db.batchHandler.delete(id, subject)
}

func (db *PostgresDatabase) DropAll() error {
	_, err := db.batchHandler.client.Exec(`TRUNCATE messages`)
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

	_, err := db.batchHandler.client.Exec(table)
	if err != nil {
		return err
	}

	return nil
}

func (db *PostgresDatabase) createIndex() error {
	command := `CREATE INDEX IF NOT EXISTS idx_id_subject on messages (id,subject)`

	_, err := db.batchHandler.client.Exec(command)
	if err != nil {
		return err
	}

	return nil

}

func NewPostgres() (*PostgresDatabase, error) {
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
			batchHandler: &BatchHandler{
				client:         client,
				insertMessages: make([]BatchLog, 0),
				deleteMessages: make([]string, 0),
				deleteLock:     sync.RWMutex{},
				insertLock:     sync.RWMutex{},
				ticker:         time.NewTicker(10 * time.Millisecond),
			},
		}
		err = postgresDatabase.createTable()
		if err != nil {
			connectionError = err
			return
		}

	})
	return postgresDatabase, connectionError
}
