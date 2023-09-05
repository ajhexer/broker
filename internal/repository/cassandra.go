package repository

import (
	"broker/pkg/broker"
	"fmt"
	"github.com/gocql/gocql"
	_ "github.com/lib/pq"
	"log"
	"strconv"
	"sync"
	"time"
)

const (
	CASS_HOST     = "localhost"
	CASS_PORT     = "9042"
	CASS_USER     = "cassandra"
	CASS_PASSWORD = "cassandra"
)

type CassandraDatabase struct {
	client *gocql.Session
}

func (c *CassandraDatabase) Save(msg broker.Message, subject string) error {
	query := `INSERT INTO broker.messages (id, subject, body, expiration_date) VALUES (?, ?, ?, ?)`
	err := c.client.Query(query, msg.Id, subject, msg.Body, msg.Expiration).Exec()
	return err
}

func (c *CassandraDatabase) FetchMessage(id int, subject string) (broker.Message, error) {
	query := fmt.Sprintf("SELECT body, expiration_date from broker.messages where id=%d;", id)

	rows := c.client.Query(query).Iter()

	var body string

	var expirationDate int64

	ok := rows.Scan(&body, &expirationDate)
	if !ok {
		fmt.Println("fetch: scan error")
		return broker.Message{}, fmt.Errorf("scan error")
	}

	if body == "" {
		return broker.Message{}, fmt.Errorf("message not found")
	}

	msg := broker.Message{
		Body:       body,
		Expiration: time.Duration(expirationDate),
	}

	rows.Close()

	return msg, nil
}

func (c *CassandraDatabase) DeleteMessage(id int, subject string) error {
	query := `DELETE FROM broker.messages WHERE id=$1 AND subject=$2`
	err := c.client.Query(query, id, subject).Exec()
	return err
}

func (c *CassandraDatabase) DropAll() error {
	//TODO implement me
	panic("implement me")
}
func (c *CassandraDatabase) createTable() error {
	err := c.client.Query(`CREATE TABLE IF NOT EXISTS broker.messages (id int, subject text, body text, expiration_date bigint, PRIMARY KEY (id));`).Exec()
	if err != nil {
		log.Println(err)

		return err
	}

	var exists string

	_ = c.client.Query(`SELECT table_name FROM system_schema.tables WHERE keyspace_name='broker';`).Iter().Scan(&exists)

	err = c.client.Query(`CREATE TABLE IF NOT EXISTS broker.ids (id_name varchar, next_id int, PRIMARY KEY (id_name));`).Exec()
	if err != nil {
		log.Println(err)

		return err
	}

	if exists != "ids" {
		err = c.client.Query(`INSERT INTO broker.ids (id_name, next_id) VALUES ('messages_id', 1);`).Exec()
		if err != nil {
			log.Println(err)

			return err
		}

	} else {
		var lastID int
		ok := c.client.Query(`SELECT next_id FROM broker.ids WHERE id_name='messages_id';`).Iter().Scan(&lastID)
		if !ok {
			log.Println("error getting last id")

			return fmt.Errorf("error getting last id")
		}
	}

	return nil
}

func NewCassandra() (SecondaryDB, error) {
	var once sync.Once
	var connectionError error
	cassandraDB := new(CassandraDatabase)

	once.Do(func() {
		cluster := gocql.NewCluster(CASS_HOST)
		cluster.Port, _ = strconv.Atoi(CASS_PORT)
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: CASS_USER,
			Password: CASS_PASSWORD,
		}
		cluster.Consistency = gocql.Quorum
		cluster.Timeout = time.Second * 1000
		session, err := cluster.CreateSession()
		if err != nil {
			log.Fatal(err)
		}

		if err := session.Query(`CREATE KEYSPACE IF NOT EXISTS broker WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`).Exec(); err != nil {
			log.Fatal(err)
		}

		*cassandraDB = CassandraDatabase{
			client: session,
		}

		err = cassandraDB.createTable()
		if err != nil {
			connectionError = err

			return
		}
	})
	return cassandraDB, connectionError

}
