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
	go func() {
		err := c.client.Query(query, msg.Id, subject, msg.Body, msg.Expiration).Exec()
		if err != nil {
			log.Println(err)
		}
	}()
	return nil
}

func (c *CassandraDatabase) FetchMessage(id int, subject string) (broker.Message, error) {
	query := fmt.Sprintf("SELECT body, expiration_date from broker.messages where id=%d; and subject=%d", id, subject)

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

type BatchDaemon struct {
	batch *gocql.Batch

	tickerDuration time.Duration
	ticker         *time.Ticker
	innerMutex     *sync.Mutex
}

type CassandraDatabaseWithBatch struct {
	sync.Mutex
	client         *gocql.Session
	deleteMessages []string
	bd             *BatchDaemon
	semaphore      chan int
}

const MAX_BATCH_SIZE = 20

func (db *CassandraDatabaseWithBatch) NewBatchDaemon() *BatchDaemon {
	ans := &BatchDaemon{
		batch:          db.client.NewBatch(gocql.LoggedBatch),
		tickerDuration: time.Millisecond * 100,
		innerMutex:     &sync.Mutex{},
	}
	ans.ticker = time.NewTicker(ans.tickerDuration)

	return ans
}

func (db *CassandraDatabaseWithBatch) AddQuery(stmt string, args ...interface{}) {
	db.Lock()
	defer db.Unlock()

	db.bd.batch.Query(stmt, args...)

	if db.bd.batch.Size() > MAX_BATCH_SIZE {
		db.Execute()
	}

}

func (db *CassandraDatabaseWithBatch) Execute() {

	if db.bd.batch.Size() == 0 {
		return
	}
	var batch *gocql.Batch
	batch = db.bd.batch
	go func(batch *gocql.Batch, semaphore chan int) {
		semaphore <- 1

		err := db.client.ExecuteBatch(batch)
		if err != nil {
			log.Println("error on batch execution: %v\n", err)

		}
		<-semaphore
	}(batch, db.semaphore)

	db.bd.batch = db.client.NewBatch(gocql.LoggedBatch)

}

func (db *CassandraDatabaseWithBatch) createTable() error {
	err := db.client.Query(`CREATE TABLE IF NOT EXISTS broker.messages (id int, subject text, body text, expiration_date bigint, PRIMARY KEY (id));`).Exec()
	if err != nil {
		log.Println(err)

		return err
	}

	var exists string

	_ = db.client.Query(`SELECT table_name FROM system_schema.tables WHERE keyspace_name='broker';`).Iter().Scan(&exists)

	err = db.client.Query(`CREATE TABLE IF NOT EXISTS broker.ids (id_name varchar, next_id int, PRIMARY KEY (id_name));`).Exec()
	if err != nil {
		log.Println(err)

		return err
	}

	if exists != "ids" {
		err = db.client.Query(`INSERT INTO broker.ids (id_name, next_id) VALUES ('messages_id', 1);`).Exec()
		if err != nil {
			log.Println(err)

			return err
		}

	} else {
		var lastID int
		ok := db.client.Query(`SELECT next_id FROM broker.ids WHERE id_name='messages_id';`).Iter().Scan(&lastID)
		if !ok {
			log.Println("error getting last id")

			return fmt.Errorf("error getting last id")
		}
	}

	return nil
}

func (db *CassandraDatabaseWithBatch) Save(msg broker.Message, subject string) error {

	query := `INSERT INTO broker.messages (id, subject, body, expiration_date) VALUES (?, ?, ?, ?)`

	db.AddQuery(query, msg.Id, subject, msg.Body, msg.Expiration)

	return nil
}

func (db *CassandraDatabaseWithBatch) FetchMessage(id int, subject string) (broker.Message, error) {
	query := fmt.Sprintf("SELECT body, expiration_date from broker.messages where id=%d and subject=%d;", id, subject)

	rows := db.client.Query(query).Iter()
	// if err != nil {
	// 	fmt.Println("fetch: returned from query")
	// 	return broker.Message{}, err
	// }

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

func (db *CassandraDatabaseWithBatch) DeleteMessage(id int, subject string) error {

	query := `DELETE FROM broker.messages WHERE id=? AND subject=?`
	db.AddQuery(query, id, subject)
	return nil
}

func (db *CassandraDatabaseWithBatch) batchHandler(ticker *time.Ticker) {

	for range ticker.C {
		db.Lock()
		db.Execute()
		db.Unlock()
	}
}

func GetCassandra() (SecondaryDB, error) {
	var once sync.Once
	var connectionError error
	cassandraDB := new(CassandraDatabaseWithBatch)

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

		cassandraDB = &CassandraDatabaseWithBatch{
			client:         session,
			deleteMessages: make([]string, 0),
			semaphore:      make(chan int, 10),
		}

		err = cassandraDB.createTable()
		if err != nil {
			connectionError = err

			return
		}

		cassandraDB.bd = cassandraDB.NewBatchDaemon()

		ticker := time.NewTicker(100 * time.Microsecond)

		go cassandraDB.batchHandler(ticker)
	})

	return cassandraDB, connectionError
}

func (c *CassandraDatabaseWithBatch) DropAll() error {
	//TODO implement me
	panic("implement me")
}
