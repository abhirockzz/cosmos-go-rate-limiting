package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/abhirockzz/cosmos-cassandra-go-extension/retry"
	"github.com/gocql/gocql"
	"github.com/hashicorp/go-uuid"
)

var (
	// connection
	cosmosCassandraContactPoint string
	cosmosCassandraUser         string
	cosmosCassandraPassword     string

	keyspace string
	table    string

	cs          *gocql.Session
	insertQuery string

	useRetryPolicy bool
)

const insertQueryFormat = "insert into %s.%s (id,amount,state,time) values (?,?,?,?)"

func init() {
	cosmosCassandraContactPoint = os.Getenv("COSMOSDB_CASSANDRA_CONTACT_POINT")
	cosmosCassandraUser = os.Getenv("COSMOSDB_CASSANDRA_USER")
	cosmosCassandraPassword = os.Getenv("COSMOSDB_CASSANDRA_PASSWORD")
	keyspace = os.Getenv("COSMOSDB_CASSANDRA_KEYSPACE")
	table = os.Getenv("COSMOSDB_CASSANDRA_TABLE")

	if cosmosCassandraContactPoint == "" || cosmosCassandraUser == "" || cosmosCassandraPassword == "" {
		log.Fatal("missing mandatory environment variables")
	}

	useRetryEnvVar := os.Getenv("USE_RETRY_POLICY")
	if useRetryEnvVar == "" {
		useRetryEnvVar = "true"
	}

	var err error
	useRetryPolicy, err = strconv.ParseBool(useRetryEnvVar)
	if err != nil {
		log.Fatal(err)
	}

	clusterConfig := gocql.NewCluster(cosmosCassandraContactPoint)
	clusterConfig.Port = 10350
	clusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: cosmosCassandraUser, Password: cosmosCassandraPassword}
	clusterConfig.SslOpts = &gocql.SslOptions{Config: &tls.Config{MinVersion: tls.VersionTLS12}}

	//log.Println("USE_RETRY_POLICY ==", useRetryPolicy)
	if useRetryPolicy {
		maxRetries := os.Getenv("MAX_RETRIES")
		if maxRetries == "" {
			maxRetries = "5"
		}
		//log.Println("MAX_RETRIES ==", maxRetries)
		numRetries, err := strconv.Atoi(maxRetries)
		if err != nil {
			log.Fatal(err)
		}
		clusterConfig.RetryPolicy = retry.NewCosmosRetryPolicy(numRetries)
	}

	clusterConfig.ProtoVersion = 4
	clusterConfig.ConnectTimeout = 3 * time.Second
	clusterConfig.Timeout = 3 * time.Second

	cs, err = clusterConfig.CreateSession()
	if err != nil {
		log.Fatal("Failed to connect to Azure Cosmos DB", err)
	}
	log.Print("Connected to Azure Cosmos DB")

	insertQuery = fmt.Sprintf(insertQueryFormat, keyspace, table)
}

func main() {
	http.HandleFunc("/orders", Add)
	s := http.Server{Addr: ":8080", Handler: nil}

	go func() {
		log.Fatal(s.ListenAndServe())
	}()

	exit := make(chan os.Signal)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	<-exit
	if cs != nil {
		cs.Close()
		log.Print("Session closed. Disconnected from Cosmos DB")
	}
}

const fixedLocation = "Seattle"

// Add adds a new record in Cosmos DB table
func Add(rw http.ResponseWriter, req *http.Request) {

	rid, _ := uuid.GenerateUUID()

	err := cs.Query(insertQuery).Bind(rid, rand.Intn(200)+50, fixedLocation, time.Now()).Observer(OrderInsertErrorLogger{orderID: rid}).Exec()

	if err != nil {

		respStatus := http.StatusInternalServerError
		if strings.Contains(err.Error(), "TooManyRequests (429)") {
			respStatus = http.StatusTooManyRequests
		} else if strings.Contains(err.Error(), "timeout") {
			respStatus = http.StatusRequestTimeout
		}

		http.Error(rw, err.Error(), respStatus)
		return
	}
	log.Println("Added order ID", rid)
}

// OrderInsertErrorLogger is a gocql.Observer that logs query error
type OrderInsertErrorLogger struct {
	orderID string
}

// ObserveQuery logs query error
func (l OrderInsertErrorLogger) ObserveQuery(ctx context.Context, oq gocql.ObservedQuery) {
	err := oq.Err
	if err != nil {
		log.Printf("Query error for order ID %s\n%v", l.orderID, err)
		if oq.Attempt > 0 {
			log.Printf("Order %s is being retried. attempt #%v", l.orderID, oq.Attempt)
		}
	}
}
