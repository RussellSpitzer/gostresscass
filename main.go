package main

import (
	"flag"
	"fmt"
	"log"
	"tux21b.org/v1/gocql"
)

func main() {
	// Command line Args
	var ip = flag.String("c", "127.0.0.1", "IP address of Cluster")
	var ks = flag.String("k", "stress_keyspace", "Keyspace for stress (will be created if it does not exist)")
	var table = flag.String("t", "stress_table", "Table for stress (will be created if it does not exist)")
	var num_ops = flag.Int("n", 1000000, "Number of operations to preform")
	var parallel = flag.Int("p", 40, "Number of Parallel Operations")
	flag.Parse()
	fmt.Println("IP:", *ip)

	// connect to the cluster
	cluster := gocql.NewCluster(*ip)
	session, err := cluster.CreateSession()
	if err != nil {
		panic(fmt.Sprintf("Could not create Session: %v", err))
	}
	defer session.Close()

	createks(session, *ks, *table)

	fmt.Println("Queing up Operations")
	go stress(*num_ops)
	for p := 0; p < *parallel; p++ {
		go insert(session)
	}
	// start selectors put a for loop here
	//go select(session, ops_select, done)
	c_done := 0
	for x := range done {
		fmt.Println(x)
		c_done++
		fmt.Println(c_done)
		if c_done == *parallel {
			close(done)
		}
	}

}

var ops_insert = make(chan []int, 2000)
var ops_select = make(chan []int, 2000)
var done = make(chan bool)

func stress(num_ops int) {
	for op_ind := 0; op_ind < num_ops; op_ind++ {
		ops_insert <- []int{op_ind, 2, 3, 4, 5, 6}
		if op_ind%10000 == 0 {
			fmt.Println(op_ind)
		}
	}
	close(ops_insert)

}

func insert(session *gocql.Session) {
	query := session.Query(`INSERT INTO %s (key1,key2,key3,col1,col2,col3) VALUES (?,?,?,?,?,?)`)
	for value := range ops_insert {
		query.Scan(value)
	}
	done <- true

}

func createks(session *gocql.Session, ks string, table string) {

	if err := session.Query(fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };`, ks)).Exec(); err != nil {
		log.Fatalf("Could not create keyspace %s : %s", ks, err)
	} else {
		log.Printf("Success::Keyspace %s was created or already exists", ks)
	}

	session.Query(fmt.Sprintf("use %s;", ks)).Exec()

	if err := session.Query(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s ( key1 int, key2 int, key3 int, col1 int, col2 int, col3 int, PRIMARY KEY (key1,key2,key3));`, table)).Exec(); err != nil {
		log.Fatalf("Could not create table %s : %s", table, err)
	} else {
		log.Printf("Success::Table %s was created or already exists", table)
	}

}
