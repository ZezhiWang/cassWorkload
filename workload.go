package main 

import(
	"os"
	"strconv"
	"fmt"
	"time"
	"log"
	"sync"
//	"math/rand"
	// go get github.com/gocql/gocql
	"github.com/gocql/gocql"
)

// initialize mutex lock
var mutex = &sync.Mutex{}

// write info into table before read, with out tracking time
func initWrite(num int, session *gocql.Session){
	// write data in the form (string,blob) into table tmp
	arg := fmt.Sprintf("INSERT INTO tmp (key,val) values (?, ?)")
	if err := session.Query(arg, string(num),make([]byte, 4)).Exec(); err != nil {
		log.Fatal(err)
	}
}

// write info into table
func write(num int, session *gocql.Session, wTime chan time.Duration){
	// write data in the form (int, string) into table tmp
	arg := fmt.Sprintf("INSERT INTO tmp (key,val) values (?, ?)")
	mutex.Lock()
	start := time.Now()
	if err := session.Query(arg,string(num),make([]byte, 4)).Exec(); err != nil {
		log.Fatal(err)
	}
	end := time.Now()
	mutex.Unlock()
	elapsed := end.Sub(start)
	// send elapsed time to main thread
	wTime <- elapsed
}

// read info from table by key
func read(num int, session *gocql.Session, rTime chan time.Duration){
	var key string
	var val []byte
	// write data in the form table tmp with key = num
	arg := fmt.Sprintf("SELECT key,val FROM tmp WHERE key='%s'",string(num))
	mutex.Lock()
	start := time.Now()
	if err := session.Query(arg).Scan(&key,&val); err != nil {
		log.Fatal(err)
	}
	end := time.Now()
	mutex.Unlock()
	elapsed := end.Sub(start)
	// send elapsed time to main thread
	rTime <- elapsed
}

// delete value by key from table
func delete(num int, session *gocql.Session){
	arg := fmt.Sprintf("DELETE FROM tmp WHERE key='%s'",string(num))
	if err := session.Query(arg).Exec(); err != nil {
		log.Fatal(err)
	}
}

func main(){
	// command line arg -> number of read & write
	num,_ := strconv.Atoi(os.Args[1])
	// init cluster
	cluster := gocql.NewCluster("172.17.0.4")
	// set keyspace to demo
	cluster.Keyspace = "demo"
	session,_ := cluster.CreateSession()
	defer session.Close()

	wTime := make(chan time.Duration)
	rTime := make(chan time.Duration)
	var WTotal, RTotal int = 0, 0

	// insert value into table before start testing
	for i := 0; i < num; i++{
		initWrite(i, session)
	}

	// go routine for concurrent read & write
	for i := 0; i < num; i++{
		go write(i, session, wTime)
		go read(i, session, rTime)
	}

	// retrieve elapsed time
	for i := 0; i < num; i++{
		WTotal += int(<-wTime/time.Millisecond)
		RTotal += int(<-rTime/time.Millisecond)
	}

	// clear table
	for i := 0; i < num; i++{
		delete(i, session)
	}

	fmt.Printf("Avg write time: %f ms\n", float64(WTotal)/float64(num))
	fmt.Printf("Avg read time: %f ms\n", float64(RTotal)/float64(num))
}
