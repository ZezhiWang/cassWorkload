package main 

import(
	"encoding/csv"
	"fmt"
	"github.com/montanaflynn/stats"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
	"sync"
	"github.com/gocql/gocql"
)

var numOperations float64= 10000;
var  session *gocql.Session;

// initialize mutex lock
var mutex = &sync.Mutex{}

// write info into table before read, with out tracking time
func initWrite(num int,dataSize int){
	// write data in the form (string,blob) into table tmp
	arg := fmt.Sprintf("INSERT INTO usertable (y_id,field0) values (?, ?)")
	if err := session.Query(arg, string(num),make([]byte, dataSize)).Exec(); err != nil {
//		log.Fatal(err)
	}
}

// write info into table
func write(numKey int,dataSize int,wTime chan time.Duration){
	// write data in the form (int, string) into table tmp
	arg := fmt.Sprintf("UPDATE usertable SET field0=? WHERE y_id=?")
	key:= rand.Int() % numKey
	mutex.Lock()
	start := time.Now()
	if err := session.Query(arg,string(key),make([]byte, dataSize)).Exec(); err != nil {
		//log.Fatal(err)
	}
	end := time.Now()
	mutex.Unlock()
	elapsed := end.Sub(start)
	// send elapsed time to main thread
	wTime <- elapsed
}

// read info from table by key
func read(numKey int, rTime chan time.Duration){
	var key string
	var val string
	cassKey:= rand.Int() % numKey;
	// write data in the form table tmp with key = num
	arg := fmt.Sprintf("SELECT y_id,field0 FROM usertable WHERE y_id=?")
	mutex.Lock()
	start := time.Now()
	if err := session.Query(arg,string(cassKey)).Scan(&key,&val); err != nil {
		//log.Fatal(err)
	}
	end := time.Now()
	mutex.Unlock()
	elapsed := end.Sub(start)
	// send elapsed time to main thread
	rTime <- elapsed
}

// delete value by key from table
func delete(num int){
	arg := fmt.Sprintf("DELETE FROM usertable WHERE y_id=?")
	if err := session.Query(arg,string(num)).Exec(); err != nil {
		//log.Fatal(err)
	}
}

func runRound(writeReadFraction float64,numKey int,dataSize int)([]float64,[]float64,float64) {
	wTime := make(chan time.Duration)
	rTime := make(chan time.Duration)
	numWrites,numReads := 0,0

	var writeDurations []float64
	var readDurations []float64
	// insert value into table before start testing
	for i := 0; i < numKey; i++{
		initWrite(numKey,dataSize)
	}
	// go routine for concurrent read & write
	for i := 0; i < int(numOperations); i++{
		if(rand.Float64()>writeReadFraction){
			go write(numKey,dataSize,wTime)
			numWrites++
		} else{
			go read(numKey,rTime)
			numReads++
		}
	}
	startTime := time.Now();
	// retrieve elapsed time
	for i := 0; i <numWrites; i++{
		writeDurations = append(writeDurations,float64(<-wTime/time.Millisecond))
	}
	for i := 0; i <numReads; i++{
		readDurations = append(readDurations,float64(<-rTime/time.Millisecond))
	}
	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	totalTime := float64(elapsedTime/time.Millisecond);
	// clear table
	for i := 0; i < numKey; i++{
		delete(i)
	}
	return writeDurations,readDurations,totalTime;
}

func FloatToString(input_num float64) string {
	// to convert a float number to a string
	return strconv.FormatFloat(input_num, 'f', 4, 64)
}

func main(){
	// init cluster
	cluster := gocql.NewCluster("10.142.0.2")
	// set keyspace to demo
	cluster.Keyspace = "ycsb"
	var err error;
	session, err = cluster.CreateSession()
	if err != nil {
		fmt.Println(err)
		fmt.Println(session)
	}
	defer session.Close()
	writeReadFractions := [5]float64{.1,.3,.5,.7,.9}
	numKeys := [6]int{1,2,4,8,16,32}
	dataSizes:=[6]int{8,16,32,64,128,256}

	file, err := os.Create("result.csv")
	if err != nil {
		log.Fatal("Cannot create file", err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	var records [][]string;
	colNames := []string{"write_read_fraction","num_key","data_size","write_average","read_average","ninety_five_write","ninety_five_read","total_time"}
	records = append(records,colNames)
	for _, writeReadFraction := range writeReadFractions {
		for _, numKey := range numKeys {
			for _, dataSize := range dataSizes {
				writeDurations,readDurations,totalTime :=runRound(writeReadFraction,numKey,dataSize)
				writeAverage,_:=stats.Mean(writeDurations)
				readAverage,_:=stats.Mean(readDurations)
				fmt.Printf("Avg write time: %f ms\n",writeAverage)
				fmt.Printf("Avg read time: %f ms\n", readAverage)
				ninetyPercentileWrite,_:=stats.Percentile(writeDurations,.95)
				ninetyPercentileRead,_ :=stats.Percentile(readDurations,.95)
				vals := [8]float64{writeReadFraction,float64(numKey),float64(dataSize),writeAverage,readAverage,ninetyPercentileWrite,ninetyPercentileRead,totalTime}
				var record []string;
				for _,val:= range vals{
					record = append(record, FloatToString(val))
				}
				records = append(records,record)

			}
		}
	}
	writer.WriteAll(records);
}
