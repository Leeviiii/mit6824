package mapreduce

import (
	"container/list"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type ByKey []KeyValue

func (a ByKey) Len() int      { return len(a) }
func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a ByKey) Less(i, j int) bool { return strings.Compare(a[i].Key, a[j].Key) < 0 }
/*
func (a ByKey) Less(i, j int) bool {
	if len(a[i].Key) == len(a[j].Key) {
		return strings.Compare(a[i].Key, a[j].Key) < 0
	}
	return len(a[i].Key) < len(a[j].Key)
}*/

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	kvs := make(map[string]*list.List)
	for mapTaskNumber := 0; mapTaskNumber < nMap; mapTaskNumber++ {
		reduceFileName := reduceName(jobName, mapTaskNumber, reduceTaskNumber)
		fi, _ := os.Open(reduceFileName)
		defer fi.Close()
		dec := json.NewDecoder(fi)
		for {
			var ele KeyValue
			if err := dec.Decode(&ele); err == io.EOF {
				break
			}
			v, ok := kvs[ele.Key]
			if ok {
				v.PushBack(ele.Value)
			} else {
				kvs[ele.Key] = list.New()
				kvs[ele.Key].PushBack(ele.Value)
			}
		}
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
    sort.Strings(keys)
	reduceD := list.New()
	for _, k := range keys {
		reduceValues := convertList2Array(kvs[k])
		reduceResult := reduceF(k, reduceValues)
		reduceD.PushBack(KeyValue{k, reduceResult})
	}


	fmt.Println(outFile)
	file, _ := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, 0666)
	defer file.Close()
	enc := json.NewEncoder(file)
	for e := reduceD.Front(); e != nil; e = e.Next() {
		enc.Encode(e.Value)
	}
}
func convertList2Array(l *list.List) []string {
	var keys []string
	for e := l.Front(); e != nil ; e = e.Next() {
		keys = append(keys, e.Value.(string))
	}
	return keys
}
