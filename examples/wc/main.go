package main

import (
	"os"
	"strconv"
	"strings"
	"unicode"

	"mapreduce"
)

// Define the map and reduce functions
func Map(filename string, contents string) []mapreduce.KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	words := strings.FieldsFunc(contents, ff)

	kvs := make([]mapreduce.KeyValue, 0, len(words))
	for _, word := range words {
		kvs = append(kvs, mapreduce.KeyValue{Key: word, Value: "1"})
	}

	return kvs
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}

func main() {
	// Parse command line arguments
	args := mapreduce.ParseCommandLineArgs(os.Args)

	// Create a new MapReduce job
	job := mapreduce.CreateJob(args.InputDir, args.OutputFile, args.NMap, args.NReduce, Map, Reduce)

	// Run the job
	job.Run()
}
