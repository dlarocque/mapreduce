package mapreduce

import "strconv"

type KeyValue struct {
	Key   string
	Value string
}

type CommandLineArgs struct {
	InputDir   string
	OutputFile string
	NReduce    int
	NMap       int
}

type Job struct {
	inputDir   string
	outputFile string
	nMap       int
	nReduce    int
	mapFunc    MapFunc
	reduceFunc ReduceFunc
}

type MapFunc func(filename string, contents string) []KeyValue
type ReduceFunc func(key string, values []string) string

func (j *Job) Run() {

}

func CreateJob(inputDir, outputFile string, nMap, nReduce int, mapFunc MapFunc, reduceFunc ReduceFunc) *Job {
	return &Job{
		inputDir:   inputDir,
		outputFile: outputFile,
		nMap:       nMap,
		nReduce:    nReduce,
		mapFunc:    mapFunc,
		reduceFunc: reduceFunc,
	}
}

func ParseCommandLineArgs(osArgs []string) CommandLineArgs {
	if len(osArgs) != 5 {
		panic("Usage: wc <input-dir> <output-file> <nReduce> <nMap>")
	}

	nReduce, err := strconv.Atoi(osArgs[3])
	if err != nil {
		panic("nReduce must be an integer")
	}

	nMap, err := strconv.Atoi(osArgs[4])
	if err != nil {
		panic("nMap must be an integer")
	}

	return CommandLineArgs{
		InputDir:   osArgs[1],
		OutputFile: osArgs[2],
		NMap:       nMap,
		NReduce:    nReduce,
	}
}
