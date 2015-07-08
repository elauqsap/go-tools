package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
)

var loc, reg string
var col, max int
var done = make(chan bool)        // boolean channel to end the consumer loop
var matches = make(chan []string) // channel for processing regex matches
var stats chan []string
var top = make([]int, 0, 25)
var only = make([]int, 0, 25)

const debug = true
const version = "batch-csv-parser v0.1.0 (c) Pasquale D'Agostino"
const usage = "Given a directory of CSV files and a column to match on" +
	" a regular expression, this program will print the matches to standard out.\n" +
	version +
	"\n\nUsage:\nbatch-csv-parser [options]\n" +
	"where [options] are:\n" +
	"\t --col <i>:\t Column to perform the regex match on\n" +
	"\t --loc <s>:\t Path to the CSV file or directory of files\n" +
	"\t --reg <s>:\t Regex to match on the provided column (needs to be last argument)\n" +
	"\t --top <i+>:\t Columns to provide top occurances\n" +
	"\t --max <i>:\t Number of top values to print (default: 10)"

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetLevel(log.DebugLevel)
}

func main() {
	options()
	go produce(readCSV(loc))
	go consumeMatches()
	if len(top) != 0 {
		fmt.Println("CONDITION")
		stats = make(chan []string, 1024)
		go process()
	}
	<-done
}

func options() {
	join := strings.Join(append(os.Args), " ")
	cli := regexp.MustCompile(`(--help|--ver|--top ([1-9][0-9]? ?)+|--only ([1-9][0-9]? ?)+|--col [1-9][0-9]?|--max [1-9][0-9]?|--loc (\/.*\/.*\.csv|.*\.csv|\/.*\/)|--reg [^\s]+)`)
	matches := cli.FindAllString(join, -1)

	if len(matches) == 0 {
		fmt.Println(os.Args[0] + " --help for details")
		os.Exit(0)
	}

	for _, match := range matches {
		args := strings.Split(strings.TrimSpace(match), " ")
		switch args[0] {
		case "--help":
			fmt.Println(usage)
			os.Exit(0)
		case "--ver":
			fmt.Println(version)
			os.Exit(0)
		case "--col":
			col = str2int(args[0], args[1])
		case "--loc":
			if exists(args[1]) {
				loc = args[1]
			}
		case "--reg":
			reg = args[1]
		case "--top":
			for _, each := range args[1:] {
				top = append(top, str2int(args[0], each))
			}
		case "--max":
			max = str2int(args[0], args[1])
		case "--only":
			for _, each := range args[1:] {
				only = append(top, str2int(args[0], each))
			}
		default:
			fmt.Println(args[0] + " is an invalid argument, see " +
				os.Args[0] + " --help for details")
			os.Exit(0)
		}
		if debug {
			log.WithFields(log.Fields{
				"match": match,
			}).Debug("CLI Arguments")
		}
	}

}

func produce(entries [][]string) {
	regex, err := regexp.Compile(reg)
	if err != nil {
		log.Error(reg + ", is not a valid regular expression")
	} else {
		if len(top) != 0 {
			go process()
			<-done
		}
		for _, each := range entries {
			if regex.MatchString(each[col]) {
				matches <- each
				if stats != nil {
					stats <- each
				}
			}
		}
	}
	done <- true
}

func consumeMatches() {
	for {
		match := <-matches
		if len(match) != 0 {
			fmt.Println("MATCH: " + match[0])
		}
	}
}

func process() {
	for {
		stat := <-stats
		if len(stat) != 0 {
			fmt.Println("STAT: " + stat[0])
		}
	}
}

func str2int(opt string, str string) (x int) {
	x, err := strconv.Atoi(str)
	if err != nil {
		log.Error(opt + " " + str + ", please make sure it is a valid integer [1-99]")
		os.Exit(2)
	}
	return x
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		log.Error(path + ", is not a valid path to a file nor directory")
		os.Exit(2)
	}
	return true
}

func readCSV(location string) [][]string {
	csvFile, err := os.Open(location)
	if err != nil {
		fmt.Println("Unable to open: " + location)
		os.Exit(1)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = -1
	data, err := reader.ReadAll()
	fmt.Println(data[0])
	if err != nil {
		fmt.Println("Unable to parse the CSV file, make sure it is a valid CSV")
		os.Exit(1)
	}
	return data
}
