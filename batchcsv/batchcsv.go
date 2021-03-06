package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/elauqsap/sortedmap"
)

var header []string
var loc, reg string
var col, max = 0, 10
var out = false
var dir = false

var stats chan []string           // channel for producing stats, init if flag
var done = make(chan bool)        // boolean channel to end the consumer loop
var matches = make(chan []string) // channel for processing regex matches

var top = make([]int, 0, 25)
var only = make([]int, 0, 25)

var maps = make(map[int]map[string]int)

const debug = false
const version = "batch-csv-parser v0.1.0 (c) Pasquale D'Agostino"
const usage = "Given a directory of CSV files and a column to match on" +
	" a regular expression, this program will print the matches to standard out.\n" +
	version +
	"\n\nUsage:\nbatch-csv-parser [options]\n" +
	"where [options] are:\n" +
	"\t --col <i>:\t Column to perform the regex match on\n" +
	"\t --loc <s>:\t Path to the CSV file or directory of files\n" +
	"\t --max <i>:\t Number of top values to print (default: 10)\n" +
	"\t --only <i+>:\t Columns to only print from the CSV\n" +
	"\t --out <b>:\t Flag to write to CSV instead of stdout\n" +
	"\t --reg <s>:\t Regex to match on the provided column (needs to be last argument)\n" +
	"\t --top <i+>:\t Columns to provide top occurances"

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetLevel(log.DebugLevel)
}

func main() {
	options()
	if dir {
		files, _ := filepath.Glob(loc + "*.csv")
		for _, file := range files {
			run(file)
		}
	} else {
		run(loc)
	}
	printStats()
}

func run(file string) {
	go produce(readCSV(file))
	go process()
	if len(top) != 0 {
		stats = make(chan []string, 1024)
		go statistics()
	}
	<-done
}

func options() {
	join := strings.Join(append(os.Args), " ")
	cli := regexp.MustCompile(`(--help|--ver|--top (\d{1,2} ?)+|--only (\d{1,2} ?)+|--col (0|[1-9][0-9])?|--max [1-9][0-9]?|--loc (\/.*\/.*\.csv|.*\.csv|\/.*\/|[a-zA-Z0-9\-]+\/)|--reg [^\s]+|--out)`)
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
		case "--out":
			out = true
		case "--only":
			for _, each := range args[1:] {
				only = append(only, str2int(args[0], each))
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

func process() {
	for {
		match := <-matches
		if len(match) != 0 {
			if out {

			} else {
				if len(only) != 0 {
					var buffer bytes.Buffer
					for _, entry := range only {
						buffer.WriteString(match[entry] + ",")
					}
					fmt.Println(strings.TrimSuffix(buffer.String(), ","))
				} else {
					str := strings.Join(blanks(match), ",")
					fmt.Println(strings.TrimSuffix(str, ","))
				}
			}
		}
	}
}

func statistics() {
	for {
		stat := <-stats
		if len(stat) != 0 {
			for _, each := range top {
				if maps[each] == nil {
					maps[each] = make(map[string]int)
				}
				(maps[each][stat[each]])++
			}
		}
	}
}

func print(str string) {
	fmt.Println(str)
}

func printStats() {
	print("")
	print("############### TOP ###############")
	print("")
	for key, values := range maps {
		print(header[key])
		if max >= len(values) {
			for _, res := range sortedmap.SortedKeys(values) {
				print(strconv.Itoa(values[res]) + "\t" + res)
			}
			print("")
		} else {
			for _, res := range sortedmap.SortedKeys(values)[:max] {
				print(strconv.Itoa(values[res]) + "\t" + res)
			}
			print("")
		}
	}
	print("############### END ###############")
}

func str2int(opt string, str string) (x int) {
	x, err := strconv.Atoi(str)
	if err != nil {
		log.Error(opt + " " + str + ", please make sure it is a valid integer [1-99]")
		os.Exit(2)
	}
	return x
}

func blanks(s []string) []string {
	var r []string
	for _, str := range s {
		if str != "" {
			r = append(r, str)
		}
	}
	return r
}

func exists(path string) bool {
	src, err := os.Stat(path)
	if err == nil {
		if src.IsDir() {
			dir = true
		}
		return true
	}
	if os.IsNotExist(err) {
		log.Error(path + ", is not a valid path to a file nor directory")
		os.Exit(2)
	}
	return true
}

func readCSV(location string) [][]string {
	readfile, err := os.Open(location)
	if err != nil {
		fmt.Println("Unable to open: " + location)
		os.Exit(1)
	}
	defer readfile.Close()

	reader := csv.NewReader(readfile)
	reader.FieldsPerRecord = -1
	data, err := reader.ReadAll()
	header = data[0]
	if err != nil {
		fmt.Println("Unable to parse the CSV file, make sure it is a valid CSV")
		os.Exit(1)
	}
	return data
}
