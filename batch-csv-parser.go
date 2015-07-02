package main

import (
	"os"
	"regexp"
	"strings"

	log "github.com/Sirupsen/logrus"
)

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

// log.WithFields(log.Fields{
// 	"animal": "walrus",
// }).Info("A walrus appears")

func main() {
	parser()

}

func parser() {
	join := strings.Join(append(os.Args), " ")
	cli := regexp.MustCompile(`(--top ([\d] ?)+|--only ([\d] ?)+|--col \d|--max \d{1,2}|--loc (/.*/|/.*/*.csv)|--reg (.*)$)`)
	matches := cli.FindAllString(join, -1)

	log.WithFields(log.Fields{
		"Args": matches[1],
	}).Info("Command Line Input")
}
