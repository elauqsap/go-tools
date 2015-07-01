package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

var location string
var done = make(chan bool)      // boolean channel to end the consumer loop
var matches = make(chan string) // channel for processing regex matches
const version = "batch-csv-parser v0.1.0 (c) Pasquale D'Agostino"

/* Ideas
1. 1st channel feeds matches, 2nd channel feeds stats if any
2. In stats function use a closure to map the values
3. defer printing of stats until main closes
*/

func main() {
	/* Add a loop to go through all CSV files
	   in the directory around the go functions */
	argParser()
	go produceMatches(readCSV(location)) //
	go consumeMatches()                  //
	<-done                               //
}

func argParser() {
	if len(os.Args[1:]) == 0 {
		fmt.Println(os.Args[0] + " --help for details")
		os.Exit(0)
	}
	for _, arg := range os.Args[1:] {
		switch arg {
		case "-h", "--help":
			printHelp()
		case "-c", "--col":
		case "-l", "--loc":
		case "-r", "--reg":
		case "-t", "--top":
		case "-m", "--max":
		case "-v", "--ver":
			fmt.Println(version)
			os.Exit(0)
		default:
			fmt.Println(arg + " is an invalid argument, see " +
				os.Args[0] + " --help for details")
			os.Exit(0)
		}
	}
}

func printHelp() {
	fmt.Println("Given a directory of CSV files and a column to match on" +
		" a regular expression, this program will print the matches to standard out.\n" +
		version +
		"\n\nUsage:\nbatch-csv-parser [options]\n" +
		"where [options] are:\n" +
		"\t --col <i>:\t Column to perform the regex match on\n" +
		"\t --loc <s>:\t Path to the CSV file or directory of files\n" +
		"\t --reg <s>:\t Regex to match on the provided column\n" +
		"\t --top <i+>:\t Columns to provide top occurances\n" +
		"\t --max <i>:\t Number of top values to print (default: 10)")
	os.Exit(0)
}

/*

*/
func produceMatches(entries [][]string) {
	for _, each := range entries {
		matches <- each[0]
	}
	done <- true
}

func consumeMatches() {
	for {
		match := <-matches
		fmt.Println(match)
	}
}

func readCSV(file string) [][]string {
	csvFile, err := os.Open(file)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = -1
	data, err := reader.ReadAll()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return data
}
