package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

var done = make(chan bool)      // boolean channel to end the consumer loop
var matches = make(chan string) // channel for processing regex matches

/* Ideas
1. 1st channel feeds matches, 2nd channel feeds stats if any
2. In stats function use a closure to map the values
3. defer printing of stats until main closes
*/

func main() {
	/* Add a loop to go through all CSV files
	   in the directory around the go functions */
	go produceMatches(readCSV("test.csv")) //
	go consumeMatches()                    //
	<-done                                 //
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
