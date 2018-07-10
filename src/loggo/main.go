package main

import (
	"bufio"
	"flag"
	"fmt"
	"gopkg.in/mgo.v2"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

const (
	MNG_SERVER = "127.0.0.1:27017"
	MNG_BD     = "test"
	MNG_COL    = "logas"
)

func readLog(path string, insertChan chan *LogItem, logFormat string) {
	inFile, _ := os.Open(path)
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		scanString := scanner.Text()
		if len(scanString) == 0 {
			continue
		}
		logItem := createLogItem(scanString, path, logFormat)
		if logItem != nil {
			insertChan <- logItem
		}
	}
}

func worker(file string, insertChan chan *LogItem, logFormat string) {
	f, _ := os.Open(file)
	defer f.Close()
	fl, _ := f.Stat()
	currSize := fl.Size()
	var prevSize int64 = 0
	for {
		if currSize > 0 && currSize != prevSize {
			readLog(file, insertChan, logFormat)
			prevSize = currSize
		}
		fl, _ = f.Stat()
		currSize = fl.Size()
		time.Sleep(2 * time.Second)
	}
}

func getFiles() (files []string, format string) {
	typeFlag := flag.String("t", "first_format", "log's format: first_format | second_format")
	flag.Parse()
	if typeFlag != nil {
		format = *typeFlag
	}
	files = flag.Args()
	return
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	session, err := mgo.Dial(MNG_SERVER)
	if err != nil {
		panic(err)
	}
	session.SetMode(mgo.Monotonic, true)
	collection := session.DB(MNG_BD).C(MNG_COL)
	defer session.Close()

	insertChan := make(chan *LogItem)
	files, format := getFiles()

	if len(files) == 0 {
		usage()
		return
	}

	for _, r := range files {
		go worker(r, insertChan, format)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	signal.Notify(sigChan, syscall.SIGTERM)
	for {
		select {
		case logItem := <-insertChan:
			err := logItem.insert(collection)
			if err != nil {
				fmt.Println(err)
			}
		case <-sigChan:
			close(insertChan)
			fmt.Printf("\n")
			return
		}
	}
}

func usage() {
	fmt.Println("loggo file1.log file2.log")
}
