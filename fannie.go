package main

import (
	"flag"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/invertedv/chutils"
	"github.com/invertedv/fannie/collapse"
	"github.com/invertedv/fannie/raw"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

func main() {
	var err error
	host := flag.String("host", "127.0.0.1", "string")
	user := flag.String("user", "default", "string")
	password := flag.String("password", "", "string")
	srcDir := flag.String("dir", "", "string")
	create := flag.String("create", "Y", "string")
	table := flag.String("table", "", "string")
	mapTable := flag.String("mapTable", "", "string")
	tmp := flag.String("tmp", "", "string")
	nConcur := flag.Int("concur", 1, "int")
	max_memory := flag.Int64("memory", 40000000000, "int64")
	max_groupby := flag.Int64("groupby", 20000000000, "int64")

	flag.Parse()
	createTable := *create == "Y" || *create == "y"
	//  1m24.974557878s, 1m2.991260235s for 2001Q2EXCL.csv
	// : 1m33.066475613s, 1m5.603348601s 6 threads
	// 1m32.039205298s, 1m6.800411966s 6 threads, granularity 32768
	// s: 1m33.856826032s, 1m4.525401128s 6 threads, granularity 4096

	if (*srcDir)[len(*srcDir)-1] != '/' {
		*srcDir += "/"
	}
	// connect to ClickHouse
	con, err := chutils.NewConnect(*host, *user, *password, clickhouse.Settings{
		"max_memory_usage":                   *max_memory,
		"max_bytes_before_external_group_by": *max_groupby,
	})
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if e := con.Close(); e != nil {
			log.Fatalln(e)
		}
	}()
	// holds the set of files to work through
	fileList := make([]string, 0)

	dir, err := os.ReadDir(*srcDir)
	if err != nil {
		log.Fatalln(fmt.Errorf("error reading directory: %s", *srcDir))
	}

	gotMap := false
	// build the file list
	for _, f := range dir {
		if strings.Contains(f.Name(), ".csv") && !strings.Contains(f.Name(), "Loan") {
			fileList = append(fileList, f.Name())
		}
		if f.Name() == "Loan_Mapping.txt" {
			gotMap = true
		}
	}
	if len(fileList) == 0 {
		log.Fatalln(fmt.Errorf("%s", "diredtory has no .csv files"))
	}
	if gotMap && createTable {
		if e := raw.LoadHarpMap(*srcDir+"Loan_Mapping.txt", *mapTable, con); e != nil {
			log.Fatalln(e)
		}
	}

	sort.Strings(fileList)
	step1Time := 0.0
	step2Time := 0.0
	target := "2000Q1.csv" // "2001Q2EXCL.csv"
	for ind, fileName := range fileList {
		if fileName != target {
			continue
		}
		fullFile := *srcDir + fileName
		tmpTable := *tmp + ".source"
		s := time.Now()
		if e := raw.LoadRaw(fullFile, tmpTable, true, *nConcur, con); e != nil {
			log.Fatalln(e)
		}
		step1 := time.Since(s)
		s = time.Now()
		if e := collapse.GroupBy("tmp.source", *table, *mapTable, createTable, con); e != nil {
			log.Fatalln(e)
		}
		step2 := time.Since(s)
		createTable = false
		fmt.Printf("Done with %s. %d out of %d ,times: %v, %v\n", fileName, ind+1, len(fileList), step1, step2)
		step1Time += step1.Seconds()
		step2Time += step2.Seconds()

	}
	step1Time /= 3600.0
	step2Time /= 3600.0
	fmt.Printf("step1 time: %0.2f step2 time: %0.2f hours, total: %0.2f", step1Time, step2Time, step1Time+step2Time)
}

//TODO: drop tmp.source when done
