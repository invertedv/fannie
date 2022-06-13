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
	_, _, _ = table, tmp, nConcur
	// add trailing slash, if needed
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
	if !gotMap {
		log.Fatalln(fmt.Errorf("did not find Loan_Mapping.txt"))
	}
	if e := raw.LoadHarpMap(*srcDir+"Loan_Mapping.txt", *mapTable, con); e != nil {
		log.Fatalln(e)
	}

	sort.Strings(fileList)
	createTable := *create == "Y" || *create == "y"
	start := time.Now()
	for ind, fileName := range fileList {
		_ = ind
		fullFile := *srcDir + fileName
		tmpTable := *tmp + ".source"
		s := time.Now()
		//_, _ = fullFile, tmpTable
		if e := raw.LoadRaw(fullFile, tmpTable, true, *nConcur, con); e != nil {
			log.Fatalln(e)
		}
		if e := collapse.GroupBy("tmp.source", *table, *mapTable, createTable, con); e != nil {
			log.Fatalln(e)
		}
		fmt.Println("elapsed time", time.Since(s))
		createTable = false
		fmt.Printf("Done with %s. %d out of %d , elapsed time: %v\n", fileName, ind+1, len(fileList), time.Since(s))
		//		break

	}
	fmt.Println("elapsed time", time.Since(start))
}
