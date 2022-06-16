// package fannie loads the standard, HARP and non-standard (excluded) loans made available by Fannie Mae.
// The data is available at https://datadynamics.fanniemae.com/data-dynamics/#/reportMenu;category=HP.
//
// The final result is a single data with nested arrays for time-varying fields.
// Features:
//   - The data is subject to QA.  The results are presented as two string fields in a KeyVal format.
//   - A "DESCRIBE" of the output table provides info on each field
//   - New fields created are:
//      - vintage (e.g. 2010Q2)
//      - standard - Y/N flag, Y=standard process loan
//      - loan age based on first pay date
//      - numeric dq field
//      - property value at origination
//      - harp - Y/N flag, Y=HARP loan.
//      - file name from which the loan was loaded
//      - QA results. There are three sets of fields:
//          - The nested table qa that has two arrays:
//                - field.  The name of a field that has validation issues.
//                - cntFail. The number of months for which this field failed qa.  For static fields, this value will
//                   be 1.
//           - allFail.  An array of field names which failed for qa.  For monthly fields, this means the field failed for all months.
//
// command-line parameters:
//   -host  ClickHouse IP address. Default: 127.0.0.1.
//   -user  ClickHouse user. Default: default
//   -password ClickHouse password for user. Default: <empty>.
//   -table ClickHouse table in which to insert the data.
//   -maptable.  Clickhouse table that maps pre-HARP loan ids to HARP ids.  This table is both created and used by the package.
//   -create if Y, then the table is created/reset. Default: Y.
//   -dir directory with Fannie Mae text files.
//   -tmp ClickHouse database to use for temporary tables.
//   -concur # of concurrent processes to use in loading monthly files. Default: 1.
//   -memory max memory usage by ClickHouse.  Default: 40000000000.
//   -groupby max_bytes_before_external_groupby ClickHouse paramter. Default: 20000000000.
//
// The non-standard loans have four additional fields.  This package recognizes whether the file is standard or not.
// A combined table can be built by running the app twice pointing to the same -table.
// On the first run, set -create Y and set -create N for the second run.
//
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
	maxMemory := flag.Int64("memory", 40000000000, "int64")
	maxGroupby := flag.Int64("groupby", 20000000000, "int64")

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
		"max_memory_usage":                   *maxMemory,
		"max_bytes_before_external_group_by": *maxGroupby,
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

	gotMap := false // if true, this directory has the mapping of pre-HARP to HARP loans
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
	if gotMap {
		if e := raw.LoadHarpMap(*srcDir+"Loan_Mapping.txt", *mapTable, con); e != nil {
			log.Fatalln(e)
		}
	}

	sort.Strings(fileList)
	step1Time := 0.0
	step2Time := 0.0
	for ind, fileName := range fileList {
		fullFile := *srcDir + fileName
		tmpTable := *tmp + ".source"
		s := time.Now()
		if e := raw.LoadRaw(fullFile, tmpTable, true, *nConcur, con); e != nil {
			log.Fatalln(e)
		}
		step1 := time.Since(s).Minutes()
		s = time.Now()
		if e := collapse.GroupBy("tmp.source", *table, *mapTable, createTable, con); e != nil {
			log.Fatalln(e)
		}
		step2 := time.Since(s).Minutes()
		createTable = false
		fmt.Printf("Done with %s. %d out of %d ,times: %0.2f, %0.2f minutes\n", fileName, ind+1, len(fileList), step1, step2)
		step1Time += step1
		step2Time += step2

	}
	step1Time /= 60.0
	step2Time /= 60.0
	fmt.Printf("step1 time: %0.2f step2 time: %0.2f hours, total: %0.2f", step1Time, step2Time, step1Time+step2Time)
	// clean up
	_, _ = con.Exec(fmt.Sprintf("DROP TABLE %s.source", *tmp))
}
