package collapse

import (
	"fmt"
	"github.com/invertedv/chutils"
	"github.com/invertedv/chutils/nested"
	s "github.com/invertedv/chutils/sql"
	"strings"
)

// Rdrs generates slices of Readers of len nRdrs. The data represented by rdr0 is equally divided
// amongst the Readers in the slice.
func srdrs(nRdrs int, qry string, rowTable string, con *chutils.Connect) (r []chutils.Input, err error) {

	r = nil
	rows, err := con.Query(fmt.Sprintf("SELECT max(row) as maxRow FROM %s", rowTable))
	if err != nil {
		return
	}

	var maxRow int
	for rows.Next() {
		rows.Scan(&maxRow)
	}
	nper := (maxRow + 1) / nRdrs
	start := 0
	for ind := 0; ind < nRdrs; ind++ {
		var qryt string
		np := start + nper - 1
		if ind < nRdrs-1 {
			qryt = qry + fmt.Sprintf(" WHERE row >= %v AND row <= %v ", start, np)
		} else {
			qryt = qry + fmt.Sprintf(" WHERE row >= %v ", start)

		}
		x := s.NewReader(qryt, con)
		if err = x.Init("lnId", chutils.MergeTree); err != nil {
			return
		}
		start += nper
		r = append(r, x)
	}
	return
}

func GroupBy(sourceTable string, table string, tmpDb string, create bool, nConcur int, con *chutils.Connect) (err error) {
	// build rowTable -- generates a row number for each lnId
	rowTable := fmt.Sprintf("%s.rows", tmpDb)
	qR := strings.Replace(qryRows, "sourceTable", sourceTable, 1)
	rdrRow := s.NewReader(qR, con)
	rdrRow.Name = rowTable
	if e := rdrRow.Init("lnId", chutils.MergeTree); e != nil {
		return e
	}
	if e := rdrRow.TableSpec().Create(con, rowTable); e != nil {
		return e
	}
	if e := rdrRow.Insert(); e != nil {
		return e
	}
	if e := rdrRow.Close(); e != nil {
		return e
	}

	q := strings.Replace(qry, "rowTable", rowTable, 1)
	q = strings.Replace(q, "sourceTable", sourceTable, 1)

	rdrs, err := srdrs(nConcur, q, rowTable, con)
	if err != nil {
		return
	}

	newCalcs := make([]nested.NewCalcFn, 0)
	newCalcs = append(newCalcs, dtiField)

	// rdrsn is a slice of nested readers -- needed since we are adding fields to the raw data
	rdrsn := make([]chutils.Input, 0)
	for j, r := range rdrs {

		rn, e := nested.NewReader(r, xtraFields(), newCalcs)

		if e != nil {
			return e
		}
		if j == 0 {
			if e := rn.TableSpec().Check(); e != nil {
				return e
			}
			if create {
				if err = rn.TableSpec().Create(con, table); err != nil {
					return err
				}
			}
		}
		rdrsn = append(rdrsn, rn)
	}
	var wrtrs []chutils.Output
	// build a slice of writers
	if wrtrs, err = s.Wrtrs(table, nConcur, con); err != nil {
		return
	}

	if e := chutils.Concur(nConcur, rdrsn, wrtrs, 0); e != nil {
		return e
	}
	return nil
}

// xtraFields defines additional fields for the nested reader
func xtraFields() (fds []*chutils.FieldDef) {
	vfd := &chutils.FieldDef{
		Name:        "dti",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "dti",
		Legal:       chutils.NewLegalValues(),
		Missing:     int32(1000),
	}
	fds = []*chutils.FieldDef{vfd}
	return
}

func dtiField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	ind, _, err := td.Get("dtiArray")
	if err != nil {
		return nil, err
	}
	dti := data[ind].([]int32)[0]
	return dti, nil
}

// qryRows creates a table that maps lnId to a row number. This is needed to divvy up the table
const qryRows = `
WITH r AS (
SELECT
  lnId,
  toInt32(rowNumberInAllBlocks()) AS row
FROM 
  sourceTable
GROUP BY lnId  )
SELECT * FROM r
`

const qry = `
SELECT
  a.*,
  r.row
FROM (
SELECT
  lnId,
  groupArray(dti) AS dtiArray,
  groupArray(month) AS monthArray,
  groupArray(upb) AS upbArray
FROM 
  sourceTable
GROUP BY lnId) AS a
JOIN
  rowTable AS r
ON
  a.lnId = r.lnId
`
