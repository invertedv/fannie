package collapse

//TODO add ageFpDt
import (
	"fmt"
	"github.com/invertedv/chutils"
	"github.com/invertedv/chutils/nested"
	s "github.com/invertedv/chutils/sql"
	"github.com/invertedv/fannie/raw"
	"reflect"
	"strings"
	"time"
)

// srdrs generates slices of Readers of len nRdrs. The data represented by rdr0 is equally divided
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
	nper := maxRow / nRdrs
	start := 0
	for ind := 0; ind < nRdrs; ind++ {
		var qryt string
		np := start + nper - 1
		if ind < nRdrs-1 {
			qryt = strings.Replace(qry, "<where>", fmt.Sprintf(" WHERE row >= %v AND row <= %v ", start, np), 1) //qry + fmt.Sprintf(" WHERE row >= %v AND row <= %v ", start, np)
		} else {
			qryt = strings.Replace(qry, "<where>", fmt.Sprintf(" WHERE row >= %v ", start), 1) //qry + fmt.Sprintf(" WHERE row >= %v ", start)

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
	fmt.Println("start time", time.Now())
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

	// build the extra fields and populate the Description field of rdrs[0].TableSpec
	newCalcs := make([]nested.NewCalcFn, 0)
	newCalcs = append(newCalcs, dtiField, channelField, sellerField, rateField, opbField, termField, origDtField,
		fpDtField, ltvField, cltvField, numBorrField, ficoField, coFicoField, firstTimeField, purposeField,
		propTypeField, unitsField, occField, stateField, msaField, zip3Field, miField, amTypeField, pPenField,
		ioField, ioDtField, zbDtField, zbUpbField)

	// rdrsn is a slice of nested readers -- needed since we are adding fields to the raw data
	rdrsn := make([]chutils.Input, 0)
	for j, r := range rdrs {

		rn, e := nested.NewReader(r, xtraFields(r.TableSpec()), newCalcs)
		if e := rn.TableSpec().Nest("monthly", "month", "matDt"); e != nil {
			return e
		}

		if e != nil {
			return e
		}
		if j == 0 {
			if e := rn.TableSpec().Check(); e != nil {
				return e
			}
			if create {
				// copy over the descriptions
				for _, fd := range rn.TableSpec().FieldDefs {
					if _, fdRaw, e := raw.TableDef.Get(fd.Name); e == nil {
						fd.Description = fdRaw.Description
					}
				}
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

	if e := chutils.Concur(nConcur, rdrsn, wrtrs, 10000); e != nil {
		return e
	}
	return nil
}

// xtraFields defines additional fields for the nested reader.
// baseTable is the TableDef for the reader that will issue the create table
func xtraFields(baseTable *chutils.TableDef) (fds []*chutils.FieldDef) {
	// get the info for these fields from the raw table
	flds := []string{"dti", "channel", "seller", "rate", "opb", "term", "origDt", "fpDt", "ltv", "cltv", "numBorr",
		"fico", "coFico", "firstTime", "purpose", "propType", "units", "occ", "state", "msa", "zip3", "mi",
		"amType", "pPen", "io", "ioDt", "zbDt", "zbUpb"}
	for _, f := range flds {
		if _, fd, err := raw.TableDef.Get(f); err == nil {
			fds = append(fds, fd)
		}
	}
	// drop the arrays from the query
	for _, f := range flds {
		if _, fd, err := baseTable.Get(f + "Array"); err == nil {
			fd.Drop = true
		}
	}
	return
}

// metric finds the max, last or average of slice x of type int or float
// fn='a': avg, fn='m': max, fn='l': last
func metric(x any, missing any, fn rune) interface{} {

	var missDt, maxDt time.Time
	var isDt bool

	arr := reflect.ValueOf(x)
	if arr.Kind() != reflect.Slice {
		return missing
	}
	miss := reflect.ValueOf(missing)
	if missDt, isDt = missing.(time.Time); isDt {
		maxDt = missDt
	}
	knd := arr.Index(0).Kind()

	var metric float64 = 0.0
	cnt := 0
	for j := 0; j < arr.Len(); j++ {
		var x float64
		var dt time.Time
		val := arr.Index(j)

		if val != miss {
			switch knd {
			case reflect.Int32:
				x = float64(val.Int())
			case reflect.Float32:
				x = val.Float()
			case reflect.Struct:
				dt = val.Interface().(time.Time)
			}
			switch fn {
			case 'a':
				metric += x
			case 'm':
				if isDt {
					if dt.After(maxDt) {
						maxDt = dt
					}
				} else {
					if x > metric {
						metric = x
					}
				}
			case 'l':
				if isDt {
					maxDt = dt
				} else {
					metric = x
				}
			}
			cnt++
		}
	}
	if cnt > 0 {
		switch fn {
		case 'a':
			return float32(metric) / float32(cnt)
		case 'm', 'l':
			if isDt {
				return maxDt
			}
			return float32(metric)
		}
	}
	return missing
}

// mode finds the most frequent non-missing value in cls
func mode(cls []string, missing string) string {
	cnts := make(map[string]int)
	any := false
	for _, c := range cls {
		if c != missing {
			any = true
			cnts[c]++
		}
	}
	if !any {
		return missing
	}
	maxCnt := 0
	maxCls := ""
	for k, v := range cnts {
		if v > maxCnt {
			maxCnt = v
			maxCls = k
		}
	}
	return maxCls
}

func compress(arrayName string, avgName string, td *chutils.TableDef, data chutils.Row, fn rune) (interface{}, error) {
	ind, _, err := td.Get(arrayName)
	if err != nil {
		return nil, err
	}
	_, fd, err := td.Get(avgName)
	if err != nil {
		return nil, err
	}
	switch fd.ChSpec.Base {
	case chutils.ChInt, chutils.ChFloat, chutils.ChDate:
		return metric(data[ind], fd.Missing, fn), nil
	case chutils.ChString, chutils.ChFixedString:
		arr, missing := data[ind].([]string), fd.Missing.(string)
		m := mode(arr, missing)
		return m, nil
	}
	return nil, nil
}

func dtiField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("dtiArray", "dti", td, data, 'a')
}

func rateField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("rateArray", "rate", td, data, 'a')
}

func opbField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("opbArray", "opb", td, data, 'a')
}

func termField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("termArray", "term", td, data, 'a')
}

func origDtField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("origDtArray", "origDt", td, data, 'm')
}

func fpDtField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("fpDtArray", "fpDt", td, data, 'm')
}

func ltvField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("ltvArray", "ltv", td, data, 'a')
}

func cltvField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("cltvArray", "cltv", td, data, 'a')
}

func numBorrField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("numBorrArray", "numBorr", td, data, 'a')
}

func ficoField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("ficoArray", "fico", td, data, 'a')
}

func coFicoField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("coFicoArray", "coFico", td, data, 'a')
}

func firstTimeField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("firstTimeArray", "firstTime", td, data, 'd')
}

func purposeField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("purposeArray", "purpose", td, data, 'd')
}

func propTypeField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("propTypeArray", "propType", td, data, 'd')
}

func unitsField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("unitsArray", "units", td, data, 'a')
}

func occField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("occArray", "occ", td, data, 'd')
}

func stateField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("stateArray", "state", td, data, 'd')
}

func msaField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("msaArray", "msa", td, data, 'd')
}

func zip3Field(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("zip3Array", "zip3", td, data, 'd')
}

func miField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("miArray", "mi", td, data, 'a')
}

func amTypeField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("amTypeArray", "amType", td, data, 'd')
}

func pPenField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("pPenArray", "pPen", td, data, 'd')
}

func ioField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("ioArray", "io", td, data, 'd')
}

func ioDtField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("ioDtArray", "ioDt", td, data, 'm')
}

func zbDtField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("zbDtArray", "zbDt", td, data, 'm')
}

func zbUpbField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("zbUpbArray", "zbUpb", td, data, 'm')
}

func channelField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("channelArray", "channel", td, data, 'd')
}

func sellerField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return compress("sellerArray", "seller", td, data, 'd')
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

const qry1 = `
SELECT
  a.*,
  r.row
FROM (
SELECT
  lnId,
  groupArray(month) AS month,
  groupArray(upb * 1.0) AS upb,
  groupArray(dq) AS dq,
  groupArray(lower(servicer)) AS servicer,
  groupArray(curRate * 1.0) AS curRate,
  groupArray(age) AS age,
  groupArray(rTermLgl) AS rTermLgl,
  groupArray(rTermAct) AS rTermAct,
  groupArray(dqStat) AS dqStat,
  groupArray(mod) AS mod,
  groupArray(zb) AS zb,
  groupArray(ioRem) AS ioRem,
  groupArray(bap) AS bap,
  groupArray(program) AS program,
  groupArray(matDt) AS matDt,

  groupArray(channel) AS channelArray,
  groupArray(lower(seller)) AS sellerArray,
  groupArray(dti) AS dtiArray,
  groupArray(rate) AS rateArray,
  groupArray(opb) AS opbArray,
  groupArray(term) AS termArray,
  groupArray(origDt) AS origDtArray,
  groupArray(fpDt) AS fpDtArray,
  groupArray(ltv) AS ltvArray,
  groupArray(cltv) AS cltvArray,
  groupArray(numBorr) AS numBorrArray,
  groupArray(fico) AS ficoArray,
  groupArray(coFico) AS coFicoArray,
  groupArray(firstTime) AS firstTimeArray,
  groupArray(purpose) AS purposeArray,
  groupArray(propType) AS propTypeArray,
  groupArray(units) AS unitsArray,
  groupArray(occ) AS occArray,
  groupArray(state) AS stateArray,
  groupArray(msa) AS msaArray,
  groupArray(zip3) AS zip3Array,
  groupArray(mi) AS miArray,
  groupArray(amType) AS amTypeArray,
  groupArray(pPen) AS pPenArray,
  groupArray(io) AS ioArray,
  groupArray(ioDt) AS ioDtArray,
  groupArray(zbDt) AS zbDtArray,
  groupArray(zbUpb) AS zbUpbArray
FROM sourceTable
GROUP BY lnId) AS a
JOIN
  (SELECT * FROM rowTable <where>) AS r
ON
  a.lnId = r.lnId
`
const qry = `
WITH jn AS (
SELECT
  a.*
FROM 
  sourceTable AS a
JOIN
  (SELECT * FROM rowTable <where>) AS r
ON
  a.lnId = r.lnId
ORDER BY lnId, month)
SELECT
  lnId,
  groupArray(month) AS month,
  groupArray(upb * 1.0) AS upb,
  groupArray(dq) AS dq,
  groupArray(lower(servicer)) AS servicer,
  groupArray(curRate * 1.0) AS curRate,
  groupArray(age) AS age,
  groupArray(rTermLgl) AS rTermLgl,
  groupArray(rTermAct) AS rTermAct,
  groupArray(dqStat) AS dqStat,
  groupArray(mod) AS mod,
  groupArray(zb) AS zb,
  groupArray(ioRem) AS ioRem,
  groupArray(bap) AS bap,
  groupArray(program) AS program,
  groupArray(matDt) AS matDt,

  groupArray(channel) AS channelArray,
  groupArray(lower(seller)) AS sellerArray,
  groupArray(dti) AS dtiArray,
  groupArray(rate) AS rateArray,
  groupArray(opb) AS opbArray,
  groupArray(term) AS termArray,
  groupArray(origDt) AS origDtArray,
  groupArray(fpDt) AS fpDtArray,
  groupArray(ltv) AS ltvArray,
  groupArray(cltv) AS cltvArray,
  groupArray(numBorr) AS numBorrArray,
  groupArray(fico) AS ficoArray,
  groupArray(coFico) AS coFicoArray,
  groupArray(firstTime) AS firstTimeArray,
  groupArray(purpose) AS purposeArray,
  groupArray(propType) AS propTypeArray,
  groupArray(units) AS unitsArray,
  groupArray(occ) AS occArray,
  groupArray(state) AS stateArray,
  groupArray(msa) AS msaArray,
  groupArray(zip3) AS zip3Array,
  groupArray(mi) AS miArray,
  groupArray(amType) AS amTypeArray,
  groupArray(pPen) AS pPenArray,
  groupArray(io) AS ioArray,
  groupArray(ioDt) AS ioDtArray,
  groupArray(zbDt) AS zbDtArray,
  groupArray(zbUpb) AS zbUpbArray
FROM 
  jn
GROUP BY lnId
`
