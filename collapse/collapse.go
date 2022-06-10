package collapse

//TODO add ageFpDt
import (
	"fmt"
	"github.com/invertedv/chutils"
	s "github.com/invertedv/chutils/sql"
	"github.com/invertedv/fannie/raw"
	"strings"
	"time"
)

func GroupBy(sourceTable string, table string, tmpDb string, create bool, con *chutils.Connect) error {
	// build rowTable -- generates a row number for each lnId
	fmt.Println("start time", time.Now())
	q := strings.Replace(qry, "sourceTable", sourceTable, 1)

	rdr := s.NewReader(q, con)
	if e := rdr.Init("lnId", chutils.MergeTree); e != nil {
		return e
	}
	if create {
		// nested fields
		if e := rdr.TableSpec().Nest("monthly", "month", "matDt"); e != nil {
			return e
		}
		// bring over descriptions
		for _, f := range rdr.TableSpec().FieldDefs {
			if _, fd, e := raw.TableDef.Get(f.Name); e == nil {
				f.Description = fd.Description
			}
		}
		rdr.TableSpec().Create(con, table)
	}
	rdr.Name = table
	rdr.Insert()
	return nil
}

const qry = `
SELECT
  lnId,
  groupArray(month) AS month,
  groupArray(upb) AS upb,
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
  groupArray(nonIntUpb) AS nonIntUpb,
  groupArray(frgvUpb) AS frgvUpb,
  groupArray(totPrin) AS totPrin,
  groupArray(matDt) AS matDt,

  arrayFirst(x->x!='X' ? 1 : 0, groupArray(channel)) = '' ? 'X' : arrayFirst(x->x!='X' ? 1 : 0, groupArray(channel)) AS channel,
  arrayFirst(x->x!='unknown' ? 1 : 0, groupArray(lower(seller))) = '' ? 'unknown' : arrayFirst(x->x!='unknown' ? 1 : 0, groupArray(seller)) AS seller,
  arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(dti))) > 0 ? arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(dti))) : -1  AS dti,
  arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(rate))) > 0 ? arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(rate))) : -1  AS rate,
  arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(opb))) > 0 ? arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(opb))) : -1  AS opb,
  arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(term))) > 0 ? arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(term))) : -1  AS term,

  arrayFirst(x->year(x) > 1970 ? 1 : 0, groupArray(origDt)) AS origDt,
  arrayFirst(x->year(x) > 1970 ? 1 : 0, groupArray(fpDt)) AS fpDt,
  arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(ltv))) > 0 ? arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(ltv))) : -1  AS ltv,
  arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(cltv))) > 0 ? arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(cltv))) : -1  AS cltv,
  arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(numBorr))) > 0 ? arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(numBorr))) : -1  AS numBorr,
  arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(fico))) > 0 ? arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(fico))) : -1  AS fico,
  arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(coFico))) > 0 ? arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(coFico))) : -1  AS coFico,

  arrayFirst(x->x!='X' ? 1 : 0, groupArray(firstTime)) = '' ? 'X' : arrayFirst(x->x!='X' ? 1 : 0, groupArray(firstTime)) AS firstTime,
  arrayFirst(x->x!='X' ? 1 : 0, groupArray(purpose)) = '' ? 'X' : arrayFirst(x->x!='X' ? 1 : 0, groupArray(purpose)) AS purpose,
  arrayFirst(x->x!='XX' ? 1 : 0, groupArray(propType)) = '' ? 'XX' : arrayFirst(x->x!='XX' ? 1 : 0, groupArray(propType)) AS propType,
  arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(units))) > 0 ? arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(units))) : -1  AS units,
  arrayFirst(x->x!='X' ? 1 : 0, groupArray(occ)) = '' ? 'X' : arrayFirst(x->x!='X' ? 1 : 0, groupArray(occ)) AS occ,
  arrayFirst(x->x!='XX' ? 1 : 0, groupArray(state)) = '' ? 'XX' : arrayFirst(x->x!='XX' ? 1 : 0, groupArray(state)) AS state,
  arrayFirst(x->x!='XXXXX' ? 1 : 0, groupArray(msa)) = '' ? 'XXXXX' : arrayFirst(x->x!='XXXXX' ? 1 : 0, groupArray(msa)) AS msa,
  arrayFirst(x->x!='000' ? 1 : 0, groupArray(zip3)) = '' ? '000' : arrayFirst(x->x!='000' ? 1 : 0, groupArray(zip3)) AS zip3,
  arrayAvg(arrayFilter(x -> x >= 0 ? 1 : 0, groupArray(mi))) > 0 ? arrayAvg(arrayFilter(x -> x >= 0 ? 1 : 0, groupArray(mi))) : -1  AS mi,
  arrayFirst(x->x!='XXX' ? 1 : 0, groupArray(amType)) = '' ? 'XXX' : arrayFirst(x->x!='XXX' ? 1 : 0, groupArray(amType)) AS amType,
  arrayFirst(x->x!='X' ? 1 : 0, groupArray(pPen)) = '' ? 'X' : arrayFirst(x->x!='X' ? 1 : 0, groupArray(pPen)) AS pPen,
  arrayFirst(x->x!='X' ? 1 : 0, groupArray(io)) = '' ? 'X' : arrayFirst(x->x!='X' ? 1 : 0, groupArray(io)) AS io,

  arrayFirst(x->year(x) > 1970 ? 1 : 0, groupArray(ioDt)) AS ioDt,
  arrayFirst(x->year(x) > 1970 ? 1 : 0, groupArray(zbDt)) AS zbDt,
  arrayMax(groupArray(zbUpb)) AS zbUpb,
  arrayFirst(x->year(x) > 1970 ? 1 : 0, groupArray(lpDt)) AS lpDt,
  arrayFirst(x->year(x) > 1970 ? 1 : 0, groupArray(fclDt)) AS fclDt,
  arrayFirst(x->year(x) > 1970 ? 1 : 0, groupArray(dispDt)) AS dispDt,
  arrayMax(groupArray(fclExp)) AS fclExp,
  arrayMax(groupArray(fclPExp)) AS fclPExp,
  arrayMax(groupArray(fclLExp)) AS fclLExp,
  arrayMax(groupArray(fclMExp)) AS fclMExp,
  arrayMax(groupArray(fclTaxes)) AS fclTaxes,
  arrayMax(groupArray(fclProNet)) AS fclProNet,
  arrayMax(groupArray(fclProMi)) AS fclProMi,
  arrayMax(groupArray(fclProMw)) AS fclProMw,
  arrayMax(groupArray(fclProOth)) AS fclProOth

FROM 
(SELECT * FROM  sourceTable ORDER BY lnId, month)
GROUP BY lnId
`
