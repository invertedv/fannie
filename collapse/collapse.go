// Package collapse reduces the raw table that has one entry per loan per month to a table that has one entry
// per loan.
// There are nested tables:
//   - monthly.  These are values that change every month.
//   - qa. The qa table.
package collapse

import (
	"fmt"
	"github.com/invertedv/chutils"
	s "github.com/invertedv/chutils/sql"
	"github.com/invertedv/fannie/raw"
	"strings"
)

// GroupBy groups the raw table (which has one row per loan per month to a table with one row per loan
func GroupBy(sourceTable string, table string, harpTable string, create bool, con *chutils.Connect) error {
	// remove placeholder table names
	q := strings.Replace(strings.Replace(qry, "sourceTable", sourceTable, 2), "harpTable", harpTable, 2)

	// the query has placeholders for the missing values of fields of the form <fieldMissing>
	for _, f := range raw.TableDef.FieldDefs {
		q = strings.Replace(q, fmt.Sprintf("<%sMissing>", f.Name), fmt.Sprintf("%v", f.Missing), -1)
	}

	rdr := s.NewReader(q, con)
	defer func() { _ = rdr.Close() }()
	if e := rdr.Init("lnId", chutils.MergeTree); e != nil {
		return e
	}
	if create {
		// nested fields
		if e := rdr.TableSpec().Nest("monthly", "month", "matDt"); e != nil {
			return e
		}
		if e := rdr.TableSpec().Nest("qa", "field", "cntFail"); e != nil {
			return e
		}
		// bring over descriptions
		for _, f := range rdr.TableSpec().FieldDefs {
			if _, fraw, e := raw.TableDef.Get(f.Name); e == nil {
				f.Description = fraw.Description
				// These don't survive the groupArray
				if fraw.ChSpec.Base == chutils.ChFixedString {
					f.ChSpec.Base = chutils.ChFixedString
					f.ChSpec.Length = fraw.ChSpec.Length
				}
				for _, fnc := range fraw.ChSpec.Funcs {
					if fnc == chutils.OuterLowCardinality {
						f.ChSpec.Funcs = append(f.ChSpec.Funcs, fnc)
					}
				}
			}
			// added details for new fields
			switch f.Name {
			case "bucket":
				f.Description = "loan bucket"
			case "ageFpDt":
				f.Description = "age based on fdDt, missing=-1000"
			case "harpLnId":
				f.Description = "loan refinanced to this HARP loan"
			case "preHarpId":
				f.Description = "HARP loan refinanced from this loan"
			case "harp":
				f.Description = "loan is HARP: Y, N"
				f.ChSpec.Base = chutils.ChFixedString
				f.ChSpec.Length = 1
			case "field":
				f.Description = "field name"
				f.ChSpec.Funcs = append(f.ChSpec.Funcs, chutils.OuterLowCardinality)
			case "cntFail":
				f.Description = "# of months field failed qa"
			case "allFail":
				f.Description = "fields that failed QA all months"
				f.ChSpec.Funcs = append(f.ChSpec.Funcs, chutils.OuterLowCardinality)
			}
		}
		if e := rdr.TableSpec().Create(con, table); e != nil {
			return e
		}
	}
	rdr.Name = table
	rdr.Sql = strings.Replace(rdr.Sql, "LIMIT 10", "", 2)
	if e := rdr.Insert(); e != nil {
		return e
	}
	return nil
}

// qry is the query that collapses the multiple rows per lnId to a single one
//
// Note: there are two "LIMIT 10" statements.  These make the query to run much faster for the Init() method.
// The Init() method appends a "LIMIT 1", but this query is complex enough that isn't helpful.
// There are placeholders for the table created by package raw and the map of pre-HARP ids to HARP ids
const qry = `
WITH q AS (
  SELECT lnId, 
    groupArray(grp) AS qa,
    groupArray(n) AS nqa
FROM (
  SELECT 
     lnId, 
     arrayJoin(splitByChar(':', qa)) AS grp,
     toInt32(count(*)) AS n
  FROM sourceTable 
  WHERE grp != ''
  GROUP BY lnId, grp)
GROUP BY lnId),
r AS (
SELECT
  lnId,
  groupArray(toLastDayOfMonth(month)) AS month,
  groupArray(upb) AS upb,
  groupArray(dq) AS dq,
  groupArray(lower(servicer)) AS servicer,
  groupArray(curRate) AS curRate,
  groupArray(age) AS age,
  groupArray(ageFpDt) AS ageFpDt,
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
  groupArray(servAct) AS servAct,
  groupArray(matDt) AS matDt,

  arrayFirst(x->x!='<channelMissing>' ? 1 : 0, groupArray(channel)) = '' ? '<channelMissing>' : arrayFirst(x->x!='<channelMissing>' ? 1 : 0, groupArray(channel)) AS channel,
  arrayFirst(x->x!='<sellerMissing>' ? 1 : 0, groupArray(lower(seller))) = '' ? '<sellerMissing>' : arrayFirst(x->x!='<sellerMissing>' ? 1 : 0, groupArray(seller)) AS seller,
  toFloat32(arrayAvg(arrayFilter(x -> x != <dtiMissing> ? 1 : 0, groupArray(dti))) > 0 ? arrayAvg(arrayFilter(x -> x != <dtiMissing> ? 1 : 0, groupArray(dti))) : <dtiMissing>)  AS dti,
  toFloat32(arrayAvg(arrayFilter(x -> x != <rateMissing> ? 1 : 0, groupArray(rate))) > 0 ? arrayAvg(arrayFilter(x -> x != <rateMissing> ? 1 : 0, groupArray(rate))) : <rateMissing>)  AS rate,
  toFloat32(arrayAvg(arrayFilter(x -> x != <opbMissing> ? 1 : 0, groupArray(opb))) > 0 ? arrayAvg(arrayFilter(x -> x != <rateMissing> ? 1 : 0, groupArray(opb))) : <rateMissing>)  AS opb,
  toFloat32(arrayAvg(arrayFilter(x -> x != <termMissing> ? 1 : 0, groupArray(term))) > 0 ? arrayAvg(arrayFilter(x -> x != <termMissing> ? 1 : 0, groupArray(term))) : <termMissing>)  AS term,

  arrayFirst(x->year(x) > 1970 ? 1 : 0, groupArray(origDt)) AS origDt,
  arrayFirst(x->year(x) > 1970 ? 1 : 0, groupArray(fpDt)) AS fpDt,
  toFloat32(arrayAvg(arrayFilter(x -> x != <ltvMissing> ? 1 : 0, groupArray(ltv))) > 0 ? arrayAvg(arrayFilter(x -> x != <ltvMissing> ? 1 : 0, groupArray(ltv))) : <ltvMissing>)  AS ltv,
  toFloat32(arrayAvg(arrayFilter(x -> x != <cltvMissing> ? 1 : 0, groupArray(cltv))) > 0 ? arrayAvg(arrayFilter(x -> x != <cltvMissing> ? 1 : 0, groupArray(cltv))) : <cltvMissing>)  AS cltv,
  toInt32(arrayAvg(arrayFilter(x -> x != <numBorrMissing> ? 1 : 0, groupArray(numBorr))) > 0 ? arrayAvg(arrayFilter(x -> x != <numBorrMissing> ? 1 : 0, groupArray(numBorr))) : <numBorrMissing>)  AS numBorr,
  toInt32(arrayAvg(arrayFilter(x -> x != <ficoMissing> ? 1 : 0, groupArray(fico))) > 0 ? arrayAvg(arrayFilter(x -> x != <ficoMissing> ? 1 : 0, groupArray(fico))) : <ficoMissing>)  AS fico,
  toInt32(arrayAvg(arrayFilter(x -> x != <coFicoMissing> ? 1 : 0, groupArray(coFico))) > 0 ? arrayAvg(arrayFilter(x -> x != <coFicoMissing>? 1 : 0, groupArray(coFico))) : <coFicoMissing>)  AS coFico,

  arrayFirst(x->x!='<firstTimeMissing>' ? 1 : 0, groupArray(firstTime)) = '' ? '<firstTimeMissing>' : arrayFirst(x->x!='<firstTimeMissing>' ? 1 : 0, groupArray(firstTime)) AS firstTime,
  arrayFirst(x->x!='<purposeMissing>' ? 1 : 0, groupArray(purpose)) = '' ? '<purposeMissing>' : arrayFirst(x->x!='<purposeMissing>' ? 1 : 0, groupArray(purpose)) AS purpose,
  arrayFirst(x->x!='<propTypeMissing>' ? 1 : 0, groupArray(propType)) = '' ? '<propTypeMissing>' : arrayFirst(x->x!='<propTypeMissing>' ? 1 : 0, groupArray(propType)) AS propType,
  toFloat32(arrayAvg(arrayFilter(x -> x != <unitsMissing> ? 1 : 0, groupArray(units))) > 0 ? arrayAvg(arrayFilter(x -> x != <unitsMissing> ? 1 : 0, groupArray(units))) : <unitsMissing>)  AS units,
  arrayFirst(x->x!='<occMissing>' ? 1 : 0, groupArray(occ)) = '' ? '<occMissing>' : arrayFirst(x->x!= '<occMissing>' ? 1 : 0, groupArray(occ)) AS occ,
  arrayFirst(x->x!='<stateMissing>' ? 1 : 0, groupArray(state)) = '' ? '<stateMissing>' : arrayFirst(x->x!='<stateMissing>' ? 1 : 0, groupArray(state)) AS state,
  arrayFirst(x->x!='<msaMissing>' ? 1 : 0, groupArray(msa)) = '' ? '<msaMissing>' : arrayFirst(x->x!='<msaMissing>' ? 1 : 0, groupArray(msa)) AS msa,
  arrayFirst(x->x!='<zip3Missing>' ? 1 : 0, groupArray(zip3)) = '' ? '<zip3Missing>' : arrayFirst(x->x!='<zip3Missing>' ? 1 : 0, groupArray(zip3)) AS zip3,
  toFloat32(arrayAvg(arrayFilter(x -> x != <miMissing> ? 1 : 0, groupArray(mi))) > 0 ? arrayAvg(arrayFilter(x -> x != <miMissing> ? 1 : 0, groupArray(mi))) : <miMissing>)  AS mi,
  arrayFirst(x->x!='<amTypeMissing>' ? 1 : 0, groupArray(amType)) = '' ? '<amTypeMissing>' : arrayFirst(x->x!='<amTypeMissing>' ? 1 : 0, groupArray(amType)) AS amType,
  indexOf(groupArray(pPen), 'Y') = 0 ? 'Y' : 'N' AS pPen,
  arrayFirst(x->x!='<ioMissing>' ? 1 : 0, groupArray(io)) = '' ? '<ioMissing>' : arrayFirst(x->x!='<ioMissing>' ? 1 : 0, groupArray(io)) AS io,

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
  arrayMax(groupArray(fclProOth)) AS fclProOth,
  toFloat32(arrayAvg(arrayFilter(x -> x != <fclWriteOffMissing> ? 1 : 0, groupArray(fclWriteOff))) > 0 ? arrayAvg(arrayFilter(x -> x != <fclWriteOffMissing> ? 1 : 0, groupArray(fclWriteOff))) : <fclWriteOffMissing>)  AS fclWriteOff,
  arrayFirst(x->x!='<miTypeMissing>' ? 1 : 0, groupArray(miType)) = '' ? '<miTypeMissing>' : arrayFirst(x->x!='<miTypeMissing>' ? 1 : 0, groupArray(miType)) AS miType,
  arrayFirst(x->x!='<reloMissing>' ? 1 : 0, groupArray(relo)) = '' ? '<reloMissing>' : arrayFirst(x->x!='<reloMissing>' ? 1 : 0, groupArray(relo)) AS relo,
  arrayFirst(x->x!='<valMthdMissing>' ? 1 : 0, groupArray(valMthd)) = '' ? '<valMthdMissing>' : arrayFirst(x->x!='<valMthdMissing>' ? 1 : 0, groupArray(valMthd)) AS valMthd,
  arrayFirst(x->x!='<sConformMissing>' ? 1 : 0, groupArray(sConform)) = '' ? '<sConformMissing>' : arrayFirst(x->x!='<sConformMissing>' ? 1 : 0, groupArray(sConform)) AS sConform,
  arrayFirst(x->x!='<hltvMissing>' ? 1 : 0, groupArray(hltv)) = '' ? '<hltvMissing>' : arrayFirst(x->x!='<hltvMissing>' ? 1 : 0, groupArray(hltv)) AS hltv,

  indexOf(groupArray(reprchMw), 'Y') > 0 ? 'Y' : 'N' AS reprchMw,

  arrayFirst(x->x!='<altResMissing>' AND x!='7' AND x!='9' ? 1 : 0, groupArray(altRes)) = '' ? '<altResMissing>' : arrayFirst(x->x!='<altResMissing>' AND x!='7' AND x!='9' ? 1 : 0, groupArray(altRes)) AS altRes,
  toInt32(arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(altResCnt))) > 0 ? arrayAvg(arrayFilter(x -> x > 0 ? 1 : 0, groupArray(altResCnt))) : -1)  AS altResCnt,
  toFloat32(arrayAvg(arrayFilter(x -> x >= 0 ? 1 : 0, groupArray(totDefrl))) > 0 ? arrayAvg(arrayFilter(x -> x >= 0 ? 1 : 0, groupArray(totDefrl))) : -1)  AS totDefrl,
  arrayElement(groupArray(file), 1) AS file,
  arrayFirst(x->x!='<vintageMissing>' ? 1 : 0, groupArray(vintage)) = '' ? '<vintageMissing>' : arrayFirst(x->x!='<vintageMissing>' ? 1 : 0, groupArray(vintage)) AS vintage,
  toFloat32(arrayAvg(arrayFilter(x -> x != <propValMissing> ? 1 : 0, groupArray(propVal))) > 0 ? arrayAvg(arrayFilter(x -> x != <propValMissing> ? 1 : 0, groupArray(propVal))) : <propValMissing>)  AS propVal,
  position(lower(file), 'harp') > 0 ? 'Y' : 'N' AS harp,
  arrayElement(groupArray(standard), 1) AS standard,

  arrayElement(groupArray(nsDoc), 1) AS nsDoc,
  arrayElement(groupArray(nsUw), 1) AS nsUw,
  arrayElement(groupArray(gGuar), 1) AS gGuar,
  arrayElement(groupArray(negAm), 1) AS negAm
FROM 
  (SELECT 
    *,
    year(fpDt) > 1990 ? dateDiff('month', fpDt, month) : -1000 AS ageFpDt
  FROM  
    sourceTable as z 
  ORDER BY lnId, month
  LIMIT 10)
GROUP BY lnId)
select
  r.*,
  toInt32(modulo(arraySum(bitPositionsToArray(reinterpretAsUInt64(substr(r.lnId, 5, 8)))), 20)) AS bucket,
  v.harpLnId,
  x.oldLnId AS preHarpId,
  q.qa AS field,
  q.nqa AS cntFail,
  arrayFilter((x,y) -> y=length(month) ? 1 : 0, qa, nqa) AS allFail
FROM
  r 
LEFT JOIN
  harpTable as v
ON r.lnId=v.oldLnId
LEFT JOIN
  harpTable AS x
ON r.lnId = x.harpLnId 
LEFT JOIN q 
ON q.lnId = r.lnId
`
