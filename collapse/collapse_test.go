package collapse

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/invertedv/chutils"
	"log"
	"strings"
)

func ExampleGroupBy() {
	var con *chutils.Connect
	con, err := chutils.NewConnect("127.0.0.1", "tester", "testGoNow", clickhouse.Settings{})
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if con.Close() != nil {
			log.Fatalln(err)
		}
	}()
	rows, err := con.Query("DESCRIBE mtg.fannie")
	if err != nil {
		log.Fatalln(err)
	}
	var name, ftype, defaultType, defaultExpression, comment, n1, n2 string
	for rows.Next() {
		if e := rows.Scan(&name, &ftype, &defaultType, &defaultExpression, &comment, &n1, &n2); e != nil {
			log.Fatalln(e)
		}
		s := strings.TrimRight(fmt.Sprintf("%-20s %-31s %s", name, ftype, comment), " ")
		fmt.Println(s)

	}
	// Output:
	//lnId                 String                          Loan ID, missing=error
	//monthly.month        Array(Date)                     month of data, missing=1970/1/1
	//monthly.upb          Array(Float32)                  unpaid balance, missing=-1
	//monthly.dq           Array(Int32)                    months delinquent
	//monthly.servicer     Array(LowCardinality(String))   name of servicer, missing=unknown
	//monthly.curRate      Array(Float32)                  current note rate, missing=-1
	//monthly.age          Array(Int32)                    loan age based on origination date, missing=-1
	//monthly.ageFpDt      Array(Int64)                    age based on fdDt, missing=-1000
	//monthly.rTermLgl     Array(Int32)                    remaining legal term, missing=-1
	//monthly.rTermAct     Array(Int32)                    remaining actual term, missing=-1
	//monthly.dqStat       Array(FixedString(3))           DQ status code: 0-99 months, missing=!
	//monthly.mod          Array(FixedString(1))           modification flag: Y, N , missing=X
	//monthly.zb           Array(FixedString(2))           zero balance:00(noop), 01(pp), 03(short), 06(repurch), 09(REO), 96/97/98(removal), 02/15/16(sale), missing=X
	//monthly.ioRem        Array(Int32)                    number of IO months remaining, missing=-1
	//monthly.bap          Array(FixedString(1))           borrower assistant plan: F(forebearance), R(repayment), T(trial), O(Other), N(none), 7,9(NA) missing=X
	//monthly.program      Array(FixedString(1))           fannie program: Y (home ready) N (no program), missing=X
	//monthly.nonIntUpb    Array(Float32)                  interest bearing UPB, missing=-1
	//monthly.frgvUpb      Array(Float32)                  forgiven UPB, missing=-1
	//monthly.totPrin      Array(Float32)                  delta upb
	//monthly.servAct      Array(FixedString(1))           servicing activity: Y, N, missing=X
	//monthly.matDt        Array(Date)                     loan maturity date (initial), missing=1970/1/1
	//channel              FixedString(1)                  acquisition channel: B, R, C, missing=X
	//seller               LowCardinality(String)          name of seller, missing=unknown
	//dti                  Float32                         dti at origination, 1-65, missing=-1
	//rate                 Float32                         note rate at origination, 0-15, missing=-1
	//opb                  Float32                         balance at origination, missing=-1
	//term                 Float32                         loan term at origination, missing=-1
	//origDt               Date                            first payment date, missing=1970/1/1
	//fpDt                 Date                            first payment date, missing=1970/1/1
	//ltv                  Float32                         ltv at origination, 1-998, missing=-1
	//cltv                 Float32                         combined cltv at origination, 1-998, missing=-1
	//numBorr              Int32                           number of borrowers, 1-10, missing=-1
	//fico                 Int32                           fico at origination, 301-850, missing=-1
	//coFico               Int32                           coborrower fico at origination, 301-850, missing=-1
	//firstTime            FixedString(1)                  first time homebuyer: Y, N, missing=X
	//purpose              FixedString(1)                  loan purpose: P (purch), C (cash out refi), U (rate/term refi) R (refi), missing=X
	//propType             FixedString(2)                  property type: SF (single family), CO (condo), PU (PUD), CP (coop), MH (manufactured), missing=XX
	//units                Float32                         # of units in the property, 1-4, missing=-1
	//occ                  FixedString(1)                  property occupancy: P (primary), S (secondary), I (investor) missing=X
	//state                FixedString(2)                  property state postal abbreviation, missing=XX
	//msa                  FixedString(5)                  msa/division code, missing/not in MSA=XXXXX
	//zip3                 FixedString(3)                  3-digit zip , missing=XXX
	//mi                   Float32                         mi percentage, 0-55, missing=-1
	//amType               FixedString(3)                  amortization type: FRM, ARM, missing=XXX
	//pPen                 FixedString(1)                  prepay penalty flag: Y, N, missing=X
	//io                   FixedString(1)                  io Flag: Y, N, missing=X
	//ioDt                 Date                            month IO loan starts amortizing, missing=1970/1/1
	//zbDt                 Date                            zero balance date, missing=1970/1/1
	//zbUpb                Float32                         UPB just prior to zero balance, missing=-1
	//lpDt                 Date                            last pay date, missing=1970/1/1
	//fclDt                Date                            foreclosure date, missing=1970/1/1
	//dispDt               Date                            date Fannie is done with loan, missing=1970/1/1
	//fclExp               Float32                         total foreclosure expenses
	//fclPExp              Float32                         foreclosure property preservation expenses
	//fclLExp              Float32                         foreclosure recovery legal expenses
	//fclMExp              Float32                         foreclosure misc expenses, missing=-1
	//fclTaxes             Float32                         foreclosure property taxes and insurance
	//fclProNet            Float32                         foreclosure net proceeds
	//fclProMi             Float32                         foreclosure credit enhancement proceeds
	//fclProMw             Float32                         foreclosure make whole proceeds
	//fclProOth            Float32                         foreclosure other proceeds
	//fclWriteOff          Float32                         foreclosure principal writeoff, missing=-1
	//miType               FixedString(1)                  mi type: 1=borrower, 2=lender, 3=enterprise, 0=No MI, missing=X
	//relo                 FixedString(1)                  relocation mortgage: Y, N, missing=X
	//valMthd              FixedString(1)                  property value method A(apprsl), P(onsite), R(GSE target), W(waived), O(other), missing=X
	//sConform             FixedString(1)                  super conforming flag: Y, N, missing=X
	//hltv                 FixedString(1)                  high LTV refi missing=X
	//reprchMw             FixedString(1)                  repurchase make whole: Y, N missing=X
	//altRes               FixedString(1)                  DQ payment deferral: P(payment), C(Covid), D(disaster), 7/9 :NA missing=X
	//altResCnt            Int32                           # of alternate resolutions (deferrals), missing=-1
	//totDefrl             Float32                         total amount deferred, missing=-1
	//file                 LowCardinality(String)          source file
	//vintage              LowCardinality(FixedString(6))  vintage (from fpDt)
	//propVal              Float32                         property value at origination
	//harp                 FixedString(1)                  loan is HARP: Y, N
	//standard             LowCardinality(FixedString(1))  standard u/w process loan: Y, N
	//nsDoc                FixedString(1)                  non-standard documentation: Y, N, missing=X
	//nsUw                 FixedString(1)                  non-standard underwriting: Y, N, missing=X
	//gGuar                FixedString(1)                  government issued/guaranteed: Y, N, missing=X
	//negAm                FixedString(1)                  loan can neg am: Y, N, missing=X
	//harpLnId             String                          loan refinanced to this HARP loan
	//preHarpId            String                          HARP loan refinanced from this loan
	//qa.field             Array(LowCardinality(String))   field name
	//qa.cntFail           Array(Int32)                    # of months field failed qa
	//allFail              Array(LowCardinality(String))   fields that failed QA all months
}
