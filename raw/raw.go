package raw

import (
	"fmt"
	"github.com/invertedv/chutils"
	"github.com/invertedv/chutils/file"
	"github.com/invertedv/chutils/nested"
	s "github.com/invertedv/chutils/sql"
	"os"
	"strconv"
	"time"
)

// TableDef is TableDef for the source table.  It is exported as other packages (e.g. joined) may need fields from
// it (e.g. Description)
var TableDef *chutils.TableDef

func LoadRaw(sourceFile string, table string, create bool, nConcur int, con *chutils.Connect) (err error) {

	fileName = sourceFile

	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	rdr := file.NewReader(fileName, '|', '\n', '"', 0, 0, 0, f, 6000000)
	rdr.Skip = 0
	defer func() {
		// don't throw an error if we already have one
		if e := rdr.Close(); e != nil && err == nil {
			err = e
		}
	}()
	// rdr is the base reader the slice of readers is based on
	rdr.SetTableSpec(Build())

	// build slice of readers
	rdrs, err := file.Rdrs(rdr, nConcur)
	if err != nil {
		return
	}

	var wrtrs []chutils.Output
	// build a slice of writers
	if wrtrs, err = s.Wrtrs(table, nConcur, con); err != nil {
		return
	}

	newCalcs := make([]nested.NewCalcFn, 0)
	newCalcs = append(newCalcs, vField, fField, dqField, vintField, pvField)

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
	TableDef = rdrsn[0].TableSpec()

	err = chutils.Concur(12, rdrsn, wrtrs, 400000)
	return
}

// xtraFields defines additional fields for the nested reader
func xtraFields() (fds []*chutils.FieldDef) {
	vfd := &chutils.FieldDef{
		Name:        "qa",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "validation results for each field: 0=pass, 1=fail",
		Legal:       chutils.NewLegalValues(),
		Missing:     "!",
		Width:       0,
	}
	ffd := &chutils.FieldDef{
		Name:        "file",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "file monthly data loaded from",
		Legal:       chutils.NewLegalValues(),
		Missing:     "!",
		Width:       0,
	}
	dqfd := &chutils.FieldDef{
		Name:        "dq",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "months delinquent",
		Legal:       &chutils.LegalValues{LowLimit: int32(0), HighLimit: int32(999)},
		Missing:     int32(-1),
		Width:       0,
	}
	vintfd := &chutils.FieldDef{
		Name:        "vintage",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "vintage (from fpDt)",
		Legal:       chutils.NewLegalValues(),
		Missing:     "!",
		Width:       0,
	}
	pvfd := &chutils.FieldDef{
		Name:        "propVal",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "property value at origination",
		Legal:       &chutils.LegalValues{LowLimit: float32(1000.0), HighLimit: float32(5000000.0), Levels: nil},
		Missing:     float32(-1.0),
		Width:       0,
	}
	fds = []*chutils.FieldDef{vfd, ffd, dqfd, vintfd, pvfd}
	return
}

// vField returns the validation results for each field -- 0 = pass, 1 = fail in a string which has a  keyval format
func vField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	res := make([]byte, 0)
	for ind, v := range valid {
		name := td.FieldDefs[ind].Name

		vx := v
		// The data contains some deprecated MSAs -- update these
		if name == "msa" {
			switch data[ind].(string) {
			case "31100":
				data[ind], vx = "31080", chutils.VPass
			case "29140":
				data[ind], vx = "29200", chutils.VPass
			case "26100":
				data[ind], vx = "24340", chutils.VPass
			case "37380":
				data[ind], vx = "19660", chutils.VPass
			case "37700":
				data[ind], vx = "25060", chutils.VPass
			case "44600":
				data[ind], vx = "48540", chutils.VPass
			case "14060":
				data[ind], vx = "14010", chutils.VPass
			case "42060":
				data[ind], vx = "42020", chutils.VPass
			case "21940":
				data[ind], vx = "41980", chutils.VPass
			case "11340":
				data[ind], vx = "24860", chutils.VPass
			case "26180":
				data[ind], vx = "25900", chutils.VPass
			case "19380":
				data[ind], vx = "19430", chutils.VPass
			case "11300":
				data[ind], vx = "26900", chutils.VPass
			case "39140":
				data[ind], vx = "39150", chutils.VPass
			}
		}

		switch vx {
		case chutils.VPass, chutils.VDefault:
			res = append(res, []byte(name+":0;")...)
		default:
			res = append(res, []byte(name+":1;")...)
		}
	}
	// delete trailing ;
	res[len(res)-1] = ' '
	return string(res), nil
}

// fileName is global since used as a closure to fField
var fileName string

// fField returns the name of the file we're loading
func fField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return fileName, nil
}

// dqField returns the delinquency level as an integer
func dqField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	ind, _, err := td.Get("dqStat")
	if err != nil {
		return nil, err
	}
	dq, err := strconv.ParseInt(data[ind].(string), 10, 32)
	if err != nil {
		return -1, nil
	}
	return int32(dq), nil
}

// fField adds the file name data comes from to output table
func vintField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	ind, _, err := td.Get("fpDt")
	if err != nil {
		return nil, err
	}
	fpd := data[ind].(time.Time)
	var qtr = int((fpd.Month()-1)/3 + 1)
	vintage := fmt.Sprintf("%dQ%d", fpd.Year(), qtr)
	return vintage, nil
}

// pvField calculate property value from ltv
func pvField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	ltvInd, _, err := td.Get("ltv")
	if err != nil {
		return nil, err
	}
	if valid[ltvInd] != chutils.VPass {
		return -1.0, nil
	}
	ltv := float32(data[ltvInd].(int32))

	opbInd, _, err := td.Get("opb")
	if valid[opbInd] != chutils.VPass {
		return -1.0, nil
	}
	if err != nil {
		return nil, err
	}
	opb := data[opbInd].(float32)
	return opb / (ltv / 100.0), nil
}

// build builds the TableDef for the static field files.
func Build() *chutils.TableDef {
	var (
		// date ranges & missing value
		minDt  = time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
		nowDt  = time.Now()
		futDt  = time.Now().AddDate(40, 0, 0)
		missDt = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

		strMiss = "X" // generic missing value for FixedString(1)
		fltMiss = float32(0.0)

		lnIdMiss = "error"

		monthMin, monthMax, monthMiss = minDt, nowDt, missDt
		channelMiss                   = strMiss
		channelLvl                    = []string{"B", "R", "C"} // freddie as T as well

		sellerMiss    = "unknown"
		servicerMiss  = "unknown"
		mServicerMiss = "unknown"

		rateMin, rateMax, rateMiss          = float32(0.0), float32(15.0), float32(-1.0)
		curRateMin, curRateMax, curRateMiss = float32(0.0), float32(15.0), float32(-1.0)
		opbMin, opbMax, opbMiss             = float32(1000.0), float32(2000000.0), float32(-1.0)

		upbMin, upbMax, upbMiss    = float32(0.0), float32(2000000.0), float32(-1.0)
		termMin, termMax, termMiss = int32(1), int32(700), int32(-1)

		origDtMin, origDtMax, origDtMiss       = minDt, nowDt, missDt
		fpDtMin, fpDtMax, fpDtMiss             = minDt, nowDt, missDt
		ageMin, ageMax, ageMiss                = int32(0), int32(600), int32(-1)
		rTermLglMin, rTermLglMax, rTermLglMiss = int32(0), int32(600), int32(-1)
		rTermActMin, rTermActMax, rTermActMiss = int32(0), int32(600), int32(-1)

		matDtMin, matDtMax, matDtMiss       = minDt, futDt, missDt
		ltvMin, ltvMax, ltvMiss             = int32(1), int32(998), int32(-1)
		cltvMin, cltvMax, cltvMiss, cltvDef = int32(1), int32(998), int32(-1), int32(-1)

		numBorrMin, numBorrMax, numBorrMiss         = int32(1), int32(10), int32(-1)
		dtiMin, dtiMax, dtiMiss                     = int32(1), int32(65), int32(-1)
		ficoMin, ficoMax, ficoMiss                  = int32(301), int32(850), int32(-1)
		coFicoMin, coFicoMax, coFicoMiss, coFicoDef = int32(301), int32(850), int32(-1), int32(0)

		firstTimeMiss = strMiss
		firstTimeLvl  = []string{"Y", "N"}

		purposeMiss = strMiss
		purposeLvl  = []string{"P", "C", "U", "R"}

		propTypeMiss = "XX"
		propTypeLvl  = []string{"SF", "CO", "CP", "MH", "PU"}

		unitMin, unitMax, unitMiss = int32(1), int32(4), int32(-1)

		occMiss = strMiss
		occLvl  = []string{"P", "S", "I"}

		stateMiss = "XX"
		stateLvl  = []string{"AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "GU", "HI", "IA", "ID",
			"IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND",
			"NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN",
			"TX", "UT", "VA", "VI", "VT", "WA", "WI", "WV", "WY"}

		msaMiss, msaDef = "XXXXX", "00000"
		//┌─old_msa─┬─new_msa─┐
		//│ 31100   │ 31080   │
		//│ 29140   │ 29200   │
		//│ 26100   │ 24340   │
		//│ 37380   │ 19660   │
		//│ 37700   │ 25060   │
		//│ 44600   │ 48540   │
		//│ 14060   │ 14010   │
		//│ 42060   │ 42020   │
		//│ 21940   │ 41980   │
		//│ 11340   │ 24860   │
		//│ 26180   │ 25900   │
		//│ 19380   │ 19430   │
		//│ 11300   │ 26900   │
		//│ 39140   │ 39150   │
		msaLvl = []string{
			msaDef, "10100", "10140", "10180", "10220", "10300", "10380", "10420", "10460", "10500",
			"10540", "10580", "10620", "10660", "10700", "10740", "10760", "10780", "10820", "10860",
			"10900", "10940", "10980", "11020", "11060", "11100", "11140", "11180", "11220", "11260",
			"11380", "11420", "11460", "11500", "11540", "11580", "11620", "11640", "11660", "11700",
			"11740", "11780", "11820", "11860", "11900", "11940", "11980", "12020", "12060", "12100",
			"12120", "12140", "12180", "12220", "12260", "12300", "12380", "12420", "12460", "12540",
			"12580", "12620", "12660", "12680", "12700", "12740", "12780", "12860", "12900", "12940",
			"12980", "13020", "13060", "13100", "13140", "13180", "13220", "13260", "13300", "13340",
			"13380", "13420", "13460", "13500", "13540", "13620", "13660", "13700", "13720", "13740",
			"13780", "13820", "13900", "13940", "13980", "14010", "14020", "14100", "14140", "14180",
			"14220", "14260", "14300", "14380", "14420", "14460", "14500", "14540", "14580", "14620",
			"14660", "14700", "14720", "14740", "14780", "14820", "14860", "15020", "15060", "15100",
			"15140", "15180", "15220", "15260", "15340", "15380", "15420", "15460", "15500", "15540",
			"15580", "15620", "15660", "15680", "15700", "15740", "15780", "15820", "15860", "15940",
			"15980", "16020", "16060", "16100", "16140", "16180", "16220", "16260", "16300", "16340",
			"16380", "16420", "16460", "16500", "16540", "16580", "16620", "16660", "16700", "16740",
			"16820", "16860", "16940", "16980", "17020", "17060", "17140", "17220", "17260", "17300",
			"17340", "17380", "17420", "17460", "17500", "17540", "17580", "17620", "17640", "17660",
			"17700", "17740", "17780", "17820", "17860", "17900", "17980", "18020", "18060", "18100",
			"18140", "18180", "18220", "18260", "18300", "18380", "18420", "18460", "18500", "18580",
			"18620", "18660", "18700", "18740", "18780", "18820", "18860", "18880", "18900", "18980",
			"19000", "19060", "19100", "19140", "19180", "19220", "19260", "19300", "19340", "19420",
			"19430", "19460", "19500", "19540", "19580", "19620", "19660", "19700", "19740", "19760",
			"19780", "19820", "19860", "19940", "19980", "20020", "20060", "20100", "20140", "20180",
			"20220", "20260", "20300", "20340", "20420", "20460", "20500", "20540", "20580", "20660",
			"20700", "20740", "20780", "20820", "20900", "20940", "20980", "21020", "21060", "21120",
			"21140", "21180", "21220", "21260", "21300", "21340", "21380", "21420", "21460", "21500",
			"21540", "21580", "21640", "21660", "21700", "21740", "21780", "21820", "21840", "21860",
			"21900", "21980", "22020", "22060", "22100", "22140", "22180", "22220", "22260", "22280",
			"22300", "22340", "22380", "22420", "22500", "22520", "22540", "22580", "22620", "22660",
			"22700", "22780", "22800", "22820", "22840", "22860", "22900", "23060", "23140", "23180",
			"23240", "23300", "23340", "23380", "23420", "23460", "23500", "23540", "23580", "23620",
			"23660", "23700", "23780", "23820", "23860", "23900", "23940", "23980", "24020", "24060",
			"24100", "24140", "24180", "24220", "24260", "24300", "24330", "24340", "24380", "24420",
			"24460", "24500", "24540", "24580", "24620", "24660", "24700", "24740", "24780", "24820",
			"24860", "24900", "24940", "24980", "25020", "25060", "25100", "25180", "25200", "25220",
			"25260", "25300", "25420", "25460", "25500", "25540", "25580", "25620", "25700", "25720",
			"25740", "25760", "25780", "25820", "25840", "25860", "25880", "25900", "25940", "25980",
			"26020", "26090", "26140", "26220", "26260", "26300", "26340", "26380", "26420", "26460",
			"26500", "26540", "26580", "26620", "26660", "26700", "26740", "26780", "26820", "26860",
			"26900", "26940", "26980", "27020", "27060", "27100", "27140", "27160", "27180", "27220",
			"27260", "27300", "27340", "27380", "27420", "27460", "27500", "27530", "27540", "27580",
			"27600", "27620", "27660", "27700", "27740", "27780", "27860", "27900", "27940", "27980",
			"28020", "28060", "28100", "28140", "28180", "28260", "28300", "28340", "28380", "28420",
			"28500", "28540", "28580", "28620", "28660", "28700", "28740", "28780", "28820", "28860",
			"28900", "28940", "29020", "29060", "29100", "29180", "29200", "29260", "29300", "29340",
			"29380", "29420", "29460", "29500", "29540", "29620", "29660", "29700", "29740", "29780",
			"29820", "29860", "29900", "29940", "29980", "30020", "30060", "30100", "30140", "30220",
			"30260", "30280", "30300", "30340", "30380", "30420", "30460", "30580", "30620", "30660",
			"30700", "30780", "30820", "30860", "30900", "30940", "30980", "31020", "31060", "31080",
			"31140", "31180", "31220", "31260", "31300", "31340", "31380", "31420", "31460", "31500",
			"31540", "31580", "31620", "31660", "31680", "31700", "31740", "31820", "31860", "31900",
			"31930", "31940", "31980", "32000", "32020", "32100", "32140", "32180", "32260", "32280",
			"32300", "32340", "32380", "32420", "32460", "32500", "32540", "32580", "32620", "32660",
			"32700", "32740", "32780", "32820", "32860", "32900", "32940", "33020", "33060", "33100",
			"33140", "33180", "33220", "33260", "33300", "33340", "33380", "33420", "33460", "33500",
			"33540", "33580", "33620", "33660", "33700", "33740", "33780", "33860", "33940", "33980",
			"34020", "34060", "34100", "34140", "34180", "34220", "34260", "34300", "34340", "34350",
			"34380", "34420", "34460", "34500", "34540", "34580", "34620", "34660", "34700", "34740",
			"34780", "34820", "34860", "34900", "34940", "34980", "35020", "35060", "35100", "35140",
			"35220", "35260", "35300", "35380", "35420", "35440", "35460", "35580", "35620", "35660",
			"35700", "35740", "35820", "35840", "35860", "35900", "35940", "35980", "36020", "36100",
			"36140", "36220", "36260", "36300", "36340", "36380", "36420", "36460", "36500", "36540",
			"36580", "36620", "36660", "36700", "36740", "36780", "36820", "36830", "36837", "36840",
			"36900", "36940", "36980", "37060", "37100", "37120", "37140", "37220", "37260", "37300",
			"37340", "37420", "37460", "37500", "37540", "37580", "37620", "37660", "37740", "37770",
			"37780", "37800", "37860", "37900", "37940", "37980", "38060", "38100", "38180", "38220",
			"38240", "38260", "38300", "38340", "38380", "38420", "38460", "38500", "38540", "38580",
			"38620", "38660", "38700", "38740", "38780", "38820", "38860", "38900", "38920", "38940",
			"39020", "39060", "39100", "39150", "39220", "39260", "39300", "39340", "39380", "39420",
			"39460", "39500", "39540", "39580", "39660", "39700", "39740", "39780", "39820", "39860",
			"39900", "39940", "39980", "40060", "40080", "40100", "40140", "40180", "40220", "40260",
			"40300", "40340", "40380", "40420", "40460", "40530", "40540", "40580", "40620", "40660",
			"40700", "40740", "40760", "40780", "40820", "40860", "40900", "40940", "40980", "41060",
			"41100", "41140", "41180", "41220", "41260", "41400", "41420", "41460", "41500", "41540",
			"41620", "41660", "41700", "41740", "41760", "41780", "41820", "41860", "41900", "41940",
			"41980", "42020", "42100", "42140", "42180", "42200", "42220", "42300", "42340", "42380",
			"42420", "42460", "42500", "42540", "42620", "42660", "42680", "42700", "42740", "42780",
			"42820", "42860", "42900", "42940", "42980", "43020", "43060", "43100", "43140", "43180",
			"43220", "43260", "43300", "43320", "43340", "43380", "43420", "43460", "43500", "43580",
			"43620", "43660", "43700", "43740", "43760", "43780", "43900", "43940", "43980", "44020",
			"44060", "44100", "44140", "44180", "44220", "44260", "44300", "44340", "44420", "44460",
			"44500", "44540", "44580", "44620", "44660", "44700", "44740", "44780", "44860", "44900",
			"44940", "44980", "45000", "45020", "45060", "45140", "45180", "45220", "45300", "45340",
			"45380", "45460", "45500", "45520", "45540", "45580", "45620", "45660", "45700", "45740",
			"45780", "45820", "45860", "45900", "45940", "45980", "46020", "46060", "46100", "46140",
			"46180", "46220", "46300", "46340", "46380", "46420", "46460", "46500", "46520", "46540",
			"46620", "46660", "46700", "46780", "46820", "46860", "46900", "46980", "47020", "47080",
			"47180", "47220", "47240", "47260", "47300", "47340", "47380", "47420", "47460", "47540",
			"47580", "47620", "47660", "47700", "47780", "47820", "47900", "47920", "47940", "47980",
			"48020", "48060", "48100", "48140", "48180", "48220", "48260", "48300", "48460", "48500",
			"48540", "48580", "48620", "48660", "48700", "48780", "48820", "48900", "48940", "48980",
			"49020", "49060", "49080", "49100", "49180", "49220", "49260", "49300", "49340", "49380"}

		zip3Miss = "000"
		zip3Lvl  = []string{"005", "006", "007", "008", "009", "010", "011", "012", "013", "014", "015", "016",
			"017", "018", "019", "020", "021", "022", "023", "024", "025", "026", "027", "028",
			"029", "030", "031", "032", "033", "034", "035", "036", "037", "038", "039", "040",
			"041", "042", "043", "044", "045", "046", "047", "048", "049", "050", "051", "052",
			"053", "054", "055", "056", "057", "058", "059", "060", "061", "062", "063", "064",
			"065", "066", "067", "068", "069", "070", "071", "072", "073", "074", "075", "076",
			"077", "078", "079", "080", "081", "082", "083", "084", "085", "086", "087", "088",
			"089", "092", "095", "100", "101", "102", "103", "104", "105", "106", "107", "108",
			"109", "110", "111", "112", "113", "114", "115", "116", "117", "118", "119", "120",
			"121", "122", "123", "124", "125", "126", "127", "128", "129", "130", "131", "132",
			"133", "134", "135", "136", "137", "138", "139", "140", "141", "142", "143", "144",
			"145", "146", "147", "148", "149", "150", "151", "152", "153", "154", "155", "156",
			"157", "158", "159", "160", "161", "162", "163", "164", "165", "166", "167", "168",
			"169", "170", "171", "172", "173", "174", "175", "176", "177", "178", "179", "180",
			"181", "182", "183", "184", "185", "186", "187", "188", "189", "190", "191", "192",
			"193", "194", "195", "196", "197", "198", "199", "200", "201", "202", "203", "204",
			"205", "206", "207", "208", "209", "210", "211", "212", "213", "214", "215", "216",
			"217", "218", "219", "220", "221", "222", "223", "224", "225", "226", "227", "228",
			"229", "230", "231", "232", "233", "234", "235", "236", "237", "238", "239", "240",
			"241", "242", "243", "244", "245", "246", "247", "248", "249", "250", "251", "252",
			"253", "254", "255", "256", "257", "258", "259", "260", "261", "262", "263", "264",
			"265", "266", "267", "268", "270", "271", "272", "273", "274", "275", "276", "277",
			"278", "279", "280", "281", "282", "283", "284", "285", "286", "287", "288", "289",
			"290", "291", "292", "293", "294", "295", "296", "297", "298", "299", "300", "301",
			"302", "303", "304", "305", "306", "307", "308", "309", "310", "311", "312", "313",
			"314", "315", "316", "317", "318", "319", "320", "321", "322", "323", "324", "325",
			"326", "327", "328", "329", "330", "331", "332", "333", "334", "335", "336", "337",
			"338", "339", "341", "342", "343", "344", "345", "346", "347", "348", "349", "350",
			"351", "352", "354", "355", "356", "357", "358", "359", "360", "361", "362", "363",
			"364", "365", "366", "367", "368", "369", "370", "371", "372", "373", "374", "375",
			"376", "377", "378", "379", "380", "381", "382", "383", "384", "385", "386", "387",
			"388", "389", "390", "391", "392", "393", "394", "395", "396", "397", "398", "400",
			"401", "402", "403", "404", "405", "406", "407", "408", "409", "410", "411", "412",
			"413", "414", "415", "416", "417", "418", "420", "421", "422", "423", "424", "425",
			"426", "427", "430", "431", "432", "433", "434", "435", "436", "437", "438", "439",
			"440", "441", "442", "443", "444", "445", "446", "447", "448", "449", "450", "451",
			"452", "453", "454", "455", "456", "457", "458", "460", "461", "462", "463", "464",
			"465", "466", "467", "468", "469", "470", "471", "472", "473", "474", "475", "476",
			"477", "478", "479", "480", "481", "482", "483", "484", "485", "486", "487", "488",
			"489", "490", "491", "492", "493", "494", "495", "496", "497", "498", "499", "500",
			"501", "502", "503", "504", "505", "506", "507", "508", "510", "511", "512", "513",
			"514", "515", "516", "518", "520", "521", "522", "523", "524", "525", "526", "527",
			"528", "530", "531", "532", "533", "534", "535", "536", "537", "538", "539", "540",
			"541", "542", "543", "544", "545", "546", "547", "548", "549", "550", "551", "552",
			"553", "554", "555", "556", "557", "558", "559", "560", "561", "562", "563", "564",
			"565", "566", "567", "570", "571", "572", "573", "574", "575", "576", "577", "580",
			"581", "582", "583", "584", "585", "586", "587", "588", "590", "591", "592", "593",
			"594", "595", "596", "597", "598", "599", "600", "601", "602", "603", "604", "605",
			"606", "607", "608", "609", "610", "611", "612", "613", "614", "615", "616", "617",
			"618", "619", "620", "622", "623", "624", "625", "626", "627", "628", "629", "630",
			"631", "632", "633", "634", "635", "636", "637", "638", "639", "640", "641", "642",
			"644", "645", "646", "647", "648", "649", "650", "651", "652", "653", "654", "655",
			"656", "657", "658", "660", "661", "662", "664", "665", "666", "667", "668", "669",
			"670", "671", "672", "673", "674", "675", "676", "677", "678", "679", "680", "681",
			"683", "684", "685", "686", "687", "688", "689", "690", "691", "692", "693", "700",
			"701", "703", "704", "705", "706", "707", "708", "710", "711", "712", "713", "714",
			"716", "717", "718", "719", "720", "721", "722", "723", "724", "725", "726", "727",
			"728", "729", "730", "731", "733", "734", "735", "736", "737", "738", "739", "740",
			"741", "743", "744", "745", "746", "747", "748", "749", "750", "751", "752", "753",
			"754", "755", "756", "757", "758", "759", "760", "761", "762", "763", "764", "765",
			"766", "767", "768", "769", "770", "772", "773", "774", "775", "776", "777", "778",
			"779", "780", "781", "782", "783", "784", "785", "786", "787", "788", "789", "790",
			"791", "792", "793", "794", "795", "796", "797", "798", "799", "800", "801", "802",
			"803", "804", "805", "806", "807", "808", "809", "810", "811", "812", "813", "814",
			"815", "816", "820", "821", "822", "823", "824", "825", "826", "827", "828", "829",
			"830", "831", "832", "833", "834", "835", "836", "837", "838", "840", "841", "842",
			"843", "844", "845", "846", "847", "850", "851", "852", "853", "854", "855", "856",
			"857", "858", "859", "860", "862", "863", "864", "865", "870", "871", "872", "873",
			"874", "875", "876", "877", "878", "879", "880", "881", "882", "883", "884", "889",
			"890", "891", "892", "893", "894", "895", "897", "898", "900", "901", "902", "903",
			"904", "905", "906", "907", "908", "910", "911", "912", "913", "914", "915", "916",
			"917", "918", "919", "920", "921", "922", "923", "924", "925", "926", "927", "928",
			"929", "930", "931", "932", "933", "934", "935", "936", "937", "939", "940", "941",
			"942", "943", "944", "945", "946", "947", "948", "949", "950", "951", "952", "953",
			"954", "955", "956", "957", "958", "959", "960", "961", "962", "963", "964", "965",
			"966", "967", "968", "969", "970", "971", "972", "973", "974", "975", "976", "977",
			"978", "979", "980", "981", "982", "983", "984", "985", "986", "987", "988", "989",
			"990", "991", "992", "993", "994", "995", "996", "997", "998", "999"}

		miMin, miMax, miMiss, miDef = float32(0), float32(55), float32(-1), float32(0)

		amTypeMiss = "XXX"
		amTypeLvl  = []string{"FRM", "ARM"}

		ppenMiss = strMiss
		pPenLvl  = []string{"Y", "N"}

		ioMiss = strMiss
		ioDef  = "N"
		ioLvl  = []string{"Y", "N"}

		ioDtMin, ioDtMax, ioDtMiss, ioDtDef     = minDt, futDt, missDt, missDt
		ioRemMin, ioRemMax, ioRemMiss, ioRemDef = int32(0), int32(700), int32(-1), int32(-1)

		dqStatMiss = "!"
		dqStatLvl  = make([]string, 0)

		payHistMiss = strMiss

		modMiss = strMiss
		modLvl  = []string{"Y", "P", "N"}

		zbMiss = strMiss
		zbDef  = "00"
		zbLvl  = []string{"01", "02", "03", "06", "09", "15", "16", "96", "97", "98", ""}

		zbDtMin, zbDtMax, zbDtMiss, zbDtDef     = minDt, nowDt, missDt, missDt
		zbUpbMin, zbUpbMax, zbUpbMiss, zbUpbDef = float32(0.0), float32(2000000.0), float32(-1.0), float32(0.0)

		lpDtMin, lpDtMax, lpDtMiss, lpDtDef         = minDt, nowDt, missDt, missDt
		fclDtMin, fclDtMax, fclDtMiss, fclDtDef     = minDt, nowDt, missDt, missDt
		dispDtMin, dispDtMax, dispDtMiss, dispDtDef = minDt, nowDt, missDt, missDt

		fclExpMin, fclExpMax, fclExpMiss, fclExpDef                     = float32(0.0), float32(200000.0), float32(0.0), float32(0.0)
		fclLExpMin, fclLExpMax, fclLExpMiss, fclLExpDef                 = float32(-2000000.0), float32(2000000.0), float32(-1.0), float32(0.0)
		fclPExpMin, fclPExpMax, fclPExpMiss, fclPExpDef                 = float32(-2000000.0), float32(2000000.0), float32(-1.0), float32(0.0)
		fclMExpMin, fclMExpMax, fclMExpMiss, fclMExpDef                 = float32(-2000000.0), float32(2000000.0), float32(-1.0), float32(0.0)
		fclTaxesMin, fclTaxesMax, fclTaxesMiss, fclTaxesDef             = float32(-2000000.0), float32(2000000.0), float32(-1.0), float32(0.0)
		fclWriteOffMin, fclWriteOffMax, fclWriteOffMiss, fclWriteOffDef = float32(-2000000.0), float32(2000000.0), float32(-1.0), float32(0.0)

		fclProMiMin, fclProMiMax, fclProMiMiss, fclProMiDef     = float32(-2000000.0), float32(2000000.0), float32(-1.0), float32(0.0)
		fclProNetMin, fclProNetMax, fclProNetMiss, fclProNetDef = float32(-2000000.0), float32(2000000.0), float32(-1.0), float32(0.0)
		fclProMwMin, fclProMwMax, fclProMwMiss, fclProMwDef     = float32(-2000000.0), float32(2000000.0), float32(-1.0), float32(0.0)
		fclProOthMin, fclProOthMax, fclProOthMiss, fclProOthDef = float32(-2000000.0), float32(2000000.0), float32(-1.0), float32(0.0)

		nonIntUpbMin, nonIntUpbMax, nonIntUpbMiss, nonIntUpbDef = float32(0.0), float32(2000000.0), float32(-1.0), float32(0.0)
		frgvUpbMin, frgvUpbMax, frgvUpbMiss, frgvUpbDef         = float32(0.0), float32(200000.0), float32(-1.0), float32(0.0)

		miTypeMiss, miTypeDef = strMiss, "0"
		miTypeLvl             = []string{"1", "2", "3", ""}

		servActMiss = strMiss
		servActLvl  = []string{"Y", "N"}

		programMiss, programDef = strMiss, "N"
		programLvl              = []string{"Y", "N"}

		reloMiss = strMiss
		reloLvl  = []string{"Y", "N"}

		valMthdMiss, valMthdDef = strMiss, strMiss
		valMthdLvl              = []string{"A", "P", "R", "W", "O"}

		sConformMiss = strMiss
		sConformLvl  = []string{"Y", "N"}

		bapMiss, bapDef = strMiss, "7"
		bapLvl          = []string{"F", "R", "T", "O", "N", "7", "9"}

		hltvMiss = strMiss
		hltvLvl  = []string{"Y", "N"}

		reprchMwMiss, reprchMwDef = strMiss, "N"
		reprchMwLvl               = []string{"Y", "N"}

		altResMiss, altResDef = strMiss, "7"
		altResLvl             = []string{"P", "C", "D", "7", "9"}

		altResCntMin, altResCntMax, altResCntMiss, altResCntDef = int32(0), int32(100), int32(-1), int32(0)
		totDefrlMin, totDefrlMax, totDefrlMiss, totDefrlDef     = float32(0.0), float32(200000.0), float32(-1.0), float32(0.0)
	)

	// field prepends a 0 for values under 10
	for dq := 0; dq <= 99; dq++ {
		if dq < 10 {
			dqStatLvl = append(dqStatLvl, fmt.Sprintf("0%d", dq))

		} else {
			dqStatLvl = append(dqStatLvl, fmt.Sprintf("%d", dq))
		}
	}

	fds := make(map[int]*chutils.FieldDef)

	fd := &chutils.FieldDef{
		Name:        "unused0",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[0] = fd

	fd = &chutils.FieldDef{
		Name:        "lnId",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "Loan ID, missing=" + lnIdMiss,
		Legal:       &chutils.LegalValues{},
		Missing:     lnIdMiss,
	}
	fds[1] = fd

	fd = &chutils.FieldDef{
		Name:        "month",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Length: 0, Format: "012006"},
		Description: "month of data, missing=" + monthMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: monthMin, HighLimit: monthMax},
		Missing:     monthMiss,
	}
	fds[2] = fd

	// Freddie also as level T
	fd = &chutils.FieldDef{
		Name:        "channel",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "acquisition channel: B, R, C, missing=" + channelMiss,
		Legal:       &chutils.LegalValues{Levels: channelLvl},
		Missing:     channelMiss,
	}
	fds[3] = fd

	fd = &chutils.FieldDef{
		Name:        "seller",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "name of seller, missing=" + sellerMiss,
		Legal:       &chutils.LegalValues{},
		Missing:     sellerMiss,
		Default:     sellerMiss,
	}
	fds[4] = fd

	fd = &chutils.FieldDef{
		Name:        "servicer",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "name of servicer, missing=" + servicerMiss,
		Legal:       &chutils.LegalValues{},
		Missing:     servicerMiss,
		Default:     servicerMiss,
	}
	fds[5] = fd

	// not in Freddie
	fd = &chutils.FieldDef{
		Name:        "mServicer",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "name of master servicer, missing=" + mServicerMiss,
		Legal:       &chutils.LegalValues{},
		Missing:     mServicerMiss,
		Default:     mServicerMiss,
	}
	fds[6] = fd

	fd = &chutils.FieldDef{
		Name:        "rate",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "note rate at origination, 0-15, missing=" + fmt.Sprintf("%v", rateMiss),
		Legal:       &chutils.LegalValues{LowLimit: rateMin, HighLimit: rateMax},
		Missing:     rateMiss,
	}
	fds[7] = fd

	fd = &chutils.FieldDef{
		Name:        "curRate",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "current note rate, missing=" + fmt.Sprintf("%v", curRateMiss),
		Legal:       &chutils.LegalValues{LowLimit: curRateMin, HighLimit: curRateMax},
		Missing:     curRateMiss,
	}
	fds[8] = fd

	fd = &chutils.FieldDef{
		Name:        "opb",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "balance at origination, missing=" + fmt.Sprintf("%v", opbMiss),
		Legal:       &chutils.LegalValues{LowLimit: opbMin, HighLimit: opbMax},
		Missing:     opbMiss,
	}
	fds[9] = fd

	fd = &chutils.FieldDef{
		Name:        "unused35",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[10] = fd

	fd = &chutils.FieldDef{
		Name:        "upb",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "unpaid balance, missing=" + fmt.Sprintf("%v", upbMiss),
		Legal:       &chutils.LegalValues{LowLimit: upbMin, HighLimit: upbMax},
		Missing:     upbMiss,
	}
	fds[11] = fd

	fd = &chutils.FieldDef{
		Name:        "term",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "loan term at origination, missing=" + fmt.Sprintf("%v", termMiss),
		Legal:       &chutils.LegalValues{LowLimit: termMin, HighLimit: termMax},
		Missing:     termMiss,
	}
	fds[12] = fd

	fd = &chutils.FieldDef{
		Name:        "origDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "012006"},
		Description: "first payment date, missing=" + origDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: origDtMin, HighLimit: origDtMax},
		Missing:     origDtMiss,
	}
	fds[13] = fd

	fd = &chutils.FieldDef{
		Name:        "fpDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "012006"},
		Description: "first payment date, missing=" + fpDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: fpDtMin, HighLimit: fpDtMax},
		Missing:     fpDtMiss,
	}
	fds[14] = fd

	fd = &chutils.FieldDef{
		Name:        "age",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "loan age based on origination date, missing=" + fmt.Sprintf("%v", ageMiss),
		Legal:       &chutils.LegalValues{LowLimit: ageMin, HighLimit: ageMax},
		Missing:     ageMiss,
	}
	fds[15] = fd

	fd = &chutils.FieldDef{
		Name:        "rTermLgl",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "remaining legal term, missing=" + fmt.Sprintf("%v", rTermLglMiss),
		Legal:       &chutils.LegalValues{LowLimit: rTermLglMin, HighLimit: rTermLglMax},
		Missing:     rTermLglMiss,
	}
	fds[16] = fd

	// Not in Freddie
	fd = &chutils.FieldDef{
		Name:        "rTermAct",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "remaining actual term, missing=" + fmt.Sprintf("%v", rTermActMiss),
		Legal:       &chutils.LegalValues{LowLimit: rTermActMin, HighLimit: rTermActMax},
		Missing:     rTermActMiss,
	}
	fds[17] = fd

	fd = &chutils.FieldDef{
		Name:        "matDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "012006"},
		Description: "loan maturity date (initial), missing=" + matDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: matDtMin, HighLimit: matDtMax},
		Missing:     matDtMiss,
	}
	fds[18] = fd

	fd = &chutils.FieldDef{
		Name:        "ltv",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "ltv at origination, 1-998, missing=" + fmt.Sprintf("%v", ltvMiss),
		Legal:       &chutils.LegalValues{LowLimit: ltvMin, HighLimit: ltvMax},
		Missing:     ltvMiss,
	}
	fds[19] = fd

	fd = &chutils.FieldDef{
		Name:        "cltv",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "combined cltv at origination, 1-998, missing=" + fmt.Sprintf("%v", cltvMiss),
		Legal:       &chutils.LegalValues{LowLimit: cltvMin, HighLimit: cltvMax},
		Missing:     cltvMiss,
		Default:     cltvDef,
	}
	fds[20] = fd

	fd = &chutils.FieldDef{
		Name:        "numBorr",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "number of borrowers, 1-10, missing=" + fmt.Sprintf("%v", numBorrMiss),
		Legal:       &chutils.LegalValues{LowLimit: numBorrMin, HighLimit: numBorrMax},
		Missing:     numBorrMiss,
	}
	fds[21] = fd

	fd = &chutils.FieldDef{
		Name:        "dti",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "dti at origination, 1-65, missing=" + fmt.Sprintf("%v", dtiMiss),
		Legal:       &chutils.LegalValues{LowLimit: dtiMin, HighLimit: dtiMax},
		Missing:     dtiMiss,
	}
	fds[22] = fd

	fd = &chutils.FieldDef{
		Name:        "fico",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "fico at origination, 301-850, missing=-1 ",
		Legal:       &chutils.LegalValues{LowLimit: ficoMin, HighLimit: ficoMax},
		Missing:     ficoMiss,
	}
	fds[23] = fd

	// not in Freddie
	fd = &chutils.FieldDef{
		Name:        "coFico",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "coborrower fico at origination, 301-850, missing=-1 ",
		Legal:       &chutils.LegalValues{LowLimit: coFicoMin, HighLimit: coFicoMax},
		Missing:     coFicoMiss,
		Default:     coFicoDef,
	}
	fds[24] = fd

	fd = &chutils.FieldDef{
		Name:        "firstTime",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "first time homebuyer: Y, N, missing=" + firstTimeMiss,
		Legal:       &chutils.LegalValues{Levels: firstTimeLvl},
		Missing:     firstTimeMiss,
	}
	fds[25] = fd

	// Freddie N = Fannie U
	fd = &chutils.FieldDef{
		Name:        "purpose",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "loan purpose: P (purch), C (cash out refi), U (rate/term refi) R (refi), missing=" + purposeMiss,
		Legal:       &chutils.LegalValues{Levels: purposeLvl},
		Missing:     purposeMiss,
	}
	fds[26] = fd

	fd = &chutils.FieldDef{
		Name:        "propType",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 2},
		Description: "property type: SF (single family), CO (condo), PU (PUD), CP (coop), MH (manufactured), missing=" + propTypeMiss,
		Legal:       &chutils.LegalValues{Levels: propTypeLvl},
		Missing:     propTypeMiss,
	}
	fds[27] = fd

	fd = &chutils.FieldDef{
		Name:        "units",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "# of units in the property, 1-4, missing=" + fmt.Sprintf("%v", unitMiss),
		Legal:       &chutils.LegalValues{LowLimit: unitMin, HighLimit: unitMax},
		Missing:     unitMiss,
	}
	fds[28] = fd

	// moving "U" to occMiss
	fd = &chutils.FieldDef{
		Name:        "occ",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "property occupancy: P (primary), S (secondary), I (investor) missing=" + occMiss,
		Legal:       &chutils.LegalValues{Levels: occLvl},
		Missing:     occMiss,
	}
	fds[29] = fd

	fd = &chutils.FieldDef{
		Name:        "state",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 2},
		Description: "property state postal abbreviation, missing=" + stateMiss,
		Legal:       &chutils.LegalValues{Levels: stateLvl},
		Missing:     stateMiss,
	}
	fds[30] = fd

	fd = &chutils.FieldDef{
		Name:        "msa",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 5},
		Description: "msa/division code, missing/not in MSA=" + msaMiss,
		Legal:       &chutils.LegalValues{Levels: msaLvl},
		Default:     msaDef,
		Missing:     msaMiss,
	}
	fds[31] = fd

	// Freddie has 5 digits but the last two digits are 0
	fd = &chutils.FieldDef{
		Name:        "zip3",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 3},
		Description: "3-digit zip , missing=" + zip3Miss,
		Legal:       &chutils.LegalValues{Levels: zip3Lvl},
		Missing:     zip3Miss,
	}
	fds[32] = fd

	fd = &chutils.FieldDef{
		Name:        "mi",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "mi percentage, 0-55, missing=" + fmt.Sprintf("%v", miMiss),
		Legal:       &chutils.LegalValues{LowLimit: miMin, HighLimit: miMax},
		Missing:     miMiss,
		Default:     miDef,
	}
	fds[33] = fd

	fd = &chutils.FieldDef{
		Name:        "amType",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 3},
		Description: "amortization type: FRM, ARM, missing=" + amTypeMiss,
		Legal:       &chutils.LegalValues{Levels: amTypeLvl},
		Missing:     amTypeMiss,
	}
	fds[34] = fd

	fd = &chutils.FieldDef{
		Name:        "pPen",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "prepay penalty flag: Y, N, missing=" + ppenMiss,
		Legal:       &chutils.LegalValues{Levels: pPenLvl},
		Missing:     ppenMiss,
	}
	fds[35] = fd

	fd = &chutils.FieldDef{
		Name:        "io",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "io Flag: Y, N, missing=" + ioMiss,
		Legal:       &chutils.LegalValues{Levels: ioLvl},
		Missing:     ioMiss,
		Default:     ioDef,
	}
	fds[36] = fd

	fd = &chutils.FieldDef{
		Name:        "ioDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Length: 0, Format: "012006"},
		Description: "month IO loan starts amortizing, missing=" + ioDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: ioDtMin, HighLimit: ioDtMax},
		Missing:     ioDtMiss,
		Default:     ioDtDef,
	}
	fds[37] = fd

	fd = &chutils.FieldDef{
		Name:        "ioRem",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "number of IO months remaining, missing=" + fmt.Sprintf("%v", ioRemMiss),
		Legal:       &chutils.LegalValues{LowLimit: ioRemMin, HighLimit: ioRemMax},
		Missing:     ioRemMiss,
		Default:     ioRemDef,
	}
	fds[38] = fd

	// Freddie has "RA" code value
	fd = &chutils.FieldDef{
		Name:        "dqStat",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 3},
		Description: "DQ status code: 0-99 months, missing=" + dqStatMiss,
		Legal:       &chutils.LegalValues{Levels: dqStatLvl},
		Missing:     dqStatMiss,
	}
	fds[39] = fd

	// Not in Freddie
	fd = &chutils.FieldDef{
		Name:        "payHist",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Length: 3},
		Description: "24 month pay history (oldest on left), missing=" + payHistMiss,
		Legal:       &chutils.LegalValues{},
		Missing:     payHistMiss,
	}
	fds[40] = fd

	// Freddie has code P (prior), however the Y here is sticky
	fd = &chutils.FieldDef{
		Name:        "mod",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "modification flag: Y, N , missing=" + modMiss,
		Legal:       &chutils.LegalValues{Levels: modLvl},
		Missing:     modMiss,
	}
	fds[41] = fd

	// Not in Freddie
	fd = &chutils.FieldDef{
		Name:        "unused36",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 2},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
	}
	fds[42] = fd

	fd = &chutils.FieldDef{
		Name:        "zb",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 2},
		Description: "zero balance:00(noop), 01(pp), 03(short), 06(repurch), 09(REO), 96/97/98(removal), 02/15/16(sale), missing=" + zbMiss,
		Legal:       &chutils.LegalValues{Levels: zbLvl},
		Missing:     zbMiss,
		Default:     zbDef,
	}
	fds[43] = fd

	fd = &chutils.FieldDef{
		Name:        "zbDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "012006"},
		Description: "zero balance date, missing=" + zbDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: zbDtMin, HighLimit: zbDtMax},
		Missing:     zbDtMiss,
		Default:     zbDtDef,
	}
	fds[44] = fd

	fd = &chutils.FieldDef{
		Name:        "zbUpb",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "UPB just prior to zero balance, missing=" + fmt.Sprintf("%v", zbUpbMiss),
		Legal:       &chutils.LegalValues{LowLimit: zbUpbMin, HighLimit: zbUpbMax},
		Missing:     zbUpbMiss,
		Default:     zbUpbDef,
	}
	fds[45] = fd

	fd = &chutils.FieldDef{
		Name:        "unused1",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[46] = fd

	fd = &chutils.FieldDef{
		Name:        "unused2",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[47] = fd

	fd = &chutils.FieldDef{
		Name:        "totPrin",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "delta upb",
		Legal:       &chutils.LegalValues{},
		Missing:     fltMiss,
	}
	fds[48] = fd

	fd = &chutils.FieldDef{
		Name:        "unused3",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[49] = fd

	fd = &chutils.FieldDef{
		Name:        "lpDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "012006"},
		Description: "last pay date, missing=" + lpDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: lpDtMin, HighLimit: lpDtMax},
		Missing:     lpDtMiss,
		Default:     lpDtDef,
	}
	fds[50] = fd

	fd = &chutils.FieldDef{
		Name:        "fclDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "012006"},
		Description: "foreclosure date, missing=" + fclDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: fclDtMin, HighLimit: fclDtMax},
		Missing:     fclDtMiss,
		Default:     fclDtDef,
	}
	fds[51] = fd

	// not in freddie
	fd = &chutils.FieldDef{
		Name:        "dispDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "012006"},
		Description: "date Fannie is done with loan, missing=" + dispDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: dispDtMin, HighLimit: dispDtMax},
		Missing:     dispDtMiss,
		Default:     dispDtDef,
	}
	fds[52] = fd

	fd = &chutils.FieldDef{
		Name:        "fclExp",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "total foreclosure expenses",
		Legal:       &chutils.LegalValues{LowLimit: fclExpMin, HighLimit: fclExpMax},
		Missing:     fclExpMiss,
		Default:     fclExpDef,
	}
	fds[53] = fd

	fd = &chutils.FieldDef{
		Name:        "fclPExp",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure property preservation expenses",
		Legal:       &chutils.LegalValues{LowLimit: fclPExpMin, HighLimit: fclPExpMax},
		Missing:     fclPExpMiss,
		Default:     fclPExpDef,
	}
	fds[54] = fd

	fd = &chutils.FieldDef{
		Name:        "fclLExp",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure recovery legal expenses",
		Legal:       &chutils.LegalValues{LowLimit: fclLExpMin, HighLimit: fclLExpMax},
		Missing:     fclLExpMiss,
		Default:     fclLExpDef,
	}
	fds[55] = fd

	fd = &chutils.FieldDef{
		Name:        "fclMExp",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure misc expenses, missing=" + fmt.Sprintf("%v", fclMExpMiss),
		Legal:       &chutils.LegalValues{LowLimit: fclMExpMin, HighLimit: fclMExpMax},
		Missing:     fltMiss,
		Default:     fclMExpDef,
	}
	fds[56] = fd

	fd = &chutils.FieldDef{
		Name:        "fclTaxes",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure property taxes and insurance",
		Legal:       &chutils.LegalValues{LowLimit: fclTaxesMin, HighLimit: fclTaxesMax},
		Missing:     fclTaxesMiss,
		Default:     fclTaxesDef,
	}
	fds[57] = fd

	fd = &chutils.FieldDef{
		Name:        "fclProNet",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure net proceeds",
		Legal:       &chutils.LegalValues{LowLimit: fclProNetMin, HighLimit: fclProNetMax},
		Missing:     fclProNetMiss,
		Default:     fclProNetDef,
	}
	fds[58] = fd

	fd = &chutils.FieldDef{
		Name:        "fclProMi",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure credit enhancement proceeds",
		Legal:       &chutils.LegalValues{LowLimit: fclProMiMin, HighLimit: fclProMiMax},
		Missing:     fclProMiMiss,
		Default:     fclProMiDef,
	}
	fds[59] = fd

	fd = &chutils.FieldDef{
		Name:        "fclProMw",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure make whole proceeds",
		Legal:       &chutils.LegalValues{LowLimit: fclProMwMin, HighLimit: fclProMwMax},
		Missing:     fclProMwMiss,
		Default:     fclProMwDef,
	}
	fds[60] = fd

	// not in Freddie
	fd = &chutils.FieldDef{
		Name:        "fclProOth",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure other proceeds",
		Legal:       &chutils.LegalValues{LowLimit: fclProOthMin, HighLimit: fclProOthMax},
		Missing:     fclProOthMiss,
		Default:     fclProOthDef,
	}
	fds[61] = fd

	// Freddie has IntUpb
	fd = &chutils.FieldDef{
		Name:        "nonIntUpb",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "interest bearing UPB, missing=" + fmt.Sprintf("%v", nonIntUpbMiss),
		Legal:       &chutils.LegalValues{LowLimit: nonIntUpbMin, HighLimit: nonIntUpbMax},
		Missing:     nonIntUpbMiss,
		Default:     nonIntUpbDef,
	}
	fds[62] = fd

	// Freddie has IntUpb
	fd = &chutils.FieldDef{
		Name:        "frgvUpb",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "forgiven UPB, missing=" + fmt.Sprintf("%v", frgvUpbMiss),
		Legal:       &chutils.LegalValues{LowLimit: frgvUpbMin, HighLimit: frgvUpbMax},
		Missing:     frgvUpbMiss,
		Default:     frgvUpbDef,
	}
	fds[63] = fd

	fd = &chutils.FieldDef{
		Name:        "unused4",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[64] = fd

	fd = &chutils.FieldDef{
		Name:        "unused5",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[65] = fd

	fd = &chutils.FieldDef{
		Name:        "unused6",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[66] = fd

	fd = &chutils.FieldDef{
		Name:        "unused7",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[67] = fd

	fd = &chutils.FieldDef{
		Name:        "unused8",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[68] = fd

	fd = &chutils.FieldDef{
		Name:        "unused9",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[69] = fd

	fd = &chutils.FieldDef{
		Name:        "unused10",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[70] = fd

	fd = &chutils.FieldDef{
		Name:        "unused11",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[71] = fd

	fd = &chutils.FieldDef{
		Name:        "miType",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "mi type: 1=borrower, 2=lender, 3=enterprise, 0=No MI, missing=" + miTypeMiss,
		Legal:       &chutils.LegalValues{Levels: miTypeLvl},
		Missing:     miTypeMiss,
		Default:     miTypeDef,
	}
	fds[72] = fd

	fd = &chutils.FieldDef{
		Name:        "servAct",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "servicing activity: Y, N, missing=" + servActMiss,
		Legal:       &chutils.LegalValues{Levels: servActLvl},
		Missing:     servActMiss,
	}
	fds[73] = fd

	fd = &chutils.FieldDef{
		Name:        "unused12",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[74] = fd

	fd = &chutils.FieldDef{
		Name:        "unused13",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[75] = fd

	fd = &chutils.FieldDef{
		Name:        "unused14",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[76] = fd

	fd = &chutils.FieldDef{
		Name:        "unused15",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[77] = fd

	// compare to Freddie program
	fd = &chutils.FieldDef{
		Name:        "program",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "fannie program: Y (home ready) N (no program), missing=" + programMiss,
		Legal:       &chutils.LegalValues{Levels: programLvl},
		Missing:     programMiss,
		Default:     programDef,
	}
	fds[78] = fd

	// not in Freddie
	fd = &chutils.FieldDef{
		Name:        "fclWriteOff",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure principal writeoff, missing=" + fmt.Sprintf("%v", fclWriteOffMiss),
		Legal:       &chutils.LegalValues{LowLimit: fclWriteOffMin, HighLimit: fclWriteOffMax},
		Missing:     fclWriteOffMiss,
		Default:     fclWriteOffDef,
	}
	fds[79] = fd

	fd = &chutils.FieldDef{
		Name:        "relo",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "relocation mortgage: Y, N, missing=" + reloMiss,
		Legal:       &chutils.LegalValues{Levels: reloLvl},
		Missing:     reloMiss,
	}
	fds[80] = fd

	fd = &chutils.FieldDef{
		Name:        "unused16",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[81] = fd

	fd = &chutils.FieldDef{
		Name:        "unused17",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[82] = fd

	fd = &chutils.FieldDef{
		Name:        "unused18",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[83] = fd

	fd = &chutils.FieldDef{
		Name:        "unused19",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[84] = fd

	// codes different than Freddie
	fd = &chutils.FieldDef{
		Name:        "valMthd",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "property value method A(apprsl), P(onsite), R(GSE target), W(waived), O(other), missing=" + valMthdMiss,
		Legal:       &chutils.LegalValues{Levels: valMthdLvl},
		Missing:     valMthdMiss,
		Default:     valMthdDef,
	}
	fds[85] = fd

	fd = &chutils.FieldDef{
		Name:        "sConform",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "super conforming flag: Y, N, missing=" + sConformMiss,
		Legal:       &chutils.LegalValues{Levels: sConformLvl},
		Missing:     sConformMiss,
	}
	fds[86] = fd

	fd = &chutils.FieldDef{
		Name:        "unused20",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[87] = fd

	fd = &chutils.FieldDef{
		Name:        "unused21",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[88] = fd

	fd = &chutils.FieldDef{
		Name:        "unused22",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[89] = fd

	fd = &chutils.FieldDef{
		Name:        "unused23",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[90] = fd

	fd = &chutils.FieldDef{
		Name:        "unused24",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[91] = fd

	fd = &chutils.FieldDef{
		Name:        "unused25",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[92] = fd

	fd = &chutils.FieldDef{
		Name:        "unused26",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[93] = fd

	fd = &chutils.FieldDef{
		Name:        "unused27",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[94] = fd

	fd = &chutils.FieldDef{
		Name:        "unused28",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[95] = fd

	fd = &chutils.FieldDef{
		Name:        "unused29",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[96] = fd

	fd = &chutils.FieldDef{
		Name:        "unused30",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[97] = fd

	fd = &chutils.FieldDef{
		Name:        "unused31",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[98] = fd

	fd = &chutils.FieldDef{
		Name:        "unused32",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[99] = fd

	fd = &chutils.FieldDef{
		Name:        "unused33",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[100] = fd

	fd = &chutils.FieldDef{
		Name:        "bap",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "borrower assistant plan: F(forebearance), R(repayment), T(trial), O(Other), N(none), 7,9(NA) missing=" + bapMiss,
		Legal:       &chutils.LegalValues{Levels: bapLvl},
		Missing:     bapMiss,
		Default:     bapDef,
	}
	fds[101] = fd

	fd = &chutils.FieldDef{
		Name:        "hltv",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "high LTV refi missing=" + hltvMiss,
		Legal:       &chutils.LegalValues{Levels: hltvLvl},
		Missing:     hltvMiss,
	}
	fds[102] = fd

	fd = &chutils.FieldDef{
		Name:        "unused34",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "unused",
		Legal:       &chutils.LegalValues{},
		Missing:     strMiss,
	}
	fds[103] = fd

	fd = &chutils.FieldDef{
		Name:        "reprchMw",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "repurchase make whole: Y, N missing=" + reprchMwMiss,
		Legal:       &chutils.LegalValues{Levels: reprchMwLvl},
		Missing:     reprchMwMiss,
		Default:     reprchMwDef,
	}
	fds[104] = fd

	// Freddie has a dq due to disaster flag (dqDis) and payment plan flag (payPl)
	fd = &chutils.FieldDef{
		Name:        "altRes",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "DQ payment deferral: P(payment), C(Covid), D(disaster), 7/9 :NA missing=" + altResMiss,
		Legal:       &chutils.LegalValues{Levels: altResLvl},
		Missing:     altResMiss,
		Default:     altResDef,
	}
	fds[105] = fd

	// not in Freddie
	fd = &chutils.FieldDef{
		Name:        "altResCnt",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "# of alternate resolutions (deferrals), missing=" + fmt.Sprintf("%v", altResCntMiss),
		Legal:       &chutils.LegalValues{LowLimit: altResCntMin, HighLimit: altResCntMax},
		Missing:     altResCntMiss,
		Default:     altResCntDef,
	}
	fds[106] = fd

	// compare to Freddie defrl
	fd = &chutils.FieldDef{
		Name:        "totDefrl",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "total amount deferred, missing=" + fmt.Sprintf("%v", totDefrlMiss),
		Legal:       &chutils.LegalValues{LowLimit: totDefrlMin, HighLimit: totDefrlMax},
		Missing:     totDefrlMiss,
		Default:     totDefrlDef,
	}
	fds[107] = fd
	// ============================
	// not in fannie:
	//   preHARPlnId
	//   harp
	//   defectDt
	//   defrl
	//   fclLoss
	//   modTLoss
	//   stepMod
	//   payPl
	//   accrInt
	//   eLtv
	//   dqDis
	//   modCLoss

	return chutils.NewTableDef("lnId, month", chutils.MergeTree, fds)
}
