//line query.y:2
package querylang

import __yyfmt__ "fmt"

//line query.y:3
import (
	"bufio"
	"fmt"
	"github.com/gtfierro/durandal/common"
	"github.com/taylorchu/toki"
	"strconv"
	"strings"
	_time "time"
)

/**
Notes here
**/

//line query.y:21
type sqSymType struct {
	yys      int
	str      string
	dict     common.Dict
	data     *DataQuery
	limit    Limit
	timeconv common.UnitOfTime
	list     List
	time     _time.Time
	timediff _time.Duration
}

const SELECT = 57346
const DISTINCT = 57347
const DELETE = 57348
const APPLY = 57349
const STATISTICAL = 57350
const WINDOW = 57351
const STATISTICS = 57352
const CHANGED = 57353
const WHERE = 57354
const DATA = 57355
const BEFORE = 57356
const AFTER = 57357
const LIMIT = 57358
const STREAMLIMIT = 57359
const NOW = 57360
const LVALUE = 57361
const QSTRING = 57362
const EQ = 57363
const NEQ = 57364
const COMMA = 57365
const ALL = 57366
const LEFTPIPE = 57367
const LIKE = 57368
const AS = 57369
const AND = 57370
const OR = 57371
const HAS = 57372
const NOT = 57373
const IN = 57374
const TO = 57375
const LPAREN = 57376
const RPAREN = 57377
const LBRACK = 57378
const RBRACK = 57379
const NUMBER = 57380
const SEMICOLON = 57381
const NEWLINE = 57382
const TIMEUNIT = 57383

var sqToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"SELECT",
	"DISTINCT",
	"DELETE",
	"APPLY",
	"STATISTICAL",
	"WINDOW",
	"STATISTICS",
	"CHANGED",
	"WHERE",
	"DATA",
	"BEFORE",
	"AFTER",
	"LIMIT",
	"STREAMLIMIT",
	"NOW",
	"LVALUE",
	"QSTRING",
	"EQ",
	"NEQ",
	"COMMA",
	"ALL",
	"LEFTPIPE",
	"LIKE",
	"AS",
	"AND",
	"OR",
	"HAS",
	"NOT",
	"IN",
	"TO",
	"LPAREN",
	"RPAREN",
	"LBRACK",
	"RBRACK",
	"NUMBER",
	"SEMICOLON",
	"NEWLINE",
	"TIMEUNIT",
}
var sqStatenames = [...]string{}

const sqEofCode = 1
const sqErrCode = 2
const sqInitialStackSize = 16

//line query.y:386

const eof = 0

var supported_formats = []string{"1/2/2006",
	"1-2-2006",
	"1/2/2006 03:04:05 PM MST",
	"1-2-2006 03:04:05 PM MST",
	"1/2/2006 15:04:05 MST",
	"1-2-2006 15:04:05 MST",
	"2006-1-2 15:04:05 MST"}

type List []string

func (qt QueryType) String() string {
	ret := ""
	switch qt {
	case SELECT_TYPE:
		ret = "select"
	case DELETE_TYPE:
		ret = "delete"
	case DATA_TYPE:
		ret = "data"
	}
	return ret
}

type query struct {
	// the type of query we are doing
	qtype QueryType
	// information about a data query if we are one
	data *DataQuery
	// where clause for query
	where common.Dict
	// are we querying distinct values?
	distinct bool
	// list of tags to target for deletion, selection
	Contents []string
}

func (q *query) Print() {
	fmt.Printf("Type: %v\n", q.qtype.String())
	if q.qtype == DATA_TYPE {
		fmt.Printf("Data Query Type: %v\n", q.data.Dtype.String())
		fmt.Printf("Start: %v\n", q.data.Start)
		fmt.Printf("End: %v\n", q.data.End)
		fmt.Printf("Limit: %v\n", q.data.Limit.Limit)
		fmt.Printf("Streamlimit: %v\n", q.data.Limit.Streamlimit)
	}
	fmt.Printf("Contents: %v\n", q.Contents)
	fmt.Printf("Distinct? %v\n", q.distinct)
	fmt.Printf("where: %v\n", q.where)
}

type sqLex struct {
	querystring string
	query       *query
	scanner     *toki.Scanner
	lasttoken   string
	tokens      []string
	error       error
	// all keys that we encounter. Used for republish concerns
	_keys map[string]struct{}
	keys  []string
}

func NewSQLex(s string) *sqLex {
	scanner := toki.NewScanner(
		[]toki.Def{
			{Token: WHERE, Pattern: "where"},
			{Token: SELECT, Pattern: "select"},
			{Token: APPLY, Pattern: "apply"},
			{Token: DELETE, Pattern: "delete"},
			{Token: DISTINCT, Pattern: "distinct"},
			{Token: STATISTICAL, Pattern: "statistical"},
			{Token: STATISTICS, Pattern: "statistics"},
			{Token: WINDOW, Pattern: "window"},
			{Token: CHANGED, Pattern: "changed"},
			{Token: LIMIT, Pattern: "limit"},
			{Token: STREAMLIMIT, Pattern: "streamlimit"},
			{Token: ALL, Pattern: "\\*"},
			{Token: NOW, Pattern: "now"},
			{Token: BEFORE, Pattern: "before"},
			{Token: AFTER, Pattern: "after"},
			{Token: COMMA, Pattern: ","},
			{Token: AND, Pattern: "and"},
			{Token: AS, Pattern: "as"},
			{Token: TO, Pattern: "to"},
			{Token: DATA, Pattern: "data"},
			{Token: OR, Pattern: "or"},
			{Token: IN, Pattern: "in"},
			{Token: HAS, Pattern: "has"},
			{Token: NOT, Pattern: "not"},
			{Token: NEQ, Pattern: "!="},
			{Token: EQ, Pattern: "="},
			{Token: LEFTPIPE, Pattern: "<"},
			{Token: LPAREN, Pattern: "\\("},
			{Token: RPAREN, Pattern: "\\)"},
			{Token: LBRACK, Pattern: "\\["},
			{Token: RBRACK, Pattern: "\\]"},
			{Token: SEMICOLON, Pattern: ";"},
			{Token: NEWLINE, Pattern: "\n"},
			{Token: LIKE, Pattern: "(like)|~"},
			{Token: NUMBER, Pattern: "([+-]?([0-9]*\\.)?[0-9]+)"},
			{Token: LVALUE, Pattern: "[a-zA-Z\\~\\$\\_][a-zA-Z0-9\\/\\%_\\-]*"},
			{Token: QSTRING, Pattern: "(\"[^\"\\\\]*?(\\.[^\"\\\\]*?)*?\")|('[^'\\\\]*?(\\.[^'\\\\]*?)*?')"},
		})
	scanner.SetInput(s)
	q := &query{Contents: []string{}, distinct: false}
	return &sqLex{query: q, querystring: s, scanner: scanner, error: nil, lasttoken: "", _keys: map[string]struct{}{}, tokens: []string{}}
}

func (sq *sqLex) Lex(lval *sqSymType) int {
	r := sq.scanner.Next()
	sq.lasttoken = r.String()
	if r.Pos.Line == 2 || len(r.Value) == 0 {
		return eof
	}
	lval.str = string(r.Value)
	sq.tokens = append(sq.tokens, lval.str)
	return int(r.Token)
}

func (sq *sqLex) Error(s string) {
	sq.error = fmt.Errorf(s)
}

func readline(fi *bufio.Reader) (string, bool) {
	fmt.Printf("smap> ")
	s, err := fi.ReadString('\n')
	if err != nil {
		return "", false
	}
	return s, true
}

// Parse has been moved to query_processor.go

//line yacctab:1
var sqExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
}

const sqNprod = 52
const sqPrivate = 57344

var sqTokenNames []string
var sqStates []string

const sqLast = 166

var sqAct = [...]int{

	97, 75, 36, 72, 68, 14, 44, 35, 47, 49,
	48, 22, 49, 57, 48, 42, 49, 41, 18, 32,
	123, 119, 73, 105, 15, 100, 43, 87, 99, 55,
	46, 50, 51, 14, 46, 37, 34, 15, 54, 39,
	64, 40, 60, 127, 92, 20, 53, 67, 37, 69,
	70, 52, 39, 78, 40, 142, 138, 81, 137, 128,
	117, 104, 91, 80, 79, 121, 83, 84, 89, 120,
	85, 86, 88, 29, 28, 27, 96, 26, 95, 101,
	122, 24, 25, 66, 65, 114, 113, 90, 98, 6,
	58, 59, 136, 106, 131, 130, 116, 109, 107, 23,
	110, 108, 69, 8, 94, 93, 10, 12, 11, 13,
	118, 9, 82, 62, 63, 49, 71, 15, 61, 124,
	56, 111, 7, 30, 15, 129, 74, 125, 126, 112,
	17, 76, 77, 133, 132, 19, 21, 134, 135, 140,
	141, 143, 144, 139, 145, 115, 146, 31, 10, 12,
	11, 13, 18, 9, 103, 102, 18, 2, 5, 3,
	1, 45, 16, 38, 4, 33,
}
var sqPact = [...]int{

	153, -1000, 98, 140, 6, 144, -1000, -1000, 105, 67,
	43, 41, 40, 39, 100, -1000, 144, -20, 5, -22,
	-1000, -24, -1000, -8, -4, -4, 13, 8, 0, -9,
	105, -26, -1000, 62, 18, -1000, 92, 105, 52, 18,
	95, -1000, -1000, -4, 93, -16, 107, -1000, -1000, -1000,
	115, 115, 29, 28, 105, 89, -1000, -1000, 18, 18,
	-1000, 95, -11, 95, -1000, 105, 55, 27, 7, 82,
	81, -4, -1000, 105, -1000, 61, -10, -13, 61, 142,
	141, 26, -15, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	105, -1000, -1000, 95, -4, 115, -16, -1000, 102, 112,
	-1000, -1000, 54, 53, 132, 73, -1000, -1000, 25, 61,
	-1000, -1000, -17, 35, 31, 48, -18, 115, -1000, -1000,
	-4, -4, 9, 24, 61, 72, 71, -4, 120, -1000,
	-4, -4, 69, -1000, 23, 21, -4, 115, 115, 20,
	61, 61, 115, -1000, -1000, 61, -1000,
}
var sqPgo = [...]int{

	0, 165, 7, 130, 164, 89, 4, 163, 158, 6,
	161, 3, 1, 0, 8, 2, 160,
}
var sqR1 = [...]int{

	0, 16, 16, 16, 16, 16, 5, 5, 7, 6,
	6, 4, 4, 4, 4, 8, 8, 8, 8, 8,
	8, 8, 8, 9, 9, 10, 10, 10, 10, 11,
	11, 12, 12, 12, 12, 13, 13, 3, 2, 2,
	2, 2, 2, 2, 2, 2, 14, 15, 1, 1,
	1, 1,
}
var sqR2 = [...]int{

	0, 4, 3, 4, 4, 3, 1, 3, 3, 1,
	3, 1, 1, 2, 1, 9, 7, 13, 13, 14,
	9, 5, 5, 1, 2, 2, 1, 1, 1, 2,
	3, 0, 2, 2, 4, 0, 2, 2, 3, 3,
	3, 3, 2, 3, 4, 3, 1, 1, 3, 3,
	2, 1,
}
var sqChk = [...]int{

	-1000, -16, 4, 6, -4, -8, -5, 24, 5, 13,
	8, 10, 9, 11, -15, 19, -8, -3, 12, -3,
	39, -3, -15, 32, 14, 15, 34, 34, 34, 34,
	23, -3, 39, -1, 31, -2, -15, 30, -7, 34,
	36, 39, 39, 34, -9, -10, 38, -14, 18, 20,
	-9, -9, 38, 38, 38, 38, -5, 39, 28, 29,
	-2, 26, 21, 22, -15, 32, 31, -2, -6, -14,
	-9, 23, -11, 38, 19, -12, 16, 17, -12, 35,
	35, -15, 23, -2, -2, -14, -14, 38, -14, -15,
	32, 35, 37, 23, 23, -9, -15, -13, 27, 38,
	38, -13, 13, 13, 35, 38, -15, -6, -9, -12,
	-11, 19, 17, 32, 32, 13, 23, 35, -13, 38,
	34, 34, 32, 38, -12, -9, -9, 34, 35, -13,
	23, 23, -9, 13, -9, -9, 23, 35, 35, -9,
	-12, -12, 35, -13, -13, -12, -13,
}
var sqDef = [...]int{

	0, -2, 0, 0, 0, 0, 11, 12, 14, 0,
	0, 0, 0, 0, 6, 47, 0, 0, 0, 0,
	2, 0, 13, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 5, 37, 0, 51, 0, 0, 0, 0,
	0, 1, 3, 0, 0, 23, 26, 27, 28, 46,
	31, 31, 0, 0, 0, 0, 7, 4, 0, 0,
	50, 0, 0, 0, 42, 0, 0, 0, 0, 9,
	0, 0, 24, 0, 25, 35, 0, 0, 35, 0,
	0, 0, 0, 48, 49, 38, 39, 40, 41, 43,
	0, 45, 8, 0, 0, 31, 29, 21, 0, 32,
	33, 22, 0, 0, 0, 0, 44, 10, 0, 35,
	30, 36, 0, 0, 0, 0, 0, 31, 16, 34,
	0, 0, 0, 0, 35, 0, 0, 0, 0, 15,
	0, 0, 0, 20, 0, 0, 0, 31, 31, 0,
	35, 35, 31, 17, 18, 35, 19,
}
var sqTok1 = [...]int{

	1,
}
var sqTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
}
var sqTok3 = [...]int{
	0,
}

var sqErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

//line yaccpar:1

/*	parser for yacc output	*/

var (
	sqDebug        = 0
	sqErrorVerbose = false
)

type sqLexer interface {
	Lex(lval *sqSymType) int
	Error(s string)
}

type sqParser interface {
	Parse(sqLexer) int
	Lookahead() int
}

type sqParserImpl struct {
	lval  sqSymType
	stack [sqInitialStackSize]sqSymType
	char  int
}

func (p *sqParserImpl) Lookahead() int {
	return p.char
}

func sqNewParser() sqParser {
	return &sqParserImpl{}
}

const sqFlag = -1000

func sqTokname(c int) string {
	if c >= 1 && c-1 < len(sqToknames) {
		if sqToknames[c-1] != "" {
			return sqToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func sqStatname(s int) string {
	if s >= 0 && s < len(sqStatenames) {
		if sqStatenames[s] != "" {
			return sqStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func sqErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !sqErrorVerbose {
		return "syntax error"
	}

	for _, e := range sqErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + sqTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := sqPact[state]
	for tok := TOKSTART; tok-1 < len(sqToknames); tok++ {
		if n := base + tok; n >= 0 && n < sqLast && sqChk[sqAct[n]] == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if sqDef[state] == -2 {
		i := 0
		for sqExca[i] != -1 || sqExca[i+1] != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; sqExca[i] >= 0; i += 2 {
			tok := sqExca[i]
			if tok < TOKSTART || sqExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if sqExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += sqTokname(tok)
	}
	return res
}

func sqlex1(lex sqLexer, lval *sqSymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = sqTok1[0]
		goto out
	}
	if char < len(sqTok1) {
		token = sqTok1[char]
		goto out
	}
	if char >= sqPrivate {
		if char < sqPrivate+len(sqTok2) {
			token = sqTok2[char-sqPrivate]
			goto out
		}
	}
	for i := 0; i < len(sqTok3); i += 2 {
		token = sqTok3[i+0]
		if token == char {
			token = sqTok3[i+1]
			goto out
		}
	}

out:
	if token == 0 {
		token = sqTok2[1] /* unknown char */
	}
	if sqDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", sqTokname(token), uint(char))
	}
	return char, token
}

func sqParse(sqlex sqLexer) int {
	return sqNewParser().Parse(sqlex)
}

func (sqrcvr *sqParserImpl) Parse(sqlex sqLexer) int {
	var sqn int
	var sqVAL sqSymType
	var sqDollar []sqSymType
	_ = sqDollar // silence set and not used
	sqS := sqrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	sqstate := 0
	sqrcvr.char = -1
	sqtoken := -1 // sqrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		sqstate = -1
		sqrcvr.char = -1
		sqtoken = -1
	}()
	sqp := -1
	goto sqstack

ret0:
	return 0

ret1:
	return 1

sqstack:
	/* put a state and value onto the stack */
	if sqDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", sqTokname(sqtoken), sqStatname(sqstate))
	}

	sqp++
	if sqp >= len(sqS) {
		nyys := make([]sqSymType, len(sqS)*2)
		copy(nyys, sqS)
		sqS = nyys
	}
	sqS[sqp] = sqVAL
	sqS[sqp].yys = sqstate

sqnewstate:
	sqn = sqPact[sqstate]
	if sqn <= sqFlag {
		goto sqdefault /* simple state */
	}
	if sqrcvr.char < 0 {
		sqrcvr.char, sqtoken = sqlex1(sqlex, &sqrcvr.lval)
	}
	sqn += sqtoken
	if sqn < 0 || sqn >= sqLast {
		goto sqdefault
	}
	sqn = sqAct[sqn]
	if sqChk[sqn] == sqtoken { /* valid shift */
		sqrcvr.char = -1
		sqtoken = -1
		sqVAL = sqrcvr.lval
		sqstate = sqn
		if Errflag > 0 {
			Errflag--
		}
		goto sqstack
	}

sqdefault:
	/* default state action */
	sqn = sqDef[sqstate]
	if sqn == -2 {
		if sqrcvr.char < 0 {
			sqrcvr.char, sqtoken = sqlex1(sqlex, &sqrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if sqExca[xi+0] == -1 && sqExca[xi+1] == sqstate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			sqn = sqExca[xi+0]
			if sqn < 0 || sqn == sqtoken {
				break
			}
		}
		sqn = sqExca[xi+1]
		if sqn < 0 {
			goto ret0
		}
	}
	if sqn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			sqlex.Error(sqErrorMessage(sqstate, sqtoken))
			Nerrs++
			if sqDebug >= 1 {
				__yyfmt__.Printf("%s", sqStatname(sqstate))
				__yyfmt__.Printf(" saw %s\n", sqTokname(sqtoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for sqp >= 0 {
				sqn = sqPact[sqS[sqp].yys] + sqErrCode
				if sqn >= 0 && sqn < sqLast {
					sqstate = sqAct[sqn] /* simulate a shift of "error" */
					if sqChk[sqstate] == sqErrCode {
						goto sqstack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if sqDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", sqS[sqp].yys)
				}
				sqp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if sqDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", sqTokname(sqtoken))
			}
			if sqtoken == sqEofCode {
				goto ret1
			}
			sqrcvr.char = -1
			sqtoken = -1
			goto sqnewstate /* try again in the same state */
		}
	}

	/* reduction by production sqn */
	if sqDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", sqn, sqStatname(sqstate))
	}

	sqnt := sqn
	sqpt := sqp
	_ = sqpt // guard against "declared and not used"

	sqp -= sqR2[sqn]
	// sqp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if sqp+1 >= len(sqS) {
		nyys := make([]sqSymType, len(sqS)*2)
		copy(nyys, sqS)
		sqS = nyys
	}
	sqVAL = sqS[sqp+1]

	/* consult goto table to find next state */
	sqn = sqR1[sqn]
	sqg := sqPgo[sqn]
	sqj := sqg + sqS[sqp].yys + 1

	if sqj >= sqLast {
		sqstate = sqAct[sqg]
	} else {
		sqstate = sqAct[sqj]
		if sqChk[sqstate] != -sqn {
			sqstate = sqAct[sqg]
		}
	}
	// dummy call; replaced with literal code
	switch sqnt {

	case 1:
		sqDollar = sqS[sqpt-4 : sqpt+1]
		//line query.y:60
		{
			sqlex.(*sqLex).query.Contents = sqDollar[2].list
			sqlex.(*sqLex).query.where = sqDollar[3].dict
			sqlex.(*sqLex).query.qtype = SELECT_TYPE
		}
	case 2:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:66
		{
			sqlex.(*sqLex).query.Contents = sqDollar[2].list
			sqlex.(*sqLex).query.qtype = SELECT_TYPE
		}
	case 3:
		sqDollar = sqS[sqpt-4 : sqpt+1]
		//line query.y:71
		{
			sqlex.(*sqLex).query.where = sqDollar[3].dict
			sqlex.(*sqLex).query.data = sqDollar[2].data
			sqlex.(*sqLex).query.qtype = DATA_TYPE
		}
	case 4:
		sqDollar = sqS[sqpt-4 : sqpt+1]
		//line query.y:77
		{
			sqlex.(*sqLex).query.data = sqDollar[2].data
			sqlex.(*sqLex).query.where = sqDollar[3].dict
			sqlex.(*sqLex).query.qtype = DELETE_TYPE
		}
	case 5:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:83
		{
			sqlex.(*sqLex).query.Contents = []string{}
			sqlex.(*sqLex).query.where = sqDollar[2].dict
			sqlex.(*sqLex).query.qtype = DELETE_TYPE
		}
	case 6:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:91
		{
			sqVAL.list = List{sqDollar[1].str}
		}
	case 7:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:95
		{
			sqVAL.list = append(List{sqDollar[1].str}, sqDollar[3].list...)
		}
	case 8:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:101
		{
			sqVAL.list = sqDollar[2].list
		}
	case 9:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:106
		{
			sqVAL.list = List{sqDollar[1].str}
		}
	case 10:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:110
		{
			sqVAL.list = append(List{sqDollar[1].str}, sqDollar[3].list...)
		}
	case 11:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:116
		{
			sqlex.(*sqLex).query.Contents = sqDollar[1].list
			sqVAL.list = sqDollar[1].list
		}
	case 12:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:121
		{
			sqVAL.list = List{}
		}
	case 13:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:125
		{
			sqlex.(*sqLex).query.distinct = true
			sqVAL.list = List{sqDollar[2].str}
		}
	case 14:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:130
		{
			sqlex.(*sqLex).query.distinct = true
			sqVAL.list = List{}
		}
	case 15:
		sqDollar = sqS[sqpt-9 : sqpt+1]
		//line query.y:137
		{
			sqVAL.data = &DataQuery{Dtype: IN_TYPE, Start: sqDollar[4].time, End: sqDollar[6].time, Limit: sqDollar[8].limit, Timeconv: sqDollar[9].timeconv, IsStatistical: false, IsWindow: false, IsChangedRanges: false}
		}
	case 16:
		sqDollar = sqS[sqpt-7 : sqpt+1]
		//line query.y:141
		{
			sqVAL.data = &DataQuery{Dtype: IN_TYPE, Start: sqDollar[3].time, End: sqDollar[5].time, Limit: sqDollar[6].limit, Timeconv: sqDollar[7].timeconv, IsStatistical: false, IsWindow: false, IsChangedRanges: false}
		}
	case 17:
		sqDollar = sqS[sqpt-13 : sqpt+1]
		//line query.y:145
		{
			num, err := strconv.ParseInt(sqDollar[3].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[3].str, err.Error()))
			}
			sqVAL.data = &DataQuery{Dtype: IN_TYPE, Start: sqDollar[8].time, End: sqDollar[10].time, Limit: sqDollar[12].limit, Timeconv: sqDollar[13].timeconv, IsStatistical: true, IsWindow: false, IsChangedRanges: false, PointWidth: uint64(num)}
		}
	case 18:
		sqDollar = sqS[sqpt-13 : sqpt+1]
		//line query.y:153
		{
			num, err := strconv.ParseInt(sqDollar[3].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[3].str, err.Error()))
			}
			sqVAL.data = &DataQuery{Dtype: IN_TYPE, Start: sqDollar[8].time, End: sqDollar[10].time, Limit: sqDollar[12].limit, Timeconv: sqDollar[13].timeconv, IsStatistical: true, IsWindow: false, IsChangedRanges: false, PointWidth: uint64(num)}
		}
	case 19:
		sqDollar = sqS[sqpt-14 : sqpt+1]
		//line query.y:161
		{
			dur, err := common.ParseReltime(sqDollar[3].str, sqDollar[4].str)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Error parsing relative time \"%v %v\" (%v)", sqDollar[3].str, sqDollar[4].str, err.Error()))
			}
			sqVAL.data = &DataQuery{Dtype: IN_TYPE, Start: sqDollar[9].time, End: sqDollar[11].time, Limit: sqDollar[13].limit, Timeconv: sqDollar[14].timeconv, IsStatistical: false, IsWindow: true, IsChangedRanges: false, Width: uint64(dur.Nanoseconds())}
		}
	case 20:
		sqDollar = sqS[sqpt-9 : sqpt+1]
		//line query.y:169
		{
			fromgen, err := strconv.ParseInt(sqDollar[3].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[3].str, err.Error()))
			}
			togen, err := strconv.ParseInt(sqDollar[5].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[5].str, err.Error()))
			}
			resolution, err := strconv.ParseInt(sqDollar[7].str, 10, 8)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[7].str, err.Error()))
			}
			sqVAL.data = &DataQuery{Dtype: CHANGED_TYPE, IsStatistical: false, IsWindow: false, IsChangedRanges: true, FromGen: uint64(fromgen), ToGen: uint64(togen), Resolution: uint8(resolution)}
		}
	case 21:
		sqDollar = sqS[sqpt-5 : sqpt+1]
		//line query.y:185
		{
			sqVAL.data = &DataQuery{Dtype: BEFORE_TYPE, Start: sqDollar[3].time, Limit: sqDollar[4].limit, Timeconv: sqDollar[5].timeconv, IsStatistical: false, IsWindow: false, IsChangedRanges: false}
		}
	case 22:
		sqDollar = sqS[sqpt-5 : sqpt+1]
		//line query.y:189
		{
			sqVAL.data = &DataQuery{Dtype: AFTER_TYPE, Start: sqDollar[3].time, Limit: sqDollar[4].limit, Timeconv: sqDollar[5].timeconv, IsStatistical: false, IsWindow: false, IsChangedRanges: false}
		}
	case 23:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:195
		{
			sqVAL.time = sqDollar[1].time
		}
	case 24:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:199
		{
			sqVAL.time = sqDollar[1].time.Add(sqDollar[2].timediff)
		}
	case 25:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:205
		{
			foundtime, err := common.ParseAbsTime(sqDollar[1].str, sqDollar[2].str)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse time \"%v %v\" (%v)", sqDollar[1].str, sqDollar[2].str, err.Error()))
			}
			sqVAL.time = foundtime
		}
	case 26:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:213
		{
			num, err := strconv.ParseInt(sqDollar[1].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[1].str, err.Error()))
			}
			sqVAL.time = _time.Unix(num, 0)
		}
	case 27:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:221
		{
			found := false
			for _, format := range supported_formats {
				t, err := _time.Parse(format, sqDollar[1].str)
				if err != nil {
					continue
				}
				sqVAL.time = t
				found = true
				break
			}
			if !found {
				sqlex.(*sqLex).Error(fmt.Sprintf("No time format matching \"%v\" found", sqDollar[1].str))
			}
		}
	case 28:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:237
		{
			sqVAL.time = _time.Now()
		}
	case 29:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:243
		{
			var err error
			sqVAL.timediff, err = common.ParseReltime(sqDollar[1].str, sqDollar[2].str)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Error parsing relative time \"%v %v\" (%v)", sqDollar[1].str, sqDollar[2].str, err.Error()))
			}
		}
	case 30:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:251
		{
			newDuration, err := common.ParseReltime(sqDollar[1].str, sqDollar[2].str)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Error parsing relative time \"%v %v\" (%v)", sqDollar[1].str, sqDollar[2].str, err.Error()))
			}
			sqVAL.timediff = common.AddDurations(newDuration, sqDollar[3].timediff)
		}
	case 31:
		sqDollar = sqS[sqpt-0 : sqpt+1]
		//line query.y:261
		{
			sqVAL.limit = Limit{Limit: -1, Streamlimit: -1}
		}
	case 32:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:265
		{
			num, err := strconv.ParseInt(sqDollar[2].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[2].str, err.Error()))
			}
			sqVAL.limit = Limit{Limit: num, Streamlimit: -1}
		}
	case 33:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:273
		{
			num, err := strconv.ParseInt(sqDollar[2].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[2].str, err.Error()))
			}
			sqVAL.limit = Limit{Limit: -1, Streamlimit: num}
		}
	case 34:
		sqDollar = sqS[sqpt-4 : sqpt+1]
		//line query.y:281
		{
			limit_num, err := strconv.ParseInt(sqDollar[2].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[2].str, err.Error()))
			}
			slimit_num, err := strconv.ParseInt(sqDollar[4].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[2].str, err.Error()))
			}
			sqVAL.limit = Limit{Limit: limit_num, Streamlimit: slimit_num}
		}
	case 35:
		sqDollar = sqS[sqpt-0 : sqpt+1]
		//line query.y:295
		{
			sqVAL.timeconv = common.UOT_MS
		}
	case 36:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:299
		{
			uot, err := common.ParseUOT(sqDollar[2].str)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse unit of time %v (%v)", sqDollar[2].str, err))
			}
			sqVAL.timeconv = uot
		}
	case 37:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:311
		{
			sqVAL.dict = sqDollar[2].dict
		}
	case 38:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:318
		{
			sqVAL.dict = common.Dict{sqDollar[1].str: common.Dict{"$regex": sqDollar[3].str}}
		}
	case 39:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:322
		{
			sqVAL.dict = common.Dict{sqDollar[1].str: sqDollar[3].str}
		}
	case 40:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:326
		{
			sqVAL.dict = common.Dict{sqDollar[1].str: sqDollar[3].str}
		}
	case 41:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:330
		{
			sqVAL.dict = common.Dict{sqDollar[1].str: common.Dict{"$neq": sqDollar[3].str}}
		}
	case 42:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:334
		{
			sqVAL.dict = common.Dict{sqDollar[2].str: common.Dict{"$exists": true}}
		}
	case 43:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:338
		{
			sqVAL.dict = common.Dict{sqDollar[3].str: common.Dict{"$in": sqDollar[1].list}}
		}
	case 44:
		sqDollar = sqS[sqpt-4 : sqpt+1]
		//line query.y:342
		{
			sqVAL.dict = common.Dict{sqDollar[3].str: common.Dict{"$not": common.Dict{"$in": sqDollar[1].list}}}
		}
	case 45:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:346
		{
			sqVAL.dict = sqDollar[2].dict
		}
	case 46:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:352
		{
			sqVAL.str = strings.Trim(sqDollar[1].str, "\"")
		}
	case 47:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:358
		{

			sqlex.(*sqLex)._keys[sqDollar[1].str] = struct{}{}
			sqVAL.str = cleantagstring(sqDollar[1].str)
		}
	case 48:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:366
		{
			sqVAL.dict = common.Dict{"$and": []common.Dict{sqDollar[1].dict, sqDollar[3].dict}}
		}
	case 49:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:370
		{
			sqVAL.dict = common.Dict{"$or": []common.Dict{sqDollar[1].dict, sqDollar[3].dict}}
		}
	case 50:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:374
		{
			tmp := make(common.Dict)
			for k, v := range sqDollar[2].dict {
				tmp[k] = common.Dict{"$ne": v}
			}
			sqVAL.dict = tmp
		}
	case 51:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:382
		{
			sqVAL.dict = sqDollar[1].dict
		}
	}
	goto sqstack /* stack new state and value */
}
