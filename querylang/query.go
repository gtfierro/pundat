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
const WHERE = 57353
const DATA = 57354
const BEFORE = 57355
const AFTER = 57356
const LIMIT = 57357
const STREAMLIMIT = 57358
const NOW = 57359
const LVALUE = 57360
const QSTRING = 57361
const EQ = 57362
const NEQ = 57363
const COMMA = 57364
const ALL = 57365
const LEFTPIPE = 57366
const LIKE = 57367
const AS = 57368
const AND = 57369
const OR = 57370
const HAS = 57371
const NOT = 57372
const IN = 57373
const TO = 57374
const LPAREN = 57375
const RPAREN = 57376
const LBRACK = 57377
const RBRACK = 57378
const NUMBER = 57379
const SEMICOLON = 57380
const NEWLINE = 57381
const TIMEUNIT = 57382

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

//line query.y:411

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

const sqNprod = 51
const sqPrivate = 57344

var sqTokenNames []string
var sqStates []string

const sqLast = 157

var sqAct = [...]int{

	93, 72, 69, 34, 65, 33, 13, 14, 17, 46,
	45, 47, 21, 46, 54, 47, 40, 39, 35, 32,
	30, 113, 37, 42, 38, 41, 47, 70, 96, 44,
	95, 52, 13, 44, 88, 19, 51, 50, 57, 61,
	133, 120, 14, 64, 83, 129, 128, 48, 49, 66,
	111, 75, 100, 35, 87, 77, 78, 37, 115, 38,
	76, 79, 80, 114, 27, 67, 85, 26, 25, 81,
	82, 84, 63, 62, 92, 116, 97, 109, 23, 24,
	108, 86, 55, 56, 8, 94, 6, 10, 12, 11,
	101, 9, 91, 104, 102, 105, 22, 14, 59, 60,
	66, 106, 7, 58, 127, 112, 123, 122, 90, 89,
	68, 28, 47, 117, 103, 53, 14, 71, 121, 107,
	16, 73, 74, 110, 99, 18, 20, 98, 17, 1,
	131, 132, 134, 135, 43, 136, 29, 137, 118, 119,
	2, 5, 3, 36, 124, 15, 125, 126, 4, 31,
	0, 130, 10, 12, 11, 17, 9,
}
var sqPact = [...]int{

	136, -1000, 79, 144, -3, 117, -1000, -1000, 98, 65,
	35, 34, 31, 89, -1000, 117, -18, -11, -21, -1000,
	-22, -1000, -8, -4, -4, 0, -1, -6, 98, -24,
	-1000, 55, 24, -1000, 78, 98, 42, 24, 93, -1000,
	-1000, -4, 88, -10, 99, -1000, -1000, -1000, 106, 106,
	26, 21, 98, -1000, -1000, 24, 24, -1000, 93, 7,
	93, -1000, 98, 50, 20, -2, 87, 86, -4, -1000,
	98, -1000, 59, -7, -9, 59, 115, 112, 18, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 98, -1000, -1000, 93,
	-4, 106, -10, -1000, 83, 103, -1000, -1000, 49, 46,
	111, -1000, -1000, 16, 59, -1000, -1000, -16, 30, 25,
	44, 106, -1000, -1000, -4, -4, 8, 59, 85, 84,
	-4, -1000, -4, -4, 82, 12, 11, -4, 106, 106,
	6, 59, 59, 106, -1000, -1000, 59, -1000,
}
var sqPgo = [...]int{

	0, 149, 5, 120, 148, 86, 4, 143, 141, 23,
	134, 2, 1, 0, 10, 3, 129,
}
var sqR1 = [...]int{

	0, 16, 16, 16, 16, 16, 5, 5, 7, 6,
	6, 4, 4, 4, 4, 8, 8, 8, 8, 8,
	8, 8, 9, 9, 10, 10, 10, 10, 11, 11,
	12, 12, 12, 12, 13, 13, 3, 2, 2, 2,
	2, 2, 2, 2, 2, 14, 15, 1, 1, 1,
	1,
}
var sqR2 = [...]int{

	0, 4, 3, 4, 4, 3, 1, 3, 3, 1,
	3, 1, 1, 2, 1, 9, 7, 13, 13, 14,
	5, 5, 1, 2, 2, 1, 1, 1, 2, 3,
	0, 2, 2, 4, 0, 2, 2, 3, 3, 3,
	3, 2, 3, 4, 3, 1, 1, 3, 3, 2,
	1,
}
var sqChk = [...]int{

	-1000, -16, 4, 6, -4, -8, -5, 23, 5, 12,
	8, 10, 9, -15, 18, -8, -3, 11, -3, 38,
	-3, -15, 31, 13, 14, 33, 33, 33, 22, -3,
	38, -1, 30, -2, -15, 29, -7, 33, 35, 38,
	38, 33, -9, -10, 37, -14, 17, 19, -9, -9,
	37, 37, 37, -5, 38, 27, 28, -2, 25, 20,
	21, -15, 31, 30, -2, -6, -14, -9, 22, -11,
	37, 18, -12, 15, 16, -12, 34, 34, -15, -2,
	-2, -14, -14, 37, -14, -15, 31, 34, 36, 22,
	22, -9, -15, -13, 26, 37, 37, -13, 12, 12,
	34, -15, -6, -9, -12, -11, 18, 16, 31, 31,
	12, 34, -13, 37, 33, 33, 31, -12, -9, -9,
	33, -13, 22, 22, -9, -9, -9, 22, 34, 34,
	-9, -12, -12, 34, -13, -13, -12, -13,
}
var sqDef = [...]int{

	0, -2, 0, 0, 0, 0, 11, 12, 14, 0,
	0, 0, 0, 6, 46, 0, 0, 0, 0, 2,
	0, 13, 0, 0, 0, 0, 0, 0, 0, 0,
	5, 36, 0, 50, 0, 0, 0, 0, 0, 1,
	3, 0, 0, 22, 25, 26, 27, 45, 30, 30,
	0, 0, 0, 7, 4, 0, 0, 49, 0, 0,
	0, 41, 0, 0, 0, 0, 9, 0, 0, 23,
	0, 24, 34, 0, 0, 34, 0, 0, 0, 47,
	48, 37, 38, 39, 40, 42, 0, 44, 8, 0,
	0, 30, 28, 20, 0, 31, 32, 21, 0, 0,
	0, 43, 10, 0, 34, 29, 35, 0, 0, 0,
	0, 30, 16, 33, 0, 0, 0, 34, 0, 0,
	0, 15, 0, 0, 0, 0, 0, 0, 30, 30,
	0, 34, 34, 30, 17, 18, 34, 19,
}
var sqTok1 = [...]int{

	1,
}
var sqTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40,
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
			sqVAL.data = &DataQuery{Dtype: IN_TYPE, Start: sqDollar[4].time, End: sqDollar[6].time, Limit: sqDollar[8].limit, Timeconv: sqDollar[9].timeconv, IsStatistical: false, IsWindow: false}
		}
	case 16:
		sqDollar = sqS[sqpt-7 : sqpt+1]
		//line query.y:141
		{
			sqVAL.data = &DataQuery{Dtype: IN_TYPE, Start: sqDollar[3].time, End: sqDollar[5].time, Limit: sqDollar[6].limit, Timeconv: sqDollar[7].timeconv, IsStatistical: false, IsWindow: false}
		}
	case 17:
		sqDollar = sqS[sqpt-13 : sqpt+1]
		//line query.y:145
		{
			num, err := strconv.ParseInt(sqDollar[3].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[1].str, err.Error()))
			}
			sqVAL.data = &DataQuery{Dtype: IN_TYPE, Start: sqDollar[8].time, End: sqDollar[10].time, Limit: sqDollar[12].limit, Timeconv: sqDollar[13].timeconv, IsStatistical: true, IsWindow: false, PointWidth: uint64(num)}
		}
	case 18:
		sqDollar = sqS[sqpt-13 : sqpt+1]
		//line query.y:153
		{
			num, err := strconv.ParseInt(sqDollar[3].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[1].str, err.Error()))
			}
			sqVAL.data = &DataQuery{Dtype: IN_TYPE, Start: sqDollar[8].time, End: sqDollar[10].time, Limit: sqDollar[12].limit, Timeconv: sqDollar[13].timeconv, IsStatistical: true, IsWindow: false, PointWidth: uint64(num)}
		}
	case 19:
		sqDollar = sqS[sqpt-14 : sqpt+1]
		//line query.y:161
		{
			dur, err := common.ParseReltime(sqDollar[3].str, sqDollar[4].str)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Error parsing relative time \"%v %v\" (%v)", sqDollar[3].str, sqDollar[4].str, err.Error()))
			}
			sqVAL.data = &DataQuery{Dtype: IN_TYPE, Start: sqDollar[9].time, End: sqDollar[11].time, Limit: sqDollar[13].limit, Timeconv: sqDollar[14].timeconv, IsStatistical: false, IsWindow: true, Width: uint64(dur.Nanoseconds())}
		}
	case 20:
		sqDollar = sqS[sqpt-5 : sqpt+1]
		//line query.y:169
		{
			sqVAL.data = &DataQuery{Dtype: BEFORE_TYPE, Start: sqDollar[3].time, Limit: sqDollar[4].limit, Timeconv: sqDollar[5].timeconv, IsStatistical: false, IsWindow: false}
		}
	case 21:
		sqDollar = sqS[sqpt-5 : sqpt+1]
		//line query.y:173
		{
			sqVAL.data = &DataQuery{Dtype: AFTER_TYPE, Start: sqDollar[3].time, Limit: sqDollar[4].limit, Timeconv: sqDollar[5].timeconv, IsStatistical: false, IsWindow: false}
		}
	case 22:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:179
		{
			sqVAL.time = sqDollar[1].time
		}
	case 23:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:183
		{
			sqVAL.time = sqDollar[1].time.Add(sqDollar[2].timediff)
		}
	case 24:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:189
		{
			foundtime, err := common.ParseAbsTime(sqDollar[1].str, sqDollar[2].str)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse time \"%v %v\" (%v)", sqDollar[1].str, sqDollar[2].str, err.Error()))
			}
			sqVAL.time = foundtime
		}
	case 25:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:197
		{
			num, err := strconv.ParseInt(sqDollar[1].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[1].str, err.Error()))
			}
			sqVAL.time = _time.Unix(num, 0)
		}
	case 26:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:205
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
	case 27:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:221
		{
			sqVAL.time = _time.Now()
		}
	case 28:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:227
		{
			var err error
			sqVAL.timediff, err = common.ParseReltime(sqDollar[1].str, sqDollar[2].str)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Error parsing relative time \"%v %v\" (%v)", sqDollar[1].str, sqDollar[2].str, err.Error()))
			}
		}
	case 29:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:235
		{
			newDuration, err := common.ParseReltime(sqDollar[1].str, sqDollar[2].str)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Error parsing relative time \"%v %v\" (%v)", sqDollar[1].str, sqDollar[2].str, err.Error()))
			}
			sqVAL.timediff = common.AddDurations(newDuration, sqDollar[3].timediff)
		}
	case 30:
		sqDollar = sqS[sqpt-0 : sqpt+1]
		//line query.y:245
		{
			sqVAL.limit = Limit{Limit: -1, Streamlimit: -1}
		}
	case 31:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:249
		{
			num, err := strconv.ParseInt(sqDollar[2].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[2].str, err.Error()))
			}
			sqVAL.limit = Limit{Limit: num, Streamlimit: -1}
		}
	case 32:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:257
		{
			num, err := strconv.ParseInt(sqDollar[2].str, 10, 64)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse integer \"%v\" (%v)", sqDollar[2].str, err.Error()))
			}
			sqVAL.limit = Limit{Limit: -1, Streamlimit: num}
		}
	case 33:
		sqDollar = sqS[sqpt-4 : sqpt+1]
		//line query.y:265
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
	case 34:
		sqDollar = sqS[sqpt-0 : sqpt+1]
		//line query.y:279
		{
			sqVAL.timeconv = common.UOT_MS
		}
	case 35:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:283
		{
			uot, err := common.ParseUOT(sqDollar[2].str)
			if err != nil {
				sqlex.(*sqLex).Error(fmt.Sprintf("Could not parse unit of time %v (%v)", sqDollar[2].str, err))
			}
			sqVAL.timeconv = uot
		}
	case 36:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:295
		{
			sqVAL.dict = sqDollar[2].dict
		}
	case 37:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:302
		{
			key := fixMongoKey(sqDollar[1].str)
			if key == "uuid" {
				sqVAL.dict = common.Dict{"uuid": common.Dict{"$regex": sqDollar[3].str}}
			} else {
				sqVAL.dict = common.Dict{"key": key, "value": common.Dict{"$regex": sqDollar[3].str}}
			}
		}
	case 38:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:311
		{
			key := fixMongoKey(sqDollar[1].str)
			if key == "uuid" {
				sqVAL.dict = common.Dict{"uuid": sqDollar[3].str}
			} else {
				sqVAL.dict = common.Dict{"key": key, "value": sqDollar[3].str}
			}
		}
	case 39:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:320
		{
			key := fixMongoKey(sqDollar[1].str)
			if key == "uuid" {
				sqVAL.dict = common.Dict{"uuid": sqDollar[3].str}
			} else {
				sqVAL.dict = common.Dict{"key": key, "value": sqDollar[3].str}
			}
		}
	case 40:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:329
		{
			key := fixMongoKey(sqDollar[1].str)
			if key == "uuid" {
				sqVAL.dict = common.Dict{"uuid": common.Dict{"$neq": sqDollar[3].str}}
			} else {
				sqVAL.dict = common.Dict{"key": key, "value": common.Dict{"$neq": sqDollar[3].str}}
			}
		}
	case 41:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:338
		{
			key := fixMongoKey(sqDollar[2].str)
			if key == "uuid" {
				sqVAL.dict = common.Dict{"uuid": common.Dict{"$exists": true}}
			} else {
				sqVAL.dict = common.Dict{"key": key}
			}
			//$$ = common.Dict{"$and": []common.Dict{{"key": fixMongoKey($2)}}}
			//$$ = common.Dict{fixMongoKey($2): common.Dict{"$exists": true}}
		}
	case 42:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:349
		{
			key := fixMongoKey(sqDollar[3].str)
			if key == "uuid" {
				sqVAL.dict = common.Dict{"uuid": common.Dict{"$in": sqDollar[1].list}}
			} else {
				sqVAL.dict = common.Dict{"$and": []common.Dict{{"key": fixMongoKey(sqDollar[3].str)}, {"value": common.Dict{"$in": sqDollar[1].list}}}}
			}
			//$$ = common.Dict{fixMongoKey($3): common.Dict{"$in": $1}}
		}
	case 43:
		sqDollar = sqS[sqpt-4 : sqpt+1]
		//line query.y:359
		{
			key := fixMongoKey(sqDollar[3].str)
			if key == "uuid" {
				sqVAL.dict = common.Dict{"uuid": common.Dict{"$in": sqDollar[1].list}}
			} else {
				sqVAL.dict = common.Dict{"$and": []common.Dict{{"key": fixMongoKey(sqDollar[3].str)}, {"$not": common.Dict{"value": common.Dict{"$in": sqDollar[1].list}}}}}
			}
			//$$ = common.Dict{"$and": []common.Dict{{"key": fixMongoKey($3)}, {"$not": common.Dict{"value": common.Dict{"$in": $1}}}}}
			//$$ = common.Dict{fixMongoKey($3): common.Dict{"$not": common.Dict{"$in": $1}}}
		}
	case 44:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:370
		{
			sqVAL.dict = sqDollar[2].dict
		}
	case 45:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:376
		{
			sqVAL.str = strings.Trim(sqDollar[1].str, "\"")
			//$$ = $1[1:len($1)-1]
		}
	case 46:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:383
		{

			sqlex.(*sqLex)._keys[sqDollar[1].str] = struct{}{}
			sqVAL.str = cleantagstring(sqDollar[1].str)
		}
	case 47:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:391
		{
			sqVAL.dict = common.Dict{"$and": []common.Dict{sqDollar[1].dict, sqDollar[3].dict}}
		}
	case 48:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:395
		{
			sqVAL.dict = common.Dict{"$or": []common.Dict{sqDollar[1].dict, sqDollar[3].dict}}
		}
	case 49:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:399
		{
			tmp := make(common.Dict)
			for k, v := range sqDollar[2].dict {
				tmp[k] = common.Dict{"$ne": v}
			}
			sqVAL.dict = tmp
		}
	case 50:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:407
		{
			sqVAL.dict = sqDollar[1].dict
		}
	}
	goto sqstack /* stack new state and value */
}
