//line query.y:2
package querylang

import __yyfmt__ "fmt"

//line query.y:3
import (
	"bufio"
	"fmt"
	"github.com/gtfierro/pundat/common"
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
const MATCHES = 57370
const AND = 57371
const OR = 57372
const HAS = 57373
const NOT = 57374
const IN = 57375
const TO = 57376
const LPAREN = 57377
const RPAREN = 57378
const LBRACK = 57379
const RBRACK = 57380
const NUMBER = 57381
const SEMICOLON = 57382
const NEWLINE = 57383
const TIMEUNIT = 57384

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
	"MATCHES",
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

//line query.y:391

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
			{Token: WHERE, Pattern: "\bwhere\b"},
			{Token: SELECT, Pattern: "select\b"},
			{Token: APPLY, Pattern: "\bapply\b"},
			{Token: DELETE, Pattern: "\bdelete\b"},
			{Token: DISTINCT, Pattern: "\bdistinct\b"},
			{Token: STATISTICAL, Pattern: "\bstatistical\b"},
			{Token: STATISTICS, Pattern: "\bstatistics\b"},
			{Token: WINDOW, Pattern: "\bwindow\b"},
			{Token: CHANGED, Pattern: "\bchanged\b"},
			{Token: LIMIT, Pattern: "\blimit\b"},
			{Token: STREAMLIMIT, Pattern: "\bstreamlimit\b"},
			{Token: ALL, Pattern: "\\*"},
			{Token: NOW, Pattern: "\bnow\b"},
			{Token: BEFORE, Pattern: "\bbefore\b"},
			{Token: AFTER, Pattern: "\bafter\b"},
			{Token: COMMA, Pattern: ","},
			{Token: AND, Pattern: "\band\b"},
			{Token: AS, Pattern: "\bas\b"},
			{Token: TO, Pattern: "\bto\b"},
			{Token: DATA, Pattern: "\bdata\b"},
			{Token: MATCHES, Pattern: "\bmatches\b"},
			{Token: OR, Pattern: "\bor\b"},
			{Token: IN, Pattern: "\bin\b"},
			{Token: HAS, Pattern: "\bhas\b"},
			{Token: NOT, Pattern: "\bnot\b"},
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

const sqNprod = 53
const sqPrivate = 57344

var sqTokenNames []string
var sqStates []string

const sqLast = 169

var sqAct = [...]int{

	99, 77, 48, 74, 50, 70, 45, 36, 35, 49,
	14, 50, 58, 49, 43, 50, 22, 42, 32, 18,
	125, 121, 75, 89, 15, 107, 44, 102, 101, 56,
	47, 51, 52, 38, 47, 55, 37, 34, 14, 54,
	40, 66, 41, 61, 71, 65, 15, 20, 53, 69,
	94, 72, 144, 140, 80, 38, 139, 130, 37, 119,
	106, 93, 40, 83, 41, 87, 88, 90, 85, 86,
	82, 81, 129, 123, 122, 91, 29, 28, 27, 26,
	97, 103, 124, 98, 116, 24, 25, 68, 67, 115,
	92, 59, 60, 100, 6, 138, 133, 132, 71, 111,
	108, 109, 112, 110, 23, 8, 118, 96, 10, 12,
	11, 13, 120, 9, 95, 63, 64, 50, 84, 15,
	62, 126, 73, 113, 7, 57, 30, 131, 15, 127,
	128, 76, 17, 78, 79, 114, 134, 19, 21, 136,
	137, 142, 143, 145, 146, 141, 147, 135, 148, 31,
	10, 12, 11, 13, 18, 9, 117, 105, 104, 18,
	2, 5, 3, 1, 46, 16, 39, 4, 33,
}
var sqPact = [...]int{

	156, -1000, 100, 142, 7, 147, -1000, -1000, 109, 71,
	44, 43, 42, 41, 103, -1000, 147, -22, 5, -23,
	-1000, -26, -1000, -9, -5, -5, 9, 0, -4, -10,
	109, -28, -1000, 62, 27, -1000, 94, 109, 97, 55,
	27, 97, -1000, -1000, -5, 99, -17, 112, -1000, -1000,
	-1000, 117, 117, 35, 34, 109, 95, -1000, -1000, 27,
	27, -1000, 97, -16, 97, -1000, -1000, 109, 57, 25,
	12, 91, 84, -5, -1000, 109, -1000, 66, -11, -12,
	66, 145, 144, 24, -14, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 109, -1000, -1000, 97, -5, 117, -17, -1000,
	104, 118, -1000, -1000, 56, 51, 143, 83, -1000, -1000,
	23, 66, -1000, -1000, -18, 39, 38, 49, -19, 117,
	-1000, -1000, -5, -5, 37, 21, 66, 74, 73, -5,
	134, -1000, -5, -5, 72, -1000, 20, 17, -5, 117,
	117, 16, 66, 66, 117, -1000, -1000, 66, -1000,
}
var sqPgo = [...]int{

	0, 168, 8, 132, 167, 94, 5, 166, 161, 6,
	164, 3, 1, 0, 2, 7, 163,
}
var sqR1 = [...]int{

	0, 16, 16, 16, 16, 16, 5, 5, 7, 6,
	6, 4, 4, 4, 4, 8, 8, 8, 8, 8,
	8, 8, 8, 9, 9, 10, 10, 10, 10, 11,
	11, 12, 12, 12, 12, 13, 13, 3, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 14, 15, 1,
	1, 1, 1,
}
var sqR2 = [...]int{

	0, 4, 3, 4, 4, 3, 1, 3, 3, 1,
	3, 1, 1, 2, 1, 9, 7, 13, 13, 14,
	9, 5, 5, 1, 2, 2, 1, 1, 1, 2,
	3, 0, 2, 2, 4, 0, 2, 2, 3, 3,
	3, 3, 2, 2, 3, 4, 3, 1, 1, 3,
	3, 2, 1,
}
var sqChk = [...]int{

	-1000, -16, 4, 6, -4, -8, -5, 24, 5, 13,
	8, 10, 9, 11, -15, 19, -8, -3, 12, -3,
	40, -3, -15, 33, 14, 15, 35, 35, 35, 35,
	23, -3, 40, -1, 32, -2, -15, 31, 28, -7,
	35, 37, 40, 40, 35, -9, -10, 39, -14, 18,
	20, -9, -9, 39, 39, 39, 39, -5, 40, 29,
	30, -2, 26, 21, 22, -15, -14, 33, 32, -2,
	-6, -14, -9, 23, -11, 39, 19, -12, 16, 17,
	-12, 36, 36, -15, 23, -2, -2, -14, -14, 39,
	-14, -15, 33, 36, 38, 23, 23, -9, -15, -13,
	27, 39, 39, -13, 13, 13, 36, 39, -15, -6,
	-9, -12, -11, 19, 17, 33, 33, 13, 23, 36,
	-13, 39, 35, 35, 33, 39, -12, -9, -9, 35,
	36, -13, 23, 23, -9, 13, -9, -9, 23, 36,
	36, -9, -12, -12, 36, -13, -13, -12, -13,
}
var sqDef = [...]int{

	0, -2, 0, 0, 0, 0, 11, 12, 14, 0,
	0, 0, 0, 0, 6, 48, 0, 0, 0, 0,
	2, 0, 13, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 5, 37, 0, 52, 0, 0, 0, 0,
	0, 0, 1, 3, 0, 0, 23, 26, 27, 28,
	47, 31, 31, 0, 0, 0, 0, 7, 4, 0,
	0, 51, 0, 0, 0, 42, 43, 0, 0, 0,
	0, 9, 0, 0, 24, 0, 25, 35, 0, 0,
	35, 0, 0, 0, 0, 49, 50, 38, 39, 40,
	41, 44, 0, 46, 8, 0, 0, 31, 29, 21,
	0, 32, 33, 22, 0, 0, 0, 0, 45, 10,
	0, 35, 30, 36, 0, 0, 0, 0, 0, 31,
	16, 34, 0, 0, 0, 0, 35, 0, 0, 0,
	0, 15, 0, 0, 0, 20, 0, 0, 0, 31,
	31, 0, 35, 35, 31, 17, 18, 35, 19,
}
var sqTok1 = [...]int{

	1,
}
var sqTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42,
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
			sqVAL.timeconv = common.UOT_NS
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
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:338
		{
			//$$ = common.Dict{"$text": common.Dict{"$search": $2}}
			sqVAL.dict = common.Dict{"$where": fmt.Sprintf("JSON.stringify(this).match(new RegExp('%s'))", sqDollar[2].str)}
		}
	case 44:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:343
		{
			sqVAL.dict = common.Dict{sqDollar[3].str: common.Dict{"$in": sqDollar[1].list}}
		}
	case 45:
		sqDollar = sqS[sqpt-4 : sqpt+1]
		//line query.y:347
		{
			sqVAL.dict = common.Dict{sqDollar[3].str: common.Dict{"$not": common.Dict{"$in": sqDollar[1].list}}}
		}
	case 46:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:351
		{
			sqVAL.dict = sqDollar[2].dict
		}
	case 47:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:357
		{
			sqVAL.str = strings.Trim(sqDollar[1].str, "\"'")
		}
	case 48:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:363
		{

			sqlex.(*sqLex)._keys[sqDollar[1].str] = struct{}{}
			sqVAL.str = cleantagstring(sqDollar[1].str)
		}
	case 49:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:371
		{
			sqVAL.dict = common.Dict{"$and": []common.Dict{sqDollar[1].dict, sqDollar[3].dict}}
		}
	case 50:
		sqDollar = sqS[sqpt-3 : sqpt+1]
		//line query.y:375
		{
			sqVAL.dict = common.Dict{"$or": []common.Dict{sqDollar[1].dict, sqDollar[3].dict}}
		}
	case 51:
		sqDollar = sqS[sqpt-2 : sqpt+1]
		//line query.y:379
		{
			tmp := make(common.Dict)
			for k, v := range sqDollar[2].dict {
				tmp[k] = common.Dict{"$ne": v}
			}
			sqVAL.dict = tmp
		}
	case 52:
		sqDollar = sqS[sqpt-1 : sqpt+1]
		//line query.y:387
		{
			sqVAL.dict = sqDollar[1].dict
		}
	}
	goto sqstack /* stack new state and value */
}
