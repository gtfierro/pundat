//line expr.y:2
package ob

import __yyfmt__ "fmt"

//line expr.y:2
import (
	"errors"
	"github.com/taylorchu/toki"
	"strconv"
	"sync"
)

//line expr.y:13
type exSymType struct {
	yys int
	str string
	op  Operation
	opl []Operation
}

const LBRACKET = 57346
const RBRACKET = 57347
const DOT = 57348
const KEY = 57349
const COLON = 57350
const NUMBER = 57351

var exToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"LBRACKET",
	"RBRACKET",
	"DOT",
	"KEY",
	"COLON",
	"NUMBER",
}
var exStatenames = [...]string{}

const exEofCode = 1
const exErrCode = 2
const exInitialStackSize = 16

//line expr.y:88

const eof = 0

func getName(tok uint32) string {
	switch tok {
	case LBRACKET:
		return "LBRACKET"
	case RBRACKET:
		return "RBRACKET"
	case DOT:
		return "DOT"
	case KEY:
		return "KEY"
	case COLON:
		return "COLON"
	case NUMBER:
		return "NUMBER"
	}
	return "UNKNOWN"
}

type lexer struct {
	sync.Mutex
	expression string
	scanner    *toki.Scanner
	tokens     []string
	lextokens  []uint32
	operations []Operation
	error      error
}

func (l *lexer) addOperation(o Operation) {
	l.operations = append(l.operations, o)
}

func NewExprLexer() *lexer {
	l := &lexer{}
	l.scanner = toki.NewScanner(
		[]toki.Def{
			{Token: DOT, Pattern: "\\."},
			{Token: COLON, Pattern: ":"},
			{Token: LBRACKET, Pattern: "\\["},
			{Token: RBRACKET, Pattern: "\\]"},
			{Token: NUMBER, Pattern: "([+-]?([0-9]*\\.)?[0-9]+)"},
			{Token: KEY, Pattern: "[a-zA-Z\\~\\$\\_][a-zA-Z0-9\\/\\%_\\-]*"},
		})
	return l
}

func (l *lexer) Parse(s string) ([]Operation, error) {
	l.Lock()
	defer l.Unlock()
	l.expression = s
	l.operations = []Operation{}
	l.tokens = []string{}
	l.error = nil
	l.scanner.SetInput(s)
	exParse(l)
	return l.operations, l.error
}

func (l *lexer) Lex(lval *exSymType) int {
	r := l.scanner.Next()
	if r.Pos.Line == 2 || len(r.Value) == 0 {
		return eof
	}
	lval.str = string(r.Value)
	l.tokens = append(l.tokens, lval.str)
	l.lextokens = append(l.lextokens, uint32(r.Token))
	return int(r.Token)
}

func (l *lexer) Error(s string) {
	l.error = errors.New(s)
}

//line yacctab:1
var exExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
}

const exNprod = 13
const exPrivate = 57344

var exTokenNames []string
var exStates []string

const exLast = 21

var exAct = [...]int{

	12, 11, 18, 15, 6, 2, 16, 5, 10, 4,
	4, 5, 13, 8, 14, 19, 17, 9, 3, 1,
	7,
}
var exPact = [...]int{

	3, -1000, 7, 7, -1000, -8, -1000, 7, 2, -1000,
	-1000, -2, 11, -1000, -1000, -1000, -7, -1000, 10, -1000,
}
var exPgo = [...]int{

	0, 17, 5, 20, 4, 19,
}
var exR1 = [...]int{

	0, 5, 5, 5, 5, 4, 4, 3, 3, 1,
	1, 1, 2,
}
var exR2 = [...]int{

	0, 2, 2, 1, 1, 1, 2, 2, 1, 3,
	5, 3, 1,
}
var exChk = [...]int{

	-1000, -5, -2, -1, 7, 4, -4, -3, 6, -1,
	-4, 9, 8, -4, -2, 5, 8, 5, 9, 5,
}
var exDef = [...]int{

	0, -2, 3, 4, 12, 0, 1, 5, 0, 8,
	2, 0, 0, 6, 7, 9, 0, 11, 0, 10,
}
var exTok1 = [...]int{

	1,
}
var exTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9,
}
var exTok3 = [...]int{
	0,
}

var exErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

//line yaccpar:1

/*	parser for yacc output	*/

var (
	exDebug        = 0
	exErrorVerbose = false
)

type exLexer interface {
	Lex(lval *exSymType) int
	Error(s string)
}

type exParser interface {
	Parse(exLexer) int
	Lookahead() int
}

type exParserImpl struct {
	lval  exSymType
	stack [exInitialStackSize]exSymType
	char  int
}

func (p *exParserImpl) Lookahead() int {
	return p.char
}

func exNewParser() exParser {
	return &exParserImpl{}
}

const exFlag = -1000

func exTokname(c int) string {
	if c >= 1 && c-1 < len(exToknames) {
		if exToknames[c-1] != "" {
			return exToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func exStatname(s int) string {
	if s >= 0 && s < len(exStatenames) {
		if exStatenames[s] != "" {
			return exStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func exErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !exErrorVerbose {
		return "syntax error"
	}

	for _, e := range exErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + exTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := exPact[state]
	for tok := TOKSTART; tok-1 < len(exToknames); tok++ {
		if n := base + tok; n >= 0 && n < exLast && exChk[exAct[n]] == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if exDef[state] == -2 {
		i := 0
		for exExca[i] != -1 || exExca[i+1] != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; exExca[i] >= 0; i += 2 {
			tok := exExca[i]
			if tok < TOKSTART || exExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if exExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += exTokname(tok)
	}
	return res
}

func exlex1(lex exLexer, lval *exSymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = exTok1[0]
		goto out
	}
	if char < len(exTok1) {
		token = exTok1[char]
		goto out
	}
	if char >= exPrivate {
		if char < exPrivate+len(exTok2) {
			token = exTok2[char-exPrivate]
			goto out
		}
	}
	for i := 0; i < len(exTok3); i += 2 {
		token = exTok3[i+0]
		if token == char {
			token = exTok3[i+1]
			goto out
		}
	}

out:
	if token == 0 {
		token = exTok2[1] /* unknown char */
	}
	if exDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", exTokname(token), uint(char))
	}
	return char, token
}

func exParse(exlex exLexer) int {
	return exNewParser().Parse(exlex)
}

func (exrcvr *exParserImpl) Parse(exlex exLexer) int {
	var exn int
	var exVAL exSymType
	var exDollar []exSymType
	_ = exDollar // silence set and not used
	exS := exrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	exstate := 0
	exrcvr.char = -1
	extoken := -1 // exrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		exstate = -1
		exrcvr.char = -1
		extoken = -1
	}()
	exp := -1
	goto exstack

ret0:
	return 0

ret1:
	return 1

exstack:
	/* put a state and value onto the stack */
	if exDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", exTokname(extoken), exStatname(exstate))
	}

	exp++
	if exp >= len(exS) {
		nyys := make([]exSymType, len(exS)*2)
		copy(nyys, exS)
		exS = nyys
	}
	exS[exp] = exVAL
	exS[exp].yys = exstate

exnewstate:
	exn = exPact[exstate]
	if exn <= exFlag {
		goto exdefault /* simple state */
	}
	if exrcvr.char < 0 {
		exrcvr.char, extoken = exlex1(exlex, &exrcvr.lval)
	}
	exn += extoken
	if exn < 0 || exn >= exLast {
		goto exdefault
	}
	exn = exAct[exn]
	if exChk[exn] == extoken { /* valid shift */
		exrcvr.char = -1
		extoken = -1
		exVAL = exrcvr.lval
		exstate = exn
		if Errflag > 0 {
			Errflag--
		}
		goto exstack
	}

exdefault:
	/* default state action */
	exn = exDef[exstate]
	if exn == -2 {
		if exrcvr.char < 0 {
			exrcvr.char, extoken = exlex1(exlex, &exrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if exExca[xi+0] == -1 && exExca[xi+1] == exstate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			exn = exExca[xi+0]
			if exn < 0 || exn == extoken {
				break
			}
		}
		exn = exExca[xi+1]
		if exn < 0 {
			goto ret0
		}
	}
	if exn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			exlex.Error(exErrorMessage(exstate, extoken))
			Nerrs++
			if exDebug >= 1 {
				__yyfmt__.Printf("%s", exStatname(exstate))
				__yyfmt__.Printf(" saw %s\n", exTokname(extoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for exp >= 0 {
				exn = exPact[exS[exp].yys] + exErrCode
				if exn >= 0 && exn < exLast {
					exstate = exAct[exn] /* simulate a shift of "error" */
					if exChk[exstate] == exErrCode {
						goto exstack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if exDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", exS[exp].yys)
				}
				exp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if exDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", exTokname(extoken))
			}
			if extoken == exEofCode {
				goto ret1
			}
			exrcvr.char = -1
			extoken = -1
			goto exnewstate /* try again in the same state */
		}
	}

	/* reduction by production exn */
	if exDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", exn, exStatname(exstate))
	}

	exnt := exn
	expt := exp
	_ = expt // guard against "declared and not used"

	exp -= exR2[exn]
	// exp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if exp+1 >= len(exS) {
		nyys := make([]exSymType, len(exS)*2)
		copy(nyys, exS)
		exS = nyys
	}
	exVAL = exS[exp+1]

	/* consult goto table to find next state */
	exn = exR1[exn]
	exg := exPgo[exn]
	exj := exg + exS[exp].yys + 1

	if exj >= exLast {
		exstate = exAct[exg]
	} else {
		exstate = exAct[exj]
		if exChk[exstate] != -exn {
			exstate = exAct[exg]
		}
	}
	// dummy call; replaced with literal code
	switch exnt {

	case 1:
		exDollar = exS[expt-2 : expt+1]
		//line expr.y:29
		{
			exlex.(*lexer).operations = append([]Operation{exDollar[1].op}, exDollar[2].opl...)
		}
	case 2:
		exDollar = exS[expt-2 : expt+1]
		//line expr.y:33
		{
			exlex.(*lexer).operations = append([]Operation{exDollar[1].op}, exDollar[2].opl...)
		}
	case 3:
		exDollar = exS[expt-1 : expt+1]
		//line expr.y:37
		{
			exlex.(*lexer).operations = []Operation{exDollar[1].op}
		}
	case 4:
		exDollar = exS[expt-1 : expt+1]
		//line expr.y:41
		{
			exlex.(*lexer).operations = []Operation{exDollar[1].op}
		}
	case 5:
		exDollar = exS[expt-1 : expt+1]
		//line expr.y:47
		{
			exVAL.opl = []Operation{exDollar[1].op}
		}
	case 6:
		exDollar = exS[expt-2 : expt+1]
		//line expr.y:51
		{
			exVAL.opl = append([]Operation{exDollar[1].op}, exDollar[2].opl...)
		}
	case 7:
		exDollar = exS[expt-2 : expt+1]
		//line expr.y:57
		{
			exVAL.op = exDollar[2].op
		}
	case 8:
		exDollar = exS[expt-1 : expt+1]
		//line expr.y:61
		{
			exVAL.op = exDollar[1].op
		}
	case 9:
		exDollar = exS[expt-3 : expt+1]
		//line expr.y:67
		{
			num, _ := strconv.Atoi(exDollar[2].str)
			exVAL.op = ArrayOperator{index: num, slice: false, all: false}
		}
	case 10:
		exDollar = exS[expt-5 : expt+1]
		//line expr.y:72
		{
			num, _ := strconv.Atoi(exDollar[2].str)
			num2, _ := strconv.Atoi(exDollar[4].str)
			exVAL.op = ArrayOperator{slice_start: num, slice_end: num2, slice: true, all: false}
		}
	case 11:
		exDollar = exS[expt-3 : expt+1]
		//line expr.y:78
		{
			exVAL.op = ArrayOperator{slice: false, all: true}
		}
	case 12:
		exDollar = exS[expt-1 : expt+1]
		//line expr.y:84
		{
			exVAL.op = ObjectOperator{key: exDollar[1].str}
		}
	}
	goto exstack /* stack new state and value */
}
