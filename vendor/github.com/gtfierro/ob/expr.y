%{
package ob

import (
	"github.com/taylorchu/toki"
    "errors"
    "strconv"
    "sync"
)

%}

%union{
    str string
    op Operation
    opl []Operation
}

%token <str> LBRACKET RBRACKET DOT KEY COLON
%token NUMBER

%type <str> KEY NUMBER
%type <op> array object operator
%type <opl> operatorList


%%
expression   : object operatorList
             {
                 exlex.(*lexer).operations = append([]Operation{$1}, $2...)
             }
             | array operatorList
             {
                 exlex.(*lexer).operations = append([]Operation{$1}, $2...)
             }
             | object
             {
                 exlex.(*lexer).operations = []Operation{$1}
             }
             | array
             {
                 exlex.(*lexer).operations = []Operation{$1}
             }
             ;

operatorList : operator
             {
                 $$ = []Operation{$1}
             }
             | operator operatorList
             {
                 $$ = append([]Operation{$1}, $2...)
             }
             ;

operator     : DOT object
             {
                 $$ = $2
             }
             | array
             {
                 $$ = $1
             }
             ;

array       : LBRACKET NUMBER RBRACKET
            {
                num, _ := strconv.Atoi($2)
                $$ = ArrayOperator{index: num, slice: false, all: false}
            }
            | LBRACKET NUMBER COLON NUMBER RBRACKET
            {
                num, _ := strconv.Atoi($2)
                num2, _ := strconv.Atoi($4)
                $$ = ArrayOperator{slice_start: num, slice_end: num2, slice: true, all: false}
            }
            | LBRACKET COLON RBRACKET
            {
                $$ = ArrayOperator{slice: false, all: true}
            }
            ;

object      : KEY
            {
                $$ = ObjectOperator{key: $1}
            }
            ;
%%

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
    scanner *toki.Scanner
    tokens  []string
    lextokens []uint32
    operations []Operation
    error   error
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
