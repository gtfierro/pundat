//go:generate go tool yacc -o expr.go -p ex expr.y
package ob

import (
	"fmt"
	"reflect"
)

var l *lexer

func init() {
	l = NewExprLexer()
}

func Parse(s string) []Operation {
	ops, err := l.Parse(s)
	if err != nil {
		fmt.Println(err)
	}
	return ops
}

func Eval(ops []Operation, v interface{}) interface{} {
	for _, op := range ops {
		v = op.Eval(v)
	}
	return v
}

type Operation interface {
	Eval(interface{}) interface{}
}

type ArrayOperator struct {
	index       int
	slice       bool
	slice_start int
	slice_end   int
	all         bool
}

func (o ArrayOperator) Eval(i interface{}) interface{} {
	val := reflect.ValueOf(i)
	if !val.IsValid() {
		return nil
	}
	kind := val.Type().Kind()
	isarray := (kind == reflect.Slice || kind == reflect.Array)

	if !isarray || o.all {
		return i
	}

	length := val.Len()

	// execute slice
	if o.slice {
		if o.slice_end > length {
			o.slice_end = length
		}
		return val.Slice(o.slice_start, o.slice_end).Interface()
	}

	// return index
	if o.index > length {
		return val.Index(length - 1).Interface()
	} else if o.index < 0 {
		return val.Index(length + o.index).Interface()
	}

	ret := val.Index(o.index)
	if !ret.IsValid() {
		return nil
	}

	return ret.Interface()
}

type ObjectOperator struct {
	key string
}

func (o ObjectOperator) Eval(i interface{}) interface{} {
	val := reflect.ValueOf(i)
	if !val.IsValid() {
		return nil
	}
	kind := val.Type().Kind()
	if kind == reflect.Ptr {
		val = val.Elem()
		if !val.IsValid() {
			return nil
		}
		kind = val.Type().Kind()
	}
	ismap := (kind == reflect.Map)
	isstruct := (kind == reflect.Struct)
	isarray := (kind == reflect.Slice || kind == reflect.Array)

	if isarray {
		var results []interface{}
		for i := 0; i < val.Len(); i++ {
			results = append(results, o.Eval(val.Index(i).Interface()))
		}
		return results
	}

	if !(ismap || isstruct) {
		fmt.Println("nope", i)
		return nil
	}

	if ismap {
		try := val.MapIndex(reflect.ValueOf(o.key))
		if try.IsValid() {
			return try.Interface()
		} else {
			return nil
		}
	}
	// is struct
	if val.FieldByName(o.key).IsValid() {
		return val.FieldByName(o.key).Interface()
	}
	return nil
}
