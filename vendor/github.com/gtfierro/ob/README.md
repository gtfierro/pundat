# ObjectBuilder

Objectbuilder takes simple string expressiosn and uses them to extract parts of
larger arrays, slices, maps, structs, or combinations thereof.


## Syntax

Its just normal object syntax:

* `[i]`: return thing at index i
    * supports negative index: `[-1]` will return last item of list
* `[i:j]`: return slice of things between indices i and 
* `[:]`: return all things
* `key1`: (only works at top level): return value at key "key1"
* `.key2`: (only works when inside an object): return nested value at key "key2"

Examples:

* `[0][1]`: return the 2nd item of the list in the 1st item of the root list
* `key1.key2`: return the value of key2 from the nested struct/map/object at key1
* `key1[3]`: return the 4th item of the array at key1
* `key1[3].key3`: return the value of key3 from the 4th item of the array at key1
* `[:].key1`: return a list of values of key1 from every item in the root array

## Usage

For now, this operates on `interface{}`s.


```go
package main

import (
    "github.com/gtfierro/ob"
    "fmt"
)

func main() {
    
    myObject := map[string]interface{}{"key1": []string{"a", "b"}},
    myExpression := "key1[0]"

    fmt.Println(ob.Eval(ob.Parse(myExpression), myObject))
    // -> "b"
}

```
