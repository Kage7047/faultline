package kv

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// Input is one operation as Porcupine sees it.
type Input struct {
	Op    Op
	Key   string
	Value string
}

// Output is what the operation returned. Known is false for a write whose answer never came
// back: it may or may not have happened.
type Output struct {
	Value string
	Known bool
}

// Model is a single-copy key-value register per key. Writes return the value after them, so the
// model checks those too: a retried append that was applied twice shows up as a wrong output.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			k := op.Input.(Input).Key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		st, in, out := state.(string), input.(Input), output.(Output)
		next := st
		switch in.Op {
		case Put:
			next = in.Value
		case Append:
			next = st + in.Value
		}
		return !out.Known || out.Value == next, next
	},
	DescribeOperation: func(input, output any) string {
		in, out := input.(Input), output.(Output)
		res := "?"
		if out.Known {
			res = fmt.Sprintf("%q", out.Value)
		}
		if in.Op == Get {
			return fmt.Sprintf("get(%s) -> %s", in.Key, res)
		}
		return fmt.Sprintf("%s(%s, %q) -> %s", in.Op, in.Key, in.Value, res)
	},
	DescribeState: func(state any) string { return fmt.Sprintf("%q", state) },
}
