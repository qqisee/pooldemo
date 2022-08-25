package main

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/jolestar/go-commons-pool/v2"
)

//func main() {
//	ExampleSimple()
//}

// ExampleSimple 简单例子
func ExampleSimple() {
	type myPoolObject struct {
		s string
	}

	v := uint64(0)

	factory := pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			return &myPoolObject{
					s: strconv.FormatUint(atomic.AddUint64(&v, 1), 10),
				},
				nil
		})

	ctx := context.Background()
	p := pool.NewObjectPoolWithDefaultConfig(ctx, factory)

	obj, err := p.BorrowObject(ctx) //借一个对象
	if err != nil {
		panic(err)
	}

	o := obj.(*myPoolObject)
	fmt.Println(o.s)

	err = p.ReturnObject(ctx, obj)
	if err != nil {
		panic(err)
	}

	// Output: 1
}
