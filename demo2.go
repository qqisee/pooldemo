package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jolestar/go-commons-pool/v2"
	"math/rand"
	"sync"
	"time"
)

type MysqlParams struct {
	USERNAME string
	PASSWORD string
	NETWORK  string
	SERVER   string
	PORT     int
	DATABASE string
}
type MysqlClient struct {
	db *sql.DB
}

type MysqlPoolFactory struct {
}

func (mp MysqlParams) newMyqsl() *sql.DB {
	conn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", mp.USERNAME, mp.PASSWORD, mp.NETWORK, mp.SERVER, mp.PORT, mp.DATABASE)
	DB, err := sql.Open("mysql", conn)
	if err != nil {
		fmt.Println("connection to mysql failed:", err)
		return nil
	}
	// 把自己的db最大连接数设置为1，防止db自己维护了一个连接池
	DB.SetMaxOpenConns(1)
	// 空闲连接数，把自己的db最大空闲数设置为1，防止db自己维护了一个连接池
	DB.SetMaxIdleConns(1)
	return DB
}

// MakeObject make对象，新建一个池化对象
func (f *MysqlPoolFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	return pool.NewPooledObject(
			&MysqlClient{
				db: MysqlParams{
					USERNAME: "root",
					PASSWORD: "root",
					NETWORK:  "tcp",
					SERVER:   "127.0.0.1",
					PORT:     3306,
					DATABASE: "test2",
				}.newMyqsl(),
			}),
		nil
}

func (f *MysqlPoolFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	// do destroy
	db := object.Object.(*MysqlClient)
	db.db.Close()
	fmt.Println("销毁了")
	return nil
}

func (f *MysqlPoolFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	// do validate 验证连接是否能用
	return true
}

func (f *MysqlPoolFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	// do activate
	fmt.Println("对象重新初始化")
	return nil
}

func (f *MysqlPoolFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	// do passivate 钝化方法
	fmt.Println("返回池子")
	return nil
}

//对象使用完被返回对象池时，如果校验失败直接销毁，如果校验通过需要先钝化对象再存入空闲队列。至于激活对象的方法在上述取用对象时也会先激活再被取出。
/*
makeObject：创建池化对象实例，并且使用 PooledObject 将其封装。

validateObject：验证对象实例是否安全或者可用，比如说 Connection 是否还保存连接状态。

activateObject：将池返回的对象实例进行重新初始化，比如说设置 Connection是否默认AutoCommit等。

passivateObject：将返回给池的对象实例进行反初始化，比如说 Connection 中未提交的事务进行 Rollback等。

destroyObject：销毁不再被池需要的对象实例，比如说 Connection不再被需要时，调用其 close 方法。
*/

func ExampleCustomfactory() {
	var wg sync.WaitGroup
	ctx := context.Background()
	p := pool.NewObjectPoolWithDefaultConfig(ctx, &MysqlPoolFactory{})
	p.Config.MaxTotal = 10
	p.Config.MaxIdle = 10
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			conn, err := p.BorrowObject(ctx) //从池子里拿一个对象
			if err != nil {
				panic(err)
			}
			client := conn.(*MysqlClient)
			rows, _ := client.db.Query("SELECT version()") //获取所有数据
			for rows.Next() {
				var ver string
				rows.Scan(&ver)
				fmt.Println(ver)
			}
			rand.Seed(time.Now().UnixNano())
			random := rand.Intn(10)
			fmt.Printf("暂停%d秒\n", random)
			time.Sleep(time.Duration(random) * time.Second)
			err = p.ReturnObject(ctx, conn) //池化对象返还给对象池
			if err != nil {
				panic(err)
			}
			wg.Done()
		}(&wg)
	}
	wg.Wait()
}

func main() {
	ExampleCustomfactory()
}
