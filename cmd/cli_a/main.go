package main

import (
	"database/sql"
	"fmt"
	"log"
	"simple-cli-query-db/pkg/common"
	"simple-cli-query-db/pkg/config"
	"simple-cli-query-db/pkg/database"
	"strconv"
	"sync"
	"time"
)

const (
	maxConcurrency = 1
)

func main() {
	// 使用说明 cd到项目根目录下 执行编译  go build -o cmd/cli_a/cli_a cmd/cli_a/main.go 然后运行

	t1 := time.Now().Unix()

	// 加载配置
	cfg, err := config.LoadConfig("configs/config_test.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 连接数据库
	db, err := database.ConnectDB(cfg.DB)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// demo
	// 执行业务逻辑

	// 展示所有品牌库
	databases, err := common.ShowBrandDatabases(db)
	if err != nil {
		log.Fatalf("Failed to show databases: %v", err)
	}

	ch := make(chan string, len(databases))
	for _, dbName := range databases {

		fmt.Println(dbName)
		ch <- dbName
	}
	// 关闭channel，关闭后可以继续读取
	close(ch)

	var wg sync.WaitGroup
	wg.Add(maxConcurrency)

	for i := 0; i < maxConcurrency; i++ {
		go func() {
			defer wg.Done()

			// 业务代码
			for dbName := range ch {
				consumer(db, dbName)
			}
		}()
	}
	wg.Wait()

	fmt.Println("main协程阻塞直到其它协程处理完数据......")
	t2 := time.Now().Unix()
	fmt.Println("main协程退出......程序总耗时", t2-t1, "s")

}

// consumer示例： demo 统计会员数量
func consumer(db *sql.DB, dbName string) error {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s.tbl_members", dbName)

	row := db.QueryRow(sql)
	var count int
	if err := row.Scan(&count); err != nil {
		log.Printf("Failed to scan row: %v\n", err)
	}
	fmt.Println(dbName + ":" + strconv.Itoa(count))
	return nil
}
