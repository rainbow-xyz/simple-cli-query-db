package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"simple-cli-query-db/pkg/common"
	"simple-cli-query-db/pkg/config"
	"simple-cli-query-db/pkg/database"
	"strconv"
	"sync"
	"time"
)

func main() {
	// 使用说明 cd到项目根目录下 执行编译  go build -o cmd/cli_b/cli_b cmd/cli_b/main.go 然后运行 通过-c可以指定读取时的并发数

	// 从命令行输入并发数
	concurrency := flag.Int("c", 1, "concurrency")
	flag.Parse()

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

	dbCh := make(chan string, len(databases))
	for _, dbName := range databases {

		//fmt.Println(dbName)
		dbCh <- dbName
	}
	// 关闭channel，关闭后可以继续读取
	close(dbCh)

	syncCh := make(chan bool)
	dataCh := make(chan string, 50000)

	var wg sync.WaitGroup
	wg.Add(*concurrency)

	for i := 0; i < *concurrency; i++ {
		go func(i int) {
			defer wg.Done()

			for {
				select {
				case dbName, ok := <-dbCh:
					if ok {
						consumerA(db, dbName, dataCh)
					} else {
						fmt.Printf("[c%d]: 数据已读取完毕 退出......\n", i)
						return
					}
				}
			}
		}(i)
	}

	go consumerB(syncCh, dataCh)

	wg.Wait()
	close(dataCh)
	fmt.Println("数据写入结束，关闭数据通道，等待读取完成......")
	<-syncCh

	fmt.Println("main协程阻塞直到其它协程处理完数据......")
	t2 := time.Now().Unix()
	fmt.Println("main协程退出......程序总耗时", t2-t1, "s")

}

// consumerA示例： demo 统计会员数量
func consumerA(db *sql.DB, dbName string, dataCh chan<- string) error {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s.tbl_members", dbName)

	row := db.QueryRow(sql)
	var count int
	if err := row.Scan(&count); err != nil {
		log.Printf("Failed to scan row: %v\n", err)
	}
	data := dbName + ":" + strconv.Itoa(count)
	dataCh <- data

	return nil
}

func consumerB(syncCh chan<- bool, dataCh <-chan string) error {
	defer func() {
		syncCh <- true
	}()

	filename := "output.csv"
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)

	for data := range dataCh {
		writer.Write([]string{data})
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}

	return nil
}
