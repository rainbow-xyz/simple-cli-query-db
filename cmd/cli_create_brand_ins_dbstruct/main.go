package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"regexp"
	"simple-cli-query-db/pkg/config"
	"simple-cli-query-db/pkg/database"
	"strings"
	"sync"
	"time"
)

func main() {
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
	databases, err := showBrandInsCommonDatabases(db)
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
	wg.Add(*concurrency)

	for i := 0; i < *concurrency; i++ {
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

func consumer(db *sql.DB, dbName string) error {

	_, err := db.Exec("USE " + dbName)
	if err != nil {
		log.Printf("Failed to switch to database: %v", err)
		return err
	}

	sql := fmt.Sprintf("SHOW TABLES WHERE Tables_in_%s NOT LIKE '%%_2' AND Tables_in_%s NOT LIKE 'dbh_%%'", dbName, dbName)

	rows, err := db.Query(sql)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			log.Printf("Failed to get create table statement: %v\n", err)
			continue
		}

		createDbStmt := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET utf8mb4;", dbName)
		fmt.Println(createDbStmt)

		query := fmt.Sprintf("SHOW CREATE TABLE %s.%s", dbName, table)

		var createTableStmt string
		err = db.QueryRow(query).Scan(&table, &createTableStmt)
		if err != nil {
			log.Printf("Failed to get create table statement: %v\n", err)
			continue
		}

		createTableStmt = strings.Replace(createTableStmt, "CREATE TABLE ", fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.", dbName), 1)

		re := regexp.MustCompile(`AUTO_INCREMENT=\d+`)
		createTableStmt = re.ReplaceAllString(createTableStmt, "") + ";"
		fmt.Println(createTableStmt)
	}

	return nil
}

func showBrandInsCommonDatabases(db *sql.DB) ([]string, error) {
	var databases []string
	rows, err := db.Query("SHOW DATABASES WHERE `Database` LIKE 'ky_%' AND `Database` NOT LIKE 'ky_hygl_%'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, err
		}

		databases = append(databases, dbName)
	}
	return databases, nil
}
