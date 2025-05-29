package main

import (
	"database/sql"
	"fmt"
	"github.com/emirpasic/gods/lists/arraylist"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

type Config struct {
	Paths       []string            `yaml:"paths"`
	DBName      string              `yaml:"db_name"`
	UrlParams   string              `yaml:"db_url_params"`
	Type        int                 `yaml:"type"`
	ConfigGroup string              `yaml:"group"`
	Databases   map[string]DBConfig `yaml:"databases"`
}

type DBConfig struct {
	DBUser string `yaml:"db_user"`
	DBPass string `yaml:"db_pass"`
	DBPort int    `yaml:"db_port"`
	DBHost string `yaml:"db_host"`
}

const (
	TypeLocal       = 1 // 本地
	TypeRemote      = 2 // 远程
	TypeLocalRemote = 3 // 本地和远程都更新
)

func main() {
	// Read configuration file
	config, err := readConfig("config.yaml")
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	dbs := loadTargetDatabases(config)
	defer func() {
		for _, db := range dbs {
			db.Close()
		}
	}()
	for _, db := range dbs {
		// Traverse paths and process .bytes fileList
		fileList := arraylist.New()
		for _, path := range config.Paths {
			err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if filepath.Ext(path) == ".bytes" {
					fileName, err := processFile(db, path, config)
					if err != nil {
						log.Printf("Error processing file %s: %v", path, err)
					}
					fileList.Add(fileName)
				}
				return nil
			})
			if err != nil {
				log.Printf("Error traversing path %s: %v", path, err)
			}
		}
		// 检查表中是否有表没有在目录中，如果有则删除
		checkAndDelete(db, config, fileList)
	}
}
func loadTargetDatabases(config *Config) map[string]*sql.DB {
	dbs := make(map[string]*sql.DB)

	for key, dbConf := range config.Databases {
		// 判断要不要连这个库
		if (config.Type == TypeLocal && key != "local") ||
			(config.Type == TypeRemote && key != "remote") {
			if config.Type != TypeLocalRemote {
				continue
			}
		}
		// 拼接 DSN
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
			dbConf.DBUser, dbConf.DBPass, dbConf.DBHost, dbConf.DBPort,
			config.DBName, config.UrlParams)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Fatalf("[%s] Error connecting to DB: %v", key, err)
		}
		log.Printf("[%s] 数据库连接成功！", key)
		dbs[key] = db
	}

	return dbs
}
func checkAndDelete(db *sql.DB, config *Config, list *arraylist.List) {

	query := fmt.Sprintf("SELECT name FROM bytes_data WHERE namespace ='%s'", config.ConfigGroup)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	deleteList := arraylist.New()
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}
		if !list.Contains(name) {
			deleteList.Add(name)
		}
	}
	if deleteList.Size() <= 0 {
		return
	}

	placeholders := ""
	for i := 0; i < deleteList.Size(); i++ {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
	}

	// 构建参数
	params := make([]interface{}, deleteList.Size())
	for i := 0; i < deleteList.Size(); i++ {
		get, _ := deleteList.Get(i)
		params[i] = get
	}

	// 拼接 SQL
	query = fmt.Sprintf(`DELETE FROM bytes_data WHERE name IN (%s) AND namespace = ?`, placeholders)

	// 添加 namespace 参数
	params = append(params, config.ConfigGroup)

	// 执行 SQL
	_, err = db.Exec(query, params...)
	if err != nil {
		log.Fatal(err)
	}
	// 打印出删除了那些表
	for i := 0; i < deleteList.Size(); i++ {
		get, _ := deleteList.Get(i)
		log.Printf("删除了表：%s", get)
	}
}

func readConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func processFile(db *sql.DB, path string, config *Config) (string, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}
	fileName := filepath.Base(path)
	log.Printf("%s", fileName)
	configGroup := config.ConfigGroup

	query := `
	INSERT INTO bytes_data (name, namespace, data)
	VALUES (?, ?, ?)
	ON DUPLICATE KEY UPDATE
		data = VALUES(data)
	`
	_, err = db.Exec(query, fileName, configGroup, data)
	if err != nil {
		return "", err
	}
	return fileName, nil
}
