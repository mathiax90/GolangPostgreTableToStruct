package main

import (
	"fmt"
	//"log"
	//"github.com/gorilla/mux"
	//"net/http"
	
	//"strconv"
	//"encoding/json"
	
	//"github.com/fxtlabs/date"
	//"context"	
	"os"
	//pgx "github.com/jackc/pgx/v4"	
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/jackc/pgx/stdlib"
	"strings"
	"time"
	
)

func init() {
    // loads values from .env into the system
    if err := godotenv.Load(); err != nil {
        fmt.Print("No .env file found")
    }
}

var db *sqlx.DB

type SchemaCount struct {
	Count int `db:"schema_count"`	
}

type Column struct {
	TableCatalog string `db:"table_catalog"`
	TableSchema string `db:"table_schema"`
	TableName string `db:"table_name"`
	Name string `db:"column_name"`
	Type string `db:"data_type"`
	IsNull string `db:"is_nullable"`
}

func main() {
	fmt.Println("hello")
	start := time.Now()
	fmt.Println(start)
	fmt.Println("start program")
	//get conf
	DATABASE_URL, exists := os.LookupEnv("DATABASE_URL")
	if !exists {
		fmt.Println("Не найдена строка подключения к БД в файле .env")
	}

	//connect to server (pgx connection no sqlx here)
	// conn, err := pgx.Connect(context.Background(), DATABASE_URL)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
	// 	os.Exit(1)
	// }
	// defer conn.Close(context.Background())

	//create db abstraction using pgx driver
	var err error
	db, err = sqlx.Open("pgx", DATABASE_URL )
    if err != nil {
        fmt.Println(err)
    }	
	defer db.Close()
	
	fmt.Println("Connection ok")
	var tableName string

	//get table name from user
	fmt.Print("Введите имя таблицы (с указанием схемы или без неё): ")
	fmt.Scan(&tableName)

	//check empty string
	tableName = strings.Trim(tableName, " .")	
	if (len(tableName)<=0){
		fmt.Println("Указана пустая строка")
		return
	}

	//splitting to dbName, schemaName, tableName

	//tableName = "FLKDB.reestr_export.t_order"
	tableDescr := strings.Split(tableName, ".")
	var schemaName string		
		
	//reading schemaName, tableName and get Columns	
	if len(tableDescr) >= 2 {		
		schemaName = tableDescr[0]
		tableName = tableDescr[1]	

	}
	if len(tableDescr) == 1 {				
		tableName = tableDescr[0]			
	}
	
	columns := []Column{}
	if len(tableDescr) >= 2 {		
		err = db.Select(&columns, `select 
		table_catalog, table_schema, table_name, column_name,data_type,is_nullable
		FROM 
		information_schema.columns
		WHERE 
		table_name = $1 and table_schema = $2;`, tableName, schemaName)		

		if err != nil {
			fmt.Println("Ошибка при получении данных о Таблице")
			fmt.Println(err)
			return
		}		
	}
	if len(tableDescr) == 1 {
		schemaCount := SchemaCount{}

		err = db.Get(&schemaCount, `select count(distinct table_schema) as schema_count		
		FROM information_schema.columns
		WHERE 
		table_name = $1;`, tableName)

		if err != nil {
			fmt.Println("Ошибка БД при проверке количества схем в которых есть заданная таблица.")
			fmt.Println(err)
			return
		}

		if (schemaCount == SchemaCount{}){
			fmt.Println("Ошибка при проверке количества схем в которых есть заданная таблица. Нет строк в результате запроса.")
			return
		}

		if (schemaCount.Count > 1){
			fmt.Println("Таблица существует в разных схемах. Уточните схему.")
			return
		}

		if (schemaCount.Count != 1){
			fmt.Println("Неизвестная ошибка при проверке количества схем в которых есть заданная таблица.")
			return
		}
		
		err = db.Select(&columns, `select 
		table_catalog, table_schema, table_name, column_name,data_type,is_nullable
		FROM 
		information_schema.columns
		WHERE 
		table_name = $1;`, tableName)		

		if err != nil {
			fmt.Println("Ошибка при получении данных о Таблице")
			fmt.Println(err)
			return
		}
	}
	//нашли Columns, генерируем output
	var ColumnAsStructField string
	for _, v := range columns {
		//fmt.Println(v)
		ColumnAsStructField = getGoNameFromDbName(v.Name) + " "
		ColumnAsStructField += getGoTypeFromDbType(v.Type, v.IsNull) + " "
		ColumnAsStructField += " `db:\"" + v.Name + "\"`"
		fmt.Println(ColumnAsStructField)
	}
	fmt.Println("Program End")
}

func getGoTypeFromDbType(dbType string, isNullableString string) string{
	
	if isNullableString == "YES" {
		switch dbType {
		case "character varying":
			return "sql.NullString"
		case "date":
			return "sql.NullTime"
		case "integer", "bigint":
			return "sql.NullInt64"
		case "boolean":
			return "sql.NullBool"
		case "real":
			return "sql.NullFloat64"
		default:
			return "can't define goType of Nullable " + dbType
		}
	}
	if isNullableString == "NO" {
		switch dbType {
		case "character varying":
			return "String"
		case "date":
			return "Time"
		case "integer", "bigint":
			return "int64"
		case "boolean":
			return "bool"
		case "real":
			return "float64"
		default:
			return "can't define goType of " + dbType
		}
	} else {
		return "Ошибка при определении isNullable " + isNullableString
	}
	

}

func getGoNameFromDbName(columnName string) string {
	stringParts:= strings.Split(columnName,"_")
	var goName string
	for _, v := range stringParts {
		firstLetter := v[0:1]
		otherLetters := v[1:len(v)]
		goName = goName + strings.ToUpper(firstLetter) + otherLetters
	}
	return goName
}




