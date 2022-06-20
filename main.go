package main

import (
	"database/sql"
	"log"

	"github.com/gin-gonic/gin"
)

// type dbString struct{
// 	DBConfig string 'mapstructure: "DBConfig"'
// 	DBSource string 'mapstructure: "DBSource"'
// }

func main() {
	connStr = ""
	db, err := sql.Open("snowflake", connStr)
	if err != nil {
		log.Fatal(err)
	}
	config.Connect()
	router := gin.Default()

}
