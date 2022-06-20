package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/gin-gonic/gin"
	_ "github.com/snowflakedb/gosnowflake"
)

type Call struct {
	callId            string `gorm:"primaryKey" json:"callId"`
	startTime         string `json:"startTime"`
	endtime           string `json:"endTime"`
	answerTime        string `json:"answerTime"`
	duration          int    `json:"duration"`
	callingNumber     string `json:"callingNumber"`
	callingNumberType string `json:"callingNumberType"`
	calledNumber      string `json:"calledNumber"`
	calledNumberType  string `json:"calledNumberType"`
	callDirection     string `json:"callDirection"`
	postDialDelay     string `json:"postDialDelay"`
	callType          string `json:"callType"`
	callResult        string `json:"callResult"`
	sipResponseCode   string `json:"sipResponseCode"`
}

// type dbString struct{
// 	DBConfig string 'mapstructure: "DBConfig"'
// 	DBSource string 'mapstructure: "DBSource"'
// }
type DBHandler struct {
	db *sql.DB
}

func (this *DBHandler) getCall(c *gin.Context) {
	callId := c.Params.ByName("callId")
	accountId := c.Params.ByName("accoundId")
	row := this.db.QueryRow(
		fmt.Sprintf(
			"SELECT * FROM calls WHERE %s=? AND %s=?",
			callId,
			startTime,
			endtime,
			answerTime,
			duration,
			callingNumber,
			callingNumberType,
			calledNumber,
			calledNumberType,
			callDirection,
			postDialDelay,
			callType,
			callResult,
			sipResponseCode,
		),
		callId,
		accountId,
	)
	var retFirstName, retLastName string
	var retAge uint
	if err := row.Scan(
		&retFirstName,
		&retLastName,
		&retAge); err != nil {
		return err
	}
	result.FirstName = retFirstName
	result.LastName = retLastName
	result.Age = retAge
	return nil
}

func main() {
	connStr = ""
	db, err := sql.Open("snowflake", connStr)
	if err != nil {
		log.Fatal(err)
	}
	Handler := new(DBHandler)
	Handler.db = db
	router := gin.Default()
	router.GET("/calls", Handler.getCalls)
	router.GET("/accounts/:accountId/calls/:callId", Handler.getCall)
	router.Run("https://insights.bandwidth.com/api/v1.beta/voice")
}
