package main

import (
	"database/sql"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	_ "github.com/snowflakedb/gosnowflake"
)

type Call struct {
	CallId            string `json:"callId"`
	StartTime         string `json:"startTime"`
	EndTime           string `json:"endTime"`
	AnswerTime        string `json:"answerTime"`
	Duration          int    `json:"duration"`
	CallingNumber     string `json:"callingNumber"`
	CallingNumberType string `json:"callingNumberType"`
	CalledNumber      string `json:"calledNumber"`
	CalledNumberType  string `json:"calledNumberType"`
	CallDirection     string `json:"callDirection"`
	PostDialDelay     int    `json:"postDialDelay"`
	CallType          string `json:"callType"`
	CallResult        string `json:"callResult"`
	SipResponseCode   string `json:"sipResponseCode"`
}

// type dbString struct{
// 	DBConfig string 'mapstructure: "DBConfig"'
// 	DBSource string 'mapstructure: "DBSource"'
// }
type DBHandler struct {
	db     *sql.DB
	result Call
}

func (this *DBHandler) getCall(c *gin.Context) {
	callId := c.Params.ByName("callId")
	accountId := c.Params.ByName("accoundId")
	row := this.db.QueryRow("SELECT * FROM calls WHERE %s=? AND %s=?",
		callId,
		accountId,
	)

	///
	var retCallId,
		retEndTime,
		retAnswerTime,
		retStartTime,
		retCallingNumber,
		retCallingNumberType,
		retCalledNumber,
		retCalledNumberType,
		retCallDirection,
		retCallType,
		retCallResult,
		retSipResponseCode string
	var retDuration,
		retPostDialDelay uint
	if err := row.Scan(
		&retCallId,
		&retEndTime,
		&retAnswerTime,
		&retDuration,
		&retStartTime,
		&retCallingNumber,
		&retCallingNumberType,
		&retCalledNumber,
		&retCalledNumberType,
		&retCallDirection,
		&retPostDialDelay,
		&retCallType,
		&retCallResult,
		&retSipResponseCode); err != nil {
		return
	}

	c.IndentedJSON(http.StatusAccepted, retCallType)
	return
}

func main() {
	connStr := "mbronson:Gearmonkey1!@lv67112.us-east-2.aws/call_search/public"
	db, err := sql.Open("snowflake", connStr)
	if err != nil {
		log.Fatal(err)
	}
	Handler := new(DBHandler)
	Handler.db = db
	router := gin.Default()
	//router.GET("/calls", Handler.getCalls)
	router.GET("/accounts/:accountId/calls/:callId", Handler.getCall)
	router.Run(":8085")
	//https://insights.bandwidth.com/api/v1.beta/voice
}
