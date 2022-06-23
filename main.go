package main

import (
	"database/sql"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	_ "github.com/snowflakedb/gosnowflake"
)

type Call struct {
	CallId            sql.NullString `json:"callId"`
	StartTime         sql.NullString `json:"startTime"`
	EndTime           sql.NullString `json:"endTime"`
	AnswerTime        sql.NullString `json:"answerTime"`
	Duration          sql.NullString `json:"duration"`
	CallingNumber     sql.NullString `json:"callingNumber"`
	CallingNumberType sql.NullString `json:"callingNumberType"`
	CalledNumber      sql.NullString `json:"calledNumber"`
	CalledNumberType  sql.NullString `json:"calledNumberType"`
	CallDirection     sql.NullString `json:"callDirection"`
	PostDialDelay     sql.NullString `json:"postDialDelay"`
	CallType          sql.NullString `json:"callType"`
	CallResult        sql.NullString `json:"callResult"`
	SipResponseCode   sql.NullString `json:"sipResponseCode"`
}
type Call_Val struct {
	CallId            string `json:"callId"`
	StartTime         string `json:"startTime"`
	EndTime           string `json:"endTime"`
	AnswerTime        string `json:"answerTime"`
	Duration          string `json:"duration"`
	CallingNumber     string `json:"callingNumber"`
	CallingNumberType string `json:"callingNumberType"`
	CalledNumber      string `json:"calledNumber"`
	CalledNumberType  string `json:"calledNumberType"`
	CallDirection     string `json:"callDirection"`
	PostDialDelay     string `json:"postDialDelay"`
	CallType          string `json:"callType"`
	CallResult        string `json:"callResult"`
	SipResponseCode   string `json:"sipResponseCode"`
}

// type dbString struct{
// 	DBConfig string 'mapstructure: "DBConfig"'
// 	DBSource string 'mapstructure: "DBSource"'
// }
type DBHandler struct {
	db *sql.DB
}

func NewNullString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}

func (this *DBHandler) getCalls(c *gin.Context) {

	accountId := c.Query("accountId")
	callId := c.Query("callId")
	startTime := c.Query("startTime")
	endTime := c.Query("endTime")
	answerTime := c.Query("answerTime")
	duration := c.Query("duration")
	callingNumber := c.Query("callingNumber")
	callingNumberType := c.Query("callingNumberType")
	calledNumber := c.Query("calledNumber")
	calledNumberType := c.Query("calledNumberType")
	callDirection := c.Query("callDirection")
	postDialDelay := c.Query("postDialDelay")
	callType := c.Query("callType")
	callResult := c.Query("callResult")
	sipResponseCode := c.Query("sipResponseCode")
	rows, err := this.db.Query("SELECT * FROM call_logs WHERE (accountid=? or ? is null) and (callid=? or ? is null) and (starttime=? or ? is null) and (endtime=? or ? is null) and (answertime=? or ? is null) and (duration=? or ? is null) and (callingnumber=? or ? is null) and (callingnumbertype=? or ? is null) and (callednumber=? or ? is null) and (callednumbertype=? or ? is null) and (calldirection=? or ? is null) and (postdialdelay=? or ? is null) and (calltype=? or ? is null) and (callresult=? or ? is null) and (sipresponsecode=? or ? is null)",
		NewNullString(accountId),
		NewNullString(accountId),
		NewNullString(callId),
		NewNullString(callId),
		NewNullString(startTime),
		NewNullString(startTime),
		NewNullString(endTime),
		NewNullString(endTime),
		NewNullString(answerTime),
		NewNullString(answerTime),
		NewNullString(duration),
		NewNullString(duration),
		NewNullString(callingNumber),
		NewNullString(callingNumber),
		NewNullString(callingNumberType),
		NewNullString(callingNumberType),
		NewNullString(calledNumber),
		NewNullString(calledNumber),
		NewNullString(calledNumberType),
		NewNullString(calledNumberType),
		NewNullString(callDirection),
		NewNullString(callDirection),
		NewNullString(postDialDelay),
		NewNullString(postDialDelay),
		NewNullString(callType),
		NewNullString(callType),
		NewNullString(callResult),
		NewNullString(callResult),
		NewNullString(sipResponseCode),
		NewNullString(sipResponseCode))
	if err != nil {
		c.String(http.StatusBadRequest, "Bad Request")

	}

	var allCalls []Call_Val
	for rows.Next() {
		var call Call
		var AccountId sql.NullString
		if err := rows.Scan(
			&call.CallId,
			&call.StartTime,
			&call.EndTime,
			&call.AnswerTime,
			&call.Duration,
			&call.CallingNumber,
			&call.CallingNumberType,
			&call.CalledNumber,
			&call.CalledNumberType,
			&call.CallDirection,
			&call.PostDialDelay,
			&call.CallType,
			&call.CallResult,
			&call.SipResponseCode,
			&AccountId); err != nil {
			c.String(http.StatusBadRequest, "Badder Request")

		}
		call_val := Call_Val{
			call.CallId.String,
			call.StartTime.String,
			call.EndTime.String,
			call.AnswerTime.String,
			call.Duration.String,
			call.CallingNumber.String,
			call.CallingNumberType.String,
			call.CalledNumber.String,
			call.CalledNumberType.String,
			call.CallDirection.String,
			call.PostDialDelay.String,
			call.CallType.String,
			call.CallResult.String,
			call.SipResponseCode.String,
		}
		allCalls = append(allCalls, call_val)
	}
	c.IndentedJSON(http.StatusAccepted, allCalls)

}

func (this *DBHandler) getCall(c *gin.Context) {
	callId := c.Params.ByName("callId")
	accountId := c.Params.ByName("accountId")
	row := this.db.QueryRow("SELECT * FROM call_logs WHERE callid=? AND accountid=?",
		callId,
		accountId,
	)
	var AccountId sql.NullString
	var call Call
	if err := row.Scan(
		&call.CallId,
		&call.StartTime,
		&call.EndTime,
		&call.AnswerTime,
		&call.Duration,
		&call.CallingNumber,
		&call.CallingNumberType,
		&call.CalledNumber,
		&call.CalledNumberType,
		&call.CallDirection,
		&call.PostDialDelay,
		&call.CallType,
		&call.CallResult,
		&call.SipResponseCode,
		&AccountId); err != nil {
		c.String(http.StatusBadRequest, "Bad Request")
	}
	call_val := Call_Val{
		call.CallId.String,
		call.StartTime.String,
		call.EndTime.String,
		call.AnswerTime.String,
		call.Duration.String,
		call.CallingNumber.String,
		call.CallingNumberType.String,
		call.CalledNumber.String,
		call.CalledNumberType.String,
		call.CallDirection.String,
		call.PostDialDelay.String,
		call.CallType.String,
		call.CallResult.String,
		call.SipResponseCode.String,
	}
	c.IndentedJSON(http.StatusAccepted, call_val)
}

func main() {
	connStr := "milesbronson:Gearmonkey1!@lv67112.us-east-2.aws/call_search/public"
	db, err := sql.Open("snowflake", connStr)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		log.Fatalf("unable to reach database: %v", err)
	}

	Handler := new(DBHandler)
	Handler.db = db
	router := gin.Default()
	router.GET("/calls", Handler.getCalls)
	router.GET("/accounts/:accountId/calls/:callId", Handler.getCall)
	router.Run(":8085")
	//https://insights.bandwidth.com/api/v1.beta/voice
}
