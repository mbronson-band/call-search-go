package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	_ "github.com/snowflakedb/gosnowflake"
)

type Call struct {
	CallId            sql.NullString `json:"callId"`
	StartTime         sql.NullTime   `json:"startTime"`
	EndTime           sql.NullTime   `json:"endTime"`
	Duration          sql.NullInt64  `json:"duration"`
	CallingNumber     sql.NullString `json:"callingNumber"`
	CallingNumberType sql.NullString `json:"callingNumberType"`
	CalledNumber      sql.NullString `json:"calledNumber"`
	CalledNumberType  sql.NullString `json:"calledNumberType"`
	CallDirection     sql.NullString `json:"callDirection"`
	PostDialDelay     sql.NullInt64  `json:"postDialDelay"`
	CallType          sql.NullString `json:"callType"`
	CallResult        sql.NullString `json:"callResult"`
	SipResponseCode   sql.NullString `json:"sipResponseCode"`
}
type Call_Val struct {
	CallId            string    `json:"callId"`
	StartTime         time.Time `json:"startTime"`
	EndTime           time.Time `json:"endTime"`
	Duration          int64     `json:"duration"`
	CallingNumber     string    `json:"callingNumber"`
	CallingNumberType string    `json:"callingNumberType"`
	CalledNumber      string    `json:"calledNumber"`
	CalledNumberType  string    `json:"calledNumberType"`
	CallDirection     string    `json:"callDirection"`
	PostDialDelay     int64     `json:"postDialDelay"`
	CallType          string    `json:"callType"`
	CallResult        string    `json:"callResult"`
	SipResponseCode   string    `json:"sipResponseCode"`
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
	rows, err := this.db.Query("SELECT unique_record_id, customer_sbc_answer_time, customer_sbc_disconnect_time, customer_sbc_call_durtion_in_milliseconds, calling_number, calling_number_type, called_number, called_number_type, call_direction, customer_sbc_post_dial_delay_in_milliseconds, call_type, call_result, sip_response_code  FROM correlated_cdr_search_v3_vw WHERE (customer_id=? or ? is null) and (unique_record_id=? or ? is null) and (customer_sbc_answer_time=? or ? is null) and (customer_sbc_disconnect_time=? or ? is null) and (customer_sbc_call_duration_in_milliseconds=? or ? is null) and (calling_number=? or ? is null) and (calling_number_type=? or ? is null) and (called_number=? or ? is null) and (called_number_type=? or ? is null) and (call_direction=? or ? is null) and (customer_sbc_post_dial_delay_in_milliseconds=? or ? is null) and (call_type=? or ? is null) and (call_result=? or ? is null) and (sip_response_code=? or ? is null)",
		NewNullString(accountId),
		NewNullString(accountId),
		NewNullString(callId),
		NewNullString(callId),
		startTime,
		startTime,
		endTime,
		endTime,
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
		c.String(http.StatusOK, "No Results")
		log.Fatal(err)
	}

	var allCalls []Call_Val
	for rows.Next() {
		var call Call
		var AccountId sql.NullString
		if err := rows.Scan(
			&call.CallId,
			&call.StartTime,
			&call.EndTime,
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
			log.Fatal(err)
			if sql.ErrNoRows == err {
				c.String(http.StatusOK, "No Results")
			} else {
				c.String(http.StatusBadRequest, "BadRequest")
			}

		}
		call_val := Call_Val{
			call.CallId.String,
			call.StartTime.Time,
			call.EndTime.Time,
			call.Duration.Int64,
			call.CallingNumber.String,
			call.CallingNumberType.String,
			call.CalledNumber.String,
			call.CalledNumberType.String,
			call.CallDirection.String,
			call.PostDialDelay.Int64,
			call.CallType.String,
			call.CallResult.String,
			call.SipResponseCode.String,
		}
		allCalls = append(allCalls, call_val)
	}
	c.IndentedJSON(http.StatusOK, allCalls)

}

func (this *DBHandler) getCall(c *gin.Context) {
	callId := c.Params.ByName("callId")
	accountId := c.Params.ByName("accountId")
	row := this.db.QueryRow("SELECT unique_record_id, customer_sbc_answer_time, customer_sbc_disconnect_time, customer_sbc_call_durtion_in_milliseconds, calling_number, calling_number_type, called_number, called_number_type, call_direction, customer_sbc_post_dial_delay_in_milliseconds, call_type, call_result, sip_response_code FROM correlated_cdr_search_v3_vw WHERE unique_record_id=? AND customer_id=?",
		callId,
		accountId,
	)
	var AccountId sql.NullString
	var call Call
	if err := row.Scan(
		&call.CallId,
		&call.StartTime,
		&call.EndTime,
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
		if sql.ErrNoRows == err {
			c.String(http.StatusOK, "No Results")
		} else {
			c.String(http.StatusBadRequest, "BadRequest")
		}

	}
	call_val := Call_Val{
		call.CallId.String,
		call.StartTime.Time,
		call.EndTime.Time,
		call.Duration.Int64,
		call.CallingNumber.String,
		call.CallingNumberType.String,
		call.CalledNumber.String,
		call.CalledNumberType.String,
		call.CallDirection.String,
		call.PostDialDelay.Int64,
		call.CallType.String,
		call.CallResult.String,
		call.SipResponseCode.String,
	}
	c.IndentedJSON(http.StatusOK, call_val)
}

func main() {
	err := godotenv.Load(".env")

	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	database := os.Getenv("DATABASE")
	connStr := os.Getenv("CONNECTION_STRING")
	db, err := sql.Open(database, connStr)
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
