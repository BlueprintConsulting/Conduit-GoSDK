package conduit

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"
)
type DatabasesStruct struct {
	Databases []string
}
func (s DatabasesStruct) Print() {
	for _, v := range s.Databases {
		fmt.Printf("Database: %v\n", v)
	}
}
type TableStruct struct {
	Table string
	Database string
	Schema string
	TableType string
}
type TablesStruct struct {
	Tables []TableStruct
}
func (s TablesStruct) Print() {
	for _, v := range s.Tables {
		fmt.Printf("Table: %v, from db: %v, is a part of schema: %v, and is of type: %v\n",
			v.Table, v.Database, v.Schema, v.TableType)
	}
}
type ColumnStruct struct {
	Name string
	ColType string
	LengthOpt string
	ScaleOpt string
	SqlType int
}
func (c ColumnStruct) Print() {
	fmt.Printf("Column name: %v, colType: %v, lengthOpt: %v, scaleOpt: %v, sqlType: %v\n",
		c.Name, c.ColType, c.LengthOpt, c.ScaleOpt, c.SqlType)
}
type TableSchemaStruct struct {
	Database string
	Table string
	Columns []ColumnStruct
}
func (c TableSchemaStruct) Print() {
	for _, v := range c.Columns {
		v.Print()
	}
}
type ConduitClient struct {
	ConduitServer string
	ConduitToken string
	Query QueryStruct
}

type QueryResultStruct struct {
	QueryId string `json:"queryId"`
	Status string `json:"status"`
	Message string `json:"message"`
	RawData struct {
		HasNext bool `json:"hasNext"`
		HasPrevious bool `json:"hasPrevious"`
		Columns *json.RawMessage `json:"columns"`
		Rows *json.RawMessage `json:"rows"`
	} `json:"data"`
	ParsedColumns []string
	ParsedRows []map[string]interface{}
}
func UnmarshalJsonToQueryResult(payload string) QueryResultStruct {
	qrs := QueryResultStruct{}
	json.Unmarshal([]byte(payload), &qrs)
	if qrs.RawData.Columns != nil {
		//column processing
		json.Unmarshal(*qrs.RawData.Columns, &qrs.ParsedColumns)
	}
	if qrs.RawData.Columns != nil {
		//row processing
		var rows []json.RawMessage
		json.Unmarshal(*qrs.RawData.Rows, &rows)
		for _, value := range rows {
			var objmap map[string]interface{}
			json.Unmarshal(value, &objmap)
			qrs.ParsedRows = append(qrs.ParsedRows, objmap)
		}
	}
	return qrs
}
type QueryStruct struct {
	SQLString string
	WindowSize int
	Timeout int
	Offset int
	StartTime time.Time
	ActiveQueryId string
	ActiveQueryStatus string
	QueryResults []QueryResultStruct
}
func NewQuery(sqlString string, windowSize, timeout int) QueryStruct {
	MaxWindowSize := 1000
	q := QueryStruct{
		SQLString:     sqlString,
		Timeout:       timeout,
		Offset:        0,
	}
	if windowSize < MaxWindowSize {
		q.WindowSize = windowSize
	} else {
		q.WindowSize = MaxWindowSize
	}
	if timeout == 0 {
		q.Timeout = 30
	}
	return q
}
func (c *ConduitClient) TimedOut() bool {
	if c.Query.StartTime.IsZero() {
		c.Query.StartTime = time.Now()
		return false
	}
	t := time.Now()
	elapsed := t.Sub(c.Query.StartTime)
	if int(elapsed.Seconds()) >= c.Query.Timeout {
		fmt.Print("Timed out...")
		return true
	}
	return false
}
func (c *ConduitClient) CancelQuery() bool {
	if c.Query.ActiveQueryId == "" ||
		c.Query.ActiveQueryStatus == "Finished"{
		log.Printf("There isn't any Active Query to attempt to cancel...")
		return false
	}
	log.Printf("Canceling QueryId %v....", c.Query.ActiveQueryId)
	type CancelStruct struct {
		IsCancelled bool `json:"isCancelled"`
	}
	cancelled := new(CancelStruct)

	err := c.GetOnTheWire(fmt.Sprintf("/cancel?queryId=%v", c.Query.ActiveQueryId), cancelled)
	fmt.Println(cancelled)
	if err != nil {
		log.Fatalf("Error on the wire: %v", err.Error())
	}
	if !cancelled.IsCancelled {
		//time.Sleep(2 * time.Second)
		//c.CancelQuery()	//recursion fun...may need to have max attempts
		return false
	} else {
		log.Printf("QueryId %v successfully canceled.", c.Query.ActiveQueryId)
		return true
	}
}
func (c *ConduitClient) Execute() error {
	if c.TimedOut() {
		c.CancelQuery()
		return nil
	}
	httpClient := &http.Client{}
	formedUrl := fmt.Sprintf("https://%v/query/execute", c.ConduitServer)
	reqBody, err := json.Marshal(map[string]interface{}{
		"queryId": nil,
		"query": c.Query.SQLString,
		"offset": c.Query.Offset,
		"limit": c.Query.WindowSize,
	})
	if err != nil {
		log.Printf("Could not marshal body for POSTing query: %v", err.Error())
		return err
	}
	req, err := http.NewRequest("POST", formedUrl, bytes.NewBuffer(reqBody))
	if err != nil {
		log.Printf("Error forming URL: %s", err.Error() )
		return err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.ConduitToken))
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Error doing request: %s", err.Error())
		return err
	}
	defer resp.Body.Close()

	err = c.ProcessQueryResult(resp)
	if err != nil {
		return err
	}
	return nil
}
func (c *ConduitClient) ProcessQueryResult(response *http.Response) error {
	buf := new(bytes.Buffer)
	buf.ReadFrom(response.Body)
	respString := buf.String()
	qrs := UnmarshalJsonToQueryResult(respString)
	if response.StatusCode != 200 {
		errstring := fmt.Sprintf("Status Code %v returned with message %v", response.StatusCode, qrs.Message)
		log.Printf(errstring)
		return errors.New(errstring)
	}
	c.Query.ActiveQueryId = qrs.QueryId
	c.Query.ActiveQueryStatus = qrs.Status
	if qrs.Status == "Finished" {
		c.Query.QueryResults = append(c.Query.QueryResults, qrs)
		if qrs.RawData.HasNext {
			log.Printf("Query is finished, but has more, so paging...")
			c.Query.Offset = c.Query.Offset + c.Query.WindowSize
			c.Query.Print()
			c.Execute()
		}
	} else if qrs.Status == "Running" {
		time.Sleep(2 * time.Second)
		log.Printf("Query is Running, need to poll for completion...")
		c.CheckQuery()
	} else {
		return errors.New(fmt.Sprintf("Query isn't running or finished. %v", c.Query))
	}
	return nil

}
func (c *ConduitClient) CheckQuery() error {
	if c.TimedOut() {
		c.CancelQuery()
		return nil
	}
	url := fmt.Sprintf("https://%v/query/execute/%v/result", c.ConduitServer, c.Query.ActiveQueryId)
	log.Printf(fmt.Sprintf("Getting URL: %v", url))
	httpClient := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("Error forming URL: %s", err.Error() )
		return err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.ConduitToken))
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Error doing request: %s", err.Error())
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		errstring := fmt.Sprintf("Status Code %v returned", resp.StatusCode)
		log.Printf(errstring)
		return errors.New(errstring)
	}
	err = c.ProcessQueryResult(resp)
	if err != nil {
		return err
	}
	return nil

}
func (q *QueryStruct) Print() {
	log.Printf("Query object is using offset %v, windowsize %v, with timeout %v, start time: %v",
		q.Offset, q.WindowSize, q.Timeout, q.StartTime)
}
func NewClient(conduitServer, conduitToken string) *ConduitClient {
	if len(conduitServer) == 0 || len(conduitToken) == 0 {
		log.Fatal("You need to set CONDUIT_SERVER and CONDUIT_TOKEN somewhere")
	}
	return &ConduitClient{
		ConduitServer: conduitServer,
		ConduitToken: conduitToken,
	}
}
func (c *ConduitClient) Print() {
	log.Printf("Conduit Client uses server: %v, with Token: <redacted>", c.ConduitServer)
}
func (c *ConduitClient) GetOnTheWire(endpoint string, target interface{}) (err error){
	formedUrl := fmt.Sprintf("https://%s/query%s", c.ConduitServer, endpoint)
	httpClient := &http.Client{}
	req, err := http.NewRequest("GET", formedUrl, nil)
	if err != nil {
		log.Printf("Error forming URL: %s", err.Error() )
		return err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.ConduitToken))
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Error doing request: %s", err.Error())
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		errstring := fmt.Sprintf("Status Code %v returned", resp.StatusCode)
		log.Printf(errstring)
		return errors.New(errstring)
	}

	//buf := new(bytes.Buffer)
	//buf.ReadFrom(resp.Body)
	//respString := buf.String()
	//fmt.Println(respString)
	json.NewDecoder(resp.Body).Decode(target)
	return nil
}
func (c *ConduitClient) GetDatabases() *DatabasesStruct {
	curlstring := "curl -X GET \"https://$CONDUIT_SERVER/query/metadata/databases\" -H  \"accept: application/json\" -H \"Authorization: Bearer $CONDUIT_TOKEN\""
	databases := new(DatabasesStruct)
	err := c.GetOnTheWire("/metadata/databases", databases)
	if err != nil {
		log.Fatalf("Error calling GetOnTheWire... equivalent CURL: %s", curlstring)
	}
	return databases
}
func (c *ConduitClient) GetTables(database string) *TablesStruct {
	curlstring := fmt.Sprintf("curl -X GET \"https://$CONDUIT_SERVER/query/metadata/databases/%s/tables\" -H  \"accept: application/json\" -H \"Authorization: Bearer $CONDUIT_TOKEN\"", database)
	//log.Print(curlstring)
	tables := new(TablesStruct)
	err := c.GetOnTheWire(fmt.Sprintf("/metadata/databases/%s/tables",database), tables)
	if err != nil {
		log.Fatalf("Error calling GetOnTheWire... equivalent CURL: %s", curlstring)
	}
	return tables
}
func (c *ConduitClient) GetTableSchema(database, table string) *TableSchemaStruct {
	curlstring := fmt.Sprintf("curl -X GET \"https://$CONDUIT_SERVER/query/metadata/databases/%s/tables/%s/schema\" -H  \"accept: application/json\" -H \"Authorization: Bearer $CONDUIT_TOKEN\"", database, table)
	tableSchema := new(TableSchemaStruct)
	tableSchema.Database = database
	tableSchema.Table = table
	err := c.GetOnTheWire(fmt.Sprintf("/metadata/databases/%s/tables/%s/schema", database, table), tableSchema)
	if err != nil {
		log.Fatalf("Error calling GetOnTheWire... equivalent CURL: %s", curlstring)
	}
	return tableSchema
}

func (c *ConduitClient) ExecuteQuery(sqlString string, windowSize, timeout int) error {
	/*
	Several activities occur here:
	1. Query is executed quickly, with no pagination.
	2. Query is _started_, returns with a Running status, to be polled until finished.
	3. Query returns paginated (either in case #1 or #2 above); must slide the window, re-execute query
	4. Timeout occurs during 1, 2, or 3; at which time a cancel is issued.
	*/
	c.Query = NewQuery(sqlString, windowSize, timeout)
	return c.Execute()
}
