package conduit

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
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
	//column processing
	json.Unmarshal(*qrs.RawData.Columns, &qrs.ParsedColumns)
	//row processing
	var rows []json.RawMessage
	json.Unmarshal(*qrs.RawData.Rows, &rows)
	for _, value := range rows {
		var objmap map[string]interface{}
		json.Unmarshal(value, &objmap)
		qrs.ParsedRows = append(qrs.ParsedRows, objmap)
	}
	return qrs
}
type QueryStruct struct {
	Server string
	Token string
	SQLString string
	WindowSize int
	Timeout int
	Offset int
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
	json.NewDecoder(resp.Body).Decode(target)
	return nil
}
func (c ConduitClient) GetDatabases() *DatabasesStruct {
	curlstring := "curl -X GET \"https://$CONDUIT_SERVER/query/metadata/databases\" -H  \"accept: application/json\" -H \"Authorization: Bearer $CONDUIT_TOKEN\""
	databases := new(DatabasesStruct)
	err := c.GetOnTheWire("/metadata/databases", databases)
	if err != nil {
		log.Fatalf("Error calling GetOnTheWire... equivalent CURL: %s", curlstring)
	}
	return databases
}
func (c ConduitClient) GetTables(database string) *TablesStruct {
	curlstring := fmt.Sprintf("curl -X GET \"https://$CONDUIT_SERVER/query/metadata/databases/%s/tables\" -H  \"accept: application/json\" -H \"Authorization: Bearer $CONDUIT_TOKEN\"", database)
	//log.Print(curlstring)
	tables := new(TablesStruct)
	err := c.GetOnTheWire(fmt.Sprintf("/metadata/databases/%s/tables",database), tables)
	if err != nil {
		log.Fatalf("Error calling GetOnTheWire... equivalent CURL: %s", curlstring)
	}
	return tables
}
func (c ConduitClient) GetTableSchema(database, table string) *TableSchemaStruct {
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

