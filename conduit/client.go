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
//type TableStruct struct {
//	Table string
//	Database string
//	Schema string
//	TableType string
//}
//type TablesStruct struct {
//	Tables
//}
//func (s TableStruct) Print() {
//	fmt.Printf("Table: %v, from db: %v, is a part of schema: %v, and is of type: %v",
//		s.Table, s.Database, s.Schema, s.TableType)
//}

type ConduitClient struct {
	ConduitServer string
	ConduitToken string
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
//func (c ConduitClient) GetTables(database string) {
//	curlstring := "curl -X GET \"https://$CONDUIT_SERVER/query/metadata/databases/{database}/tables\" -H  \"accept: application/json\" -H \"Authorization: Token $CONDUIT_TOKEN\""
//	tables := new(TablesStruct)
//	err := c.GetOnTheWire(fmt.Sprintf("/metadata/databases/%s/tables",database), tables)
//}