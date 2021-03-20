package conduit

import (
	"fmt"
	"github.com/jarcoal/httpmock"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"testing"
)

func setupMain(){
	// Find home directory.
	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
	}
	viper.AddConfigPath(home)
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("~")
	viper.SetConfigName("ConduitClient.toml")
	pflag.String("CONDUIT_SERVER","", "This is the CONDUIT Server to use.")
	pflag.String("CONDUIT_TOKEN", "", "This is the CONDUIT Token to use.")
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
	viper.AutomaticEnv()
	err = viper.ReadInConfig()
	if err != nil {
		fmt.Println("No config file found in current directory or home directory (ConduitClient.toml). Will use command-line args and envvars.")
	}
}
func teardownMain(){
}

func TestMain(m *testing.M){
	setupMain()
	code := m.Run()
	teardownMain()
	os.Exit(code)
}

func TestNewClient(t *testing.T) {
	expected := "blah"
	c := NewClient(expected, expected)
	if c.ConduitToken != expected {
		t.Errorf("Actual: \n%s\n=====\nExpected:\n%s",c.ConduitToken, expected)
	}
}
func TestGetDatabases(t *testing.T) {
	testResponse := `{
  "databases": [
    "file_blob",
    "redshift_redshift",
    "es_elasticsearch",
    "oracle_oracle_localhost"
  ]
}`
	httpmock.Activate()
	url := fmt.Sprintf("https://%v/query/metadata/databases", viper.GetString("CONDUIT_SERVER"))
	httpmock.RegisterResponder("GET", url,
		httpmock.NewStringResponder(200, testResponse))
	c := NewClient(viper.GetString("CONDUIT_SERVER"),
		viper.GetString("CONDUIT_TOKEN"))
	dbs := c.GetDatabases()
	httpmock.DeactivateAndReset()
	if len(dbs.Databases) != 4 {
		t.Errorf("Actual: \n%v\n=====\nExpected:\n%v",len(dbs.Databases), 4)
	}
}
func TestGetTables(t *testing.T){
	testResponse := `{"tables":[{"table":"TransStats___vw_airport_parsed","database":"sql_synapse_flights","schema":"sql_synapse_flights","tableType":"TABLE"},{"table":"TransStats___dimCarriers","database":"sql_synapse_flights","schema":"sql_synapse_flights","tableType":"TABLE"},{"table":"TransStats___dimCalendar","database":"sql_synapse_flights","schema":"sql_synapse_flights","tableType":"TABLE"},{"table":"TransStats___dimAirportsGeoCoded","database":"sql_synapse_flights","schema":"sql_synapse_flights","tableType":"TABLE"},{"table":"TransStats___dimAirports","database":"sql_synapse_flights","schema":"sql_synapse_flights","tableType":"TABLE"},{"table":"TransStats___Flights_All","database":"sql_synapse_flights","schema":"sql_synapse_flights","tableType":"TABLE"},{"table":"TransStats___Flights","database":"sql_synapse_flights","schema":"sql_synapse_flights","tableType":"TABLE"},{"table":"TransStats___Flight_Hold","database":"sql_synapse_flights","schema":"sql_synapse_flights","tableType":"TABLE"}]}`
	httpmock.Activate()
	url := fmt.Sprintf("https://%v/query/metadata/databases/mydatabase/tables", viper.GetString("CONDUIT_SERVER"))
	httpmock.RegisterResponder("GET", url,
		httpmock.NewStringResponder(200, testResponse))
	c := NewClient(viper.GetString("CONDUIT_SERVER"),
		viper.GetString("CONDUIT_TOKEN"))
	tables := c.GetTables("mydatabase")
	httpmock.DeactivateAndReset()
	if len(tables.Tables) != 8 {
		t.Errorf("Actual: \n%v\n=====\nExpected:\n%v",len(tables.Tables), 8)
	}
}
func TestGetTableSchema(t *testing.T){
	testResponse := `{"columns":[{"name":"airport_name","colType":"nvarchar","lengthOpt":null,"scaleOpt":null,"sqlType":1111},{"name":"city","colType":"nvarchar","lengthOpt":null,"scaleOpt":null,"sqlType":1111},{"name":"code","colType":"int","lengthOpt":null,"scaleOpt":null,"sqlType":4},{"name":"description","colType":"nvarchar","lengthOpt":null,"scaleOpt":null,"sqlType":1111},{"name":"state","colType":"nvarchar","lengthOpt":null,"scaleOpt":null,"sqlType":1111}]}`
	httpmock.Activate()
	url := fmt.Sprintf("https://%v/query/metadata/databases/mydatabase/tables/mytable/schema", viper.GetString("CONDUIT_SERVER"))
	httpmock.RegisterResponder("GET", url,
		httpmock.NewStringResponder(200, testResponse))
	c := NewClient(viper.GetString("CONDUIT_SERVER"),
		viper.GetString("CONDUIT_TOKEN"))
	tableSchema := c.GetTableSchema("mydatabase", "mytable")
	httpmock.DeactivateAndReset()
	if len(tableSchema.Columns) != 5 {
		t.Errorf("Actual: \n%v\n=====\nExpected:\n%v",len(tableSchema.Columns), 5)
	}
}
func TestQueryResultMarshal(t *testing.T){
	jsonTest := `{"queryId":"7bba5aec-2641-420e-be82-87015dcb0d7d","status":"Finished","message":null,"data":{"columns":["PassengerId","Survived","Pclass","Name","Sex","Age","SibSp","Parch","Ticket","Fare","Cabin","Embarked"],"rows":[{"PassengerId":1,"Name":"Braund, Mr. Owen Harris","Ticket":"A/5 21171","Pclass":3,"Parch":0,"Embarked":"S","Age":22,"Cabin":"","Fare":7.25,"SibSp":1,"Survived":0,"Sex":"male"},{"PassengerId":2,"Name":"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","Ticket":"PC 17599","Pclass":1,"Parch":0,"Embarked":"C","Age":38,"Cabin":"C85","Fare":71.2833,"SibSp":1,"Survived":1,"Sex":"female"},{"PassengerId":3,"Name":"Heikkinen, Miss. Laina","Ticket":"STON/O2. 3101282","Pclass":3,"Parch":0,"Embarked":"S","Age":26,"Cabin":"","Fare":7.925,"SibSp":0,"Survived":1,"Sex":"female"},{"PassengerId":4,"Name":"Futrelle, Mrs. Jacques Heath (Lily May Peel)","Ticket":"113803","Pclass":1,"Parch":0,"Embarked":"S","Age":35,"Cabin":"C123","Fare":53.1,"SibSp":1,"Survived":1,"Sex":"female"},{"PassengerId":5,"Name":"Allen, Mr. William Henry","Ticket":"373450","Pclass":3,"Parch":0,"Embarked":"S","Age":35,"Cabin":"","Fare":8.05,"SibSp":0,"Survived":0,"Sex":"male"},{"PassengerId":6,"Name":"Moran, Mr. James","Ticket":"330877","Pclass":3,"Parch":0,"Embarked":"Q","Age":60,"Cabin":"","Fare":8.4583,"SibSp":0,"Survived":0,"Sex":"male"},{"PassengerId":7,"Name":"McCarthy, Mr. Timothy J","Ticket":"17463","Pclass":1,"Parch":0,"Embarked":"S","Age":54,"Cabin":"E46","Fare":51.8625,"SibSp":0,"Survived":0,"Sex":"male"},{"PassengerId":8,"Name":"Palsson, Master. Gosta Leonard","Ticket":"349909","Pclass":3,"Parch":1,"Embarked":"S","Age":2,"Cabin":"","Fare":21.075,"SibSp":3,"Survived":0,"Sex":"male"},{"PassengerId":9,"Name":"Johnson, Mrs. Oscar W (Elisabeth Vilhelmina Berg)","Ticket":"347742","Pclass":3,"Parch":2,"Embarked":"S","Age":27,"Cabin":"","Fare":11.1333,"SibSp":0,"Survived":1,"Sex":"female"},{"PassengerId":10,"Name":"Nasser, Mrs. Nicholas (Adele Achem)","Ticket":"237736","Pclass":2,"Parch":0,"Embarked":"C","Age":14,"Cabin":"","Fare":30.0708,"SibSp":1,"Survived":1,"Sex":"female"}],"hasNext":true,"hasPrevious":false}}`
	qrs := UnmarshalJsonToQueryResult(jsonTest)
	if len(qrs.ParsedRows) != 10 {
		t.Errorf("Don't have 10 rows unmarshaled. %v", qrs.QueryId)
	}
	if len(qrs.ParsedColumns) != 12 {
		t.Errorf("Don't have 12 columns unmarshaled. %v columns", len(qrs.ParsedColumns))
	}
}