package conduit

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"testing"
	"github.com/jarcoal/httpmock"
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

