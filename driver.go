package main

import (
	"fmt"
	conduitclient "github.com/BlueprintConsulting/Conduit-GoSDK/conduit"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"log"
	"math/rand"
	"time"
)

func initConfig() (err error) {
	log.Printf("Starting Conduit Client, getting config")
	// Find home directory.
	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		return err
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
		err = nil
	}
	return err
}

func main() {
	rand.Seed(time.Now().Unix())
	err := initConfig()
	if err == nil {
		log.Printf("Initialized...")
		client := conduitclient.NewClient(
			viper.GetString("CONDUIT_SERVER"),
			viper.GetString("CONDUIT_TOKEN"))
		client.Print()
		//dbs := client.GetDatabases()
		//dbs.Print()
		//tables := client.GetTables("oracle_flights")
		//tables.Print()
		//err = client.ExecuteQuery("SELECT * FROM `oracle_flights`.`PDBADMIN___FLIGHTS` ORDER BY TAIL_NUMBER", 10000, 100)
		//if err != nil {
		//	log.Fatalf(err.Error())
		//} else {
		//	fmt.Print("-----")
		//	for _, v := range client.Query.QueryResults {
		//		fmt.Print(v.ParsedRows)
		//	}
		//}
		tbls := client.GetTableSchema("oracle_flights", "PDBADMIN___FLIGHTS")
		tbls.Print()
	} else {
		log.Fatalln(err.Error())
	}
}

