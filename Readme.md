# Blueprint Conduit Go SDK
* Is Dockerized
* Has Tests
* Can be run locally
* Has a swanky Makefile

## Instructions

### Using in an existing Go project
* Create a new directory and init a Go mod:
```
mkdir test
go mod init test
```
* Create a file called "test.go", with these contents:
```
package main

import (
	conduitclient "github.com/BlueprintConsulting/Conduit-GoSDK/conduit"
	"os"
)

func main() {
	cc := conduitclient.NewClient(
		os.Getenv("CONDUIT_SERVER"),
		os.Getenv("CONDUIT_TOKEN"))
	dbs := cc.GetDatabases()
	dbs.Print()
}

```
* Execute program, with envvars:
```
CONDUIT_SERVER=<servername> CONDUIT_TOKEN=<token> go run test.go
```
This should output databases to the screen.

### Driver Execution
* Runnable Locally (must have Go installed)
```
make run
```
* Runnable in docker (must have Docker installed)
```
make docker
```
* Run Tests (must have Go installed)
```
make showcoverage
```

## SDK Functions

* Get Databases
```
client := conduitclient.NewClient(
		        os.Getenv("CONDUIT_SERVER"),
		        os.Getenv("CONDUIT_TOKEN"))
dbs := client.GetDatabases()
dbs.Print()
```
* Get Tables for Given Database
```
client := conduitclient.NewClient(
		        os.Getenv("CONDUIT_SERVER"),
		        os.Getenv("CONDUIT_TOKEN"))
tables := client.GetTables("oracle_flights")
tables.Print()
```
* Get Table Schema
```
client := conduitclient.NewClient(
		        os.Getenv("CONDUIT_SERVER"),
		        os.Getenv("CONDUIT_TOKEN"))
tbls := client.GetTableSchema("oracle_flights", "PDBADMIN___FLIGHTS")
tbls.Print()
```
* Execute Query
```
client := conduitclient.NewClient(
		        os.Getenv("CONDUIT_SERVER"),
		        os.Getenv("CONDUIT_TOKEN"))
err = client.ExecuteQuery("SELECT * FROM `oracle_flights`.`PDBADMIN___FLIGHTS` ORDER BY TAIL_NUMBER", 10000, 100)
if err != nil {
	log.Fatalf(err.Error())
} else {
	for _, v := range client.Query.QueryResults {
		fmt.Print(v.ParsedRows)
	}
}
```
Note: the ExecuteQuery takes three parameters: the SQL String, the Window size, and the timeout (in seconds)