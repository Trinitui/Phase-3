package main

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// The following is a sample record from the Covid datasets retrieved from the City of Chicago Data Portal

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
/*
"zip_code": "60601",
"week_number": "29",
"week_start": "2022-07-17T00:00:00.000",
"week_end": "2022-07-23T00:00:00.000",
"cases_weekly": "27",
"cases_cumulative": "4933",
"case_rate_weekly": "184",
"case_rate_cumulative": "33615",
"tests_weekly": "318",
"tests_cumulative": "97852",
"test_rate_weekly": "2167",
"test_rate_cumulative": "666793.9",
"percent_tested_positive_weekly": "0.129",
"percent_tested_positive_cumulative": "0.06",
"deaths_weekly": "0",
"deaths_cumulative": "13",
"death_rate_weekly": "0",
"death_rate_cumulative": "88.6",
"population": "14675",
"row_id": "60601-2022-29",
"zip_code_location": {
  "type": "Point",
  "coordinates": [
	-87.622844,
	41.886262
  ]
},
":@computed_region_rpca_8um6": "42",
":@computed_region_vrxf_vc4k": "38",
":@computed_region_6mkv_f3dw": "14309",
":@computed_region_bdys_3d7i": "580",
":@computed_region_43wa_7qmu": "36"
*/
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"database/sql"
	"encoding/json"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type CovidDataJsonRecords []struct {
	Zip_code                       string `json:"zip_code"`
	Week_number                    string `json:"week_number"`
	Week_end                       string `json:"week_end"`
	Cases_weekly                   string `json:"cases_weekly"`
	Tests_weekly                   string `json:"tests_weekly"`
	Deaths_weekly                  string `json:"deaths_weekly"`
	Percent_tested_positive_weekly string `json:"percent_tested_positive_weekly"`
}

func main() {
	fmt.Println("Starting COVID Data MicroService!")
	// Establish connection to Postgres Database
	db_connection := "user=postgres dbname=chicago_bi password=root host=localhost sslmode=disable"

	// Docker image for the microservice - uncomment when deploy
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=host.docker.internal sslmode=disable"

	db, err := sql.Open("postgres", db_connection)
	if err != nil {
		panic(err)
	}

	// Test the database connection
	err = db.Ping()
	if err != nil {
		fmt.Println("Couldn't Connect to database")
		panic(err)
	}

	// Spin in a loop and pull data from the city of chicago data portal
	// Once every hour, day, week, etc.
	// Though, please note that Not all datasets need to be pulled on daily basis
	// fine-tune the following code-snippet as you see necessary
	for {
		// build and fine-tune functions to pull data from different data sources
		// This is a code snippet to show you how to pull data from different data sources.
		GetCovidData(db)

		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		time.Sleep(24 * time.Hour)
	}

}

func GetCovidData(db *sql.DB) {

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// You need to complete the implmentation and add the data source: https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	fmt.Println("GetCovidData: Grabbing COVID Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	geocoder.ApiKey = "AIzaSyB-JwmMaEwb3yEomj66SnNlkA5GyKcRfWU"

	drop_table := `drop table if exists covid_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS covid_data (
						"id"   SERIAL , 
						"zip_code" VARCHAR(255), 
						"week_number" INT,
						"week_end" DATE,
						"cases_weekly" FLOAT,
						"tests_weekly" FLOAT, 
						"deaths_weekly" FLOAT, 
						"percent_tested_positive_weekly" FLOAT,
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	fmt.Println("Grabbing data from Chicago Data...")
	var url = "https://data.cityofchicago.org/resource/yhhz-zm2v.json"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	fmt.Println("Starting JSON unmarshalling...")
	body, _ := ioutil.ReadAll(res.Body)
	var covid_list CovidDataJsonRecords
	json.Unmarshal(body, &covid_list)
	fmt.Println("JSON unmarshalling done...")
	fmt.Println("Now unpacking JSON and inserting into db... ")
	for i := 0; i < len(covid_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		zip_code := covid_list[i].Zip_code
		if zip_code == "" {
			continue
		}

		week_number := covid_list[i].Week_number
		if week_number == "" {
			continue
		}

		week_end := covid_list[i].Week_end
		if week_end == "" {
			continue
		}

		cases_weekly := covid_list[i].Cases_weekly
		if cases_weekly == "" {
			continue
		}

		tests_weekly := covid_list[i].Tests_weekly
		if tests_weekly == "" {
			continue
		}

		deaths_weekly := covid_list[i].Deaths_weekly
		if deaths_weekly == "" {
			continue
		}

		percent_tested_positive_weekly := covid_list[i].Percent_tested_positive_weekly
		if percent_tested_positive_weekly == "" {
			continue
		}

		sql := `INSERT INTO covid_data ("zip_code", "week_number", "week_end", "cases_weekly", "tests_weekly", "deaths_weekly", "percent_tested_positive_weekly"
			) values($1, $2, $3, $4, $5, $6, $7)`

		_, err = db.Exec(
			sql,
			zip_code,
			week_number,
			week_end,
			cases_weekly,
			tests_weekly,
			deaths_weekly,
			percent_tested_positive_weekly)

		if err != nil {
			panic(err)
		}

	}
	fmt.Println("== Done with Covid Data ==")

}
