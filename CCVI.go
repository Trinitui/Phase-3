package main

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// The following is a sample record from the CCVI dataset retrieved from the City of Chicago Data Portal

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// Geography_type	"CA"
// community_area_or_zip	"70"
// community_area_name	"Ashburn"
// ccvi_score	"45.1"
// ccvi_category	"MEDIUM"
// rank_scoioeconomic_status	"34"
// rank_household_composition	"32"
// rank_adults_no_pcp	"28"
// rank_cumulative_mobility_ratio	"45"
// rank_frontline_essential_workers "48"
// rank_age_65_plus	"29"
// rank_comorbit_conditions	"33"
// rank_covid_19_incidence_rate	"59"
// rank_covid_19_hospital_admission_rate	"66"
// rank_covid_19_crude_mortality_rate	"39"

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

type CCVIJsonRecords []struct {
	Geography_type        string `json:"geography_type"`
	Community_area_or_zip string `json:"community_area_or_zip"`
	Community_area_name   string `json:"community_area_name"`
	Ccvi_score            string `json:"ccvi_score"`
	Ccvi_category         string `json:"ccvi_category"`
}

func main() {
	fmt.Println("Starting CCVI MicroService!")
	// Establish connection to Postgres Database
	//db_connection := "user=postgres dbname=chicago_bi password=root host=localhost sslmode=disable"

	// Docker image for the microservice - uncomment when deploy
	db_connection := "user=postgres dbname=chicago_bi password=root host=8.8.8.8 sslmode=disable"

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
	fmt.Println("Done with DB ping...")

	// Spin in a loop and pull data from the city of chicago data portal
	// Once every hour, day, week, etc.
	// Though, please note that Not all datasets need to be pulled on daily basis
	// fine-tune the following code-snippet as you see necessary
	for {
		// build and fine-tune functions to pull data from different data sources
		// This is a code snippet to show you how to pull data from different data sources.
		GetCCVI(db)
		fmt.Println("Done!")
		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		time.Sleep(24 * time.Hour)
	}

}

func GetCCVI(db *sql.DB) {

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// You need to complete the implmentation and add the data source: https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	fmt.Println("GetCCVI: Grabbing CCVI Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	geocoder.ApiKey = "AIzaSyB-JwmMaEwb3yEomj66SnNlkA5GyKcRfWU"

	drop_table := `drop table if exists ccvi`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "ccvi" (
						"id"   SERIAL , 
						"geography_type" VARCHAR(255), 
						"community_area_or_zip" VARCHAR(255),
						"community_area_name" VARCHAR(255),
						"ccvi_score" FLOAT, 
						"ccvi_category" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	fmt.Println("Grabbing data from Chicago Data...")
	var url = "https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	fmt.Println("Starting JSON unmarshalling...")
	body, _ := ioutil.ReadAll(res.Body)
	var ccvi_list CCVIJsonRecords
	json.Unmarshal(body, &ccvi_list)
	fmt.Println("JSON unmarshalling done...")
	fmt.Println("Now unpacking JSON and inserting into db... ")
	for i := 0; i < len(ccvi_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		geo_type := ccvi_list[i].Geography_type
		if geo_type == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		community_zip_ca := ccvi_list[i].Community_area_or_zip
		if community_zip_ca == "" {
			continue
		}

		// get Trip_end_timestamp
		community_name := ccvi_list[i].Community_area_name
		if community_name == "" {
			continue
		}

		ccvi_score := ccvi_list[i].Ccvi_score
		if ccvi_score == "" {
			continue
		}

		ccvi_category := ccvi_list[i].Ccvi_category
		if ccvi_category == "" {
			continue
		}

		sql := `INSERT INTO ccvi ("geography_type", "community_area_or_zip", "community_area_name", "ccvi_score", "ccvi_category"
			) values($1, $2, $3, $4, $5)`

		_, err = db.Exec(
			sql,
			geo_type,
			community_zip_ca,
			community_name,
			ccvi_score,
			ccvi_category)

		if err != nil {
			panic(err)
		}

	}
	fmt.Println("== Done with CCVI ==")

}
