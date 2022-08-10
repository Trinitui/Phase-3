package main

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// The following is a sample record from the Taxi Trips dataset retrieved from the City of Chicago Data Portal

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// id	1830273
// permit_	"100190752"
// permit_type	"PERMIT - SIGNS"
// review_type	"SIGN PERMIT"
// application_start_date	"2007-10-16T00:00:00.000"
// issue_date	2008-03-05T00:00:00.000
// dropoff_census_tract	"17031081800"
// processing_time	141
// street_numer	6349
// street_direction "S"
// street_name	"COTTAGE GROVE"
// suffix 	"AVE"
// work_description "INSTALL BUILDING SIGN"
// building_fee_paid	70
// zoning_fee_paid	75
// other_fee_paid	0
// subtotal_paid	145
// building_fee_unpaid	0
// zoing_Fee_unpaid	0
// other_fee_unpaid	0
// subtotal_unpaid	0
// building_fee_wait	0
// zoning_fee_waived	0
// other_Fee_waived	0
// subtotal_waived	0
// total_fee	145
// contact_1_type	"SIGN CONTRACTOR"
// contact_1_name	"JAS. D. AHERN CO."
// contact_1_city	"CHICAGO X"
// contact_1_state	"IL"
// contact_1_zipcode	60623
// reported_cost	2000
// community_area 76
// ward	41
// pin1	20-23-100-005
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

type BuildingPermitsJsonRecords []struct {
	Permit_id         string `json:"id"`
	Permit_issue_date string `json:"issue_date"`
	Community_area    string `json:"community_area"`
}

func main() {
	fmt.Println("Starting Building Permit Microservice")
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
		GetBuildingPermits(db)

		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		time.Sleep(24 * time.Hour)
	}

}

func GetBuildingPermits(db *sql.DB) {

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// You need to complete the implmentation and add the data source: https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	geocoder.ApiKey = "AIzaSyB-JwmMaEwb3yEomj66SnNlkA5GyKcRfWU"

	drop_table := `drop table if exists building_permits`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "building_permits" (
						"id"   SERIAL , 
						"permit_id" VARCHAR(255) UNIQUE, 
						"permit_issue_date" DATE, 
						"community_area" INT, 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	fmt.Println("Grabbing data from Chicago Data...")
	var url = "https://data.cityofchicago.org/resource/ydr8-5enu.json"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	fmt.Println("Starting JSON unmarshalling...")
	body, _ := ioutil.ReadAll(res.Body)
	var building_permits_list BuildingPermitsJsonRecords
	json.Unmarshal(body, &building_permits_list)
	fmt.Println("JSON unmarshalling done...")
	fmt.Println("Now unpacking JSON and inserting into db... ")
	for i := 0; i < len(building_permits_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		permit_id := building_permits_list[i].Permit_id
		if permit_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		permit_issue_date := building_permits_list[i].Permit_issue_date
		if permit_issue_date == "" {
			continue
		}

		// get Trip_end_timestamp
		community_area := building_permits_list[i].Community_area
		if community_area == "" {
			continue
		}

		sql := `INSERT INTO building_permits ("permit_id", "permit_issue_date", "community_area") values($1, $2, $3)`

		_, err = db.Exec(
			sql,
			permit_id,
			permit_issue_date,
			community_area,
		)

		if err != nil {
			panic(err)
		}

	}
	fmt.Println("== Done with inserting Building Permits ==")

}
