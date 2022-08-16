package main

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// The following is a sample record from the Covid datasets retrieved from the City of Chicago Data Portal

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
/*
   "community_area": "1",
    "community_area_name": "Rogers Park",
    "birth_rate": "16.4",
    "general_fertility_rate": "62",
    "low_birth_weight": "11",
    "prenatal_care_beginning_in_first_trimester": "73",
    "preterm_births": "11.2",
    "teen_birth_rate": "40.8",
    "assault_homicide": "7.7",
    "breast_cancer_in_females": "23.3",
    "cancer_all_sites": "176.9",
    "colorectal_cancer": "25.3",
    "diabetes_related": "77.1",
    "firearm_related": "5.2",
    "infant_mortality_rate": "6.4",
    "lung_cancer": "36.7",
    "prostate_cancer_in_males": "21.7",
    "stroke_cerebrovascular_disease": "33.7",
    "childhood_blood_lead_level_screening": "364.7",
    "childhood_lead_poisoning": "0.5",
    "gonorrhea_in_females": "322.5",
    "gonorrhea_in_males": "423.3",
    "tuberculosis": "11.4",
    "below_poverty_level": "22.7",
    "crowded_housing": "7.9",
    "dependency": "28.8",
    "no_high_school_diploma": "18.1",
    "per_capita_income": "23714",
    "unemployment": "7.5"
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

type UnemploymentJsonRecords []struct {
	Community_area string `json:"community_area"`
	Unemployment   string `json:"unemployment"`
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
		GetUnemploymentData(db)

		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		time.Sleep(24 * time.Hour)
	}

}

func GetUnemploymentData(db *sql.DB) {

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// You need to complete the implmentation and add the data source: https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	fmt.Println("GetUnemploymentData: Grabbing Unemployment Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	geocoder.ApiKey = "AIzaSyB-JwmMaEwb3yEomj66SnNlkA5GyKcRfWU"

	drop_table := `drop table if exists unemployment_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS unemployment_data (
						"id"   SERIAL , 
						"community_area" VARCHAR(255), 
						"unemployment_rate" FLOAT,
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	fmt.Println("Grabbing data from Chicago Data...")
	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	fmt.Println("Starting JSON unmarshalling...")
	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_list UnemploymentJsonRecords
	json.Unmarshal(body, &unemployment_list)
	fmt.Println("JSON unmarshalling done...")
	fmt.Println("Now unpacking JSON and inserting into db... ")
	for i := 0; i < len(unemployment_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		community_area := unemployment_list[i].Community_area
		if community_area == "" {
			continue
		}

		unemployment_rate := unemployment_list[i].Unemployment
		if unemployment_rate == "" {
			continue
		}

		sql := `INSERT INTO unemployment_data ("community_area", "unemployment_rate"
			) values($1, $2)`

		_, err = db.Exec(
			sql,
			community_area,
			unemployment_rate)

		if err != nil {
			panic(err)
		}

	}
	fmt.Println("== Done with Unemployment Data ==")

}
