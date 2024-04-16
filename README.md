# Marketing Data Foundation Starter

## Introduction

Customers looking to use Snowflake for marketing use cases initially face a significant challenge: it is difficult to import all relevant marketing data into Snowflake and structure it in a unified format that downstream applications can easily utilize to power data-driven marketing.

This starter solution tackles this challenge of creating a marketing data foundation by offering two independent solutions and a merged version, demonstrating how to integrate both solutions into a comprehensive 'marketing app suite'. 
- Marketing Data Foundation Starter (Merges the two apps below into a ‘marketing app suite’)
  - Data Foundation Starter for Customer 360
  - Data Foundation Starter for Campaign Intelligence

This solution was inspired by how Snowflake runs its own end-to-end Marketing workflows entirely on top of the Snowflake Marketing Data Cloud.

## Solution Space

### Context

As described in the diagram below, the two Data Foundation use cases in this starter lay the groundwork to support the two Marketing Execution use cases: Planning & Activation, and Measurement.

![Alt text](images/context.png)

More specifically, this solution covers Data Ingestion, Semantic Unification, and based Analytics use cases for Customer 360 and Campaign Intelligence data.

![Alt text](images/context2.png)


## Step-By-Step Guide

For prerequisites, environment setup, step-by-step guide and instructions, please refer to the [QuickStart Guide](https://quickstarts.snowflake.com/guide/call_centre_analytics_with_snowflake_cortex_and_spcs/index.html).

## Installation of Snow CLI

Please follow the instructions in our documentation to install [Snow CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/installation/installation)

It is as simple as running the below command.

`
pip install snowflake-cli-labs
`

### Establishing connection

Please follow the instructions in our documentation to configure your [connection to Snowflake](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/connecting/specify-credentials)

#### Create a connection

`
snow connection add
`

`
snow connection test --connection="test_connection"
`

Refer to the screenshot below for more info.


![Alt text](<Images/Snow connection - create and test.png>)

## Clone the git repo

`
git clone https://github.com/Snowflake-Labs/sfguide-marketing-data-foundation-starter.git
`

## Building Native Application

### Step 1: Create Database objects

Navigate to the repo folder in your local machine and run the below command to create your database, schema and stage objects

First lets export the connection name to the default connection

`
export SNOWFLAKE_DEFAULT_CONNECTION_NAME=marketing_demo_conn
`

`
snow sql -f sql_scripts/setup.sql
`

### Step 2: Upload sample data to stage

Upload all the sample data files in the folder data to stage created in step 1


` 
snow object stage copy data/worldcities.csv @MARKETING_DATA_FOUNDATAION.demo.data_stg/data
`

`
snow object stage copy data/sf_data/ @MARKETING_DATA_FOUNDATAION.demo.data_stg/data/sf_data/
`

`
snow object stage copy data/ga_data/ @MARKETING_DATA_FOUNDATAION.demo.data_stg/data/ga_data/
`

`
snow object stage copy data/sample_data.gz @MARKETING_DATA_FOUNDATAION.demo.data_stg/data/
`


If the upload fails due to access issue then, please follow the instructions in this [document](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage-ui) to upload the files directly to Snowflake Stage.

### Step 3: Load Sample data to table and create views

Run the below command to create the views that will be bundled along with the nativeapp

`
snow sql -f sql_scripts/build_views.sql
`

### Step 4: Create Native Application

Build NativeApp

`
snow app run
`
