# Marketing Data Foundation Starter

## Overview
In this guide we will build a Call Centre Analytics solution built for analyzing insurance call center audio files. Leveraging Snowflake features like cortex, large language model running in Snowpark containers, we transcribes text and duration from audiofile,extracts essential information such as Customer details, Agent interactions, Sentiment analysis, Summary, Resolution from each audio call to name a few. Here are key highlights and features of the solution :

Whisper running in Snowpark Containers to Extract Text and Duration of the call from the audio files.

Using Cortex LLM functions for diarization to identify customer and representative.

Snowpark and Cortext LLM function to summarize and extract various information from call conversation.

Using Cortex Vector Search and Embedding to store embedding in Vector Type.

LLM model fine tuned for SQL queries running in SPCS for converting natural language to SQL query.

Streamlit APP which has a dashboard for audio analytics, chatbot on your data using RAG based approach. Also a Text2SQL chatbot for generating SQL queries and executing them from natural language input text.

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

## Building NativeApplication

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
