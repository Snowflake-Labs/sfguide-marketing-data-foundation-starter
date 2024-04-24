export SNOWFLAKE_DEFAULT_CONNECTION_NAME=marketing_demo_conn
cd sfguide-marketing-data-foundation-starter
snow sql -f sql_scripts/setup.sql
snow object stage copy data/worldcities.csv @MARKETING_DATA_FOUNDATION.demo.data_stg/data
snow object stage copy data/sf_data/ @MARKETING_DATA_FOUNDATION.demo.data_stg/data/sf_data/ auto_compress=false parallel=8 overwrite=False
snow object stage copy data/ga_data/ @MARKETING_DATA_FOUNDATION.demo.data_stg/data/ga_data/ auto_compress=false parallel=10 overwrite=False
snow object stage copy data/sample_data.gz @MARKETING_DATA_FOUNDATION.demo.data_stg/data/
snow sql -f sql_scripts/build_views.sql
snow app run
