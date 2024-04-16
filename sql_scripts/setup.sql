-- =========================
-- Define Databases
-- =========================
create or replace database MARKETING_DATA_FOUNDATAION comment = 'used for demonstrating Snowflake for Marketing demo';
create or replace schema MARKETING_DATA_FOUNDATAION.DEMO;

-- =========================
-- Define stages
-- =========================
use schema MARKETING_DATA_FOUNDATAION.DEMO;

create or replace stage lib_stg
	directory = ( enable = true )
    comment = 'used for holding udfs and procs.';

create or replace stage data_stg
    comment = 'used for holding data.';

create or replace stage scripts_stg 
    comment = 'used for holding scripts.';

-- --------------------------------------

use database MARKETING_DATA_FOUNDATAION;
use schema demo;