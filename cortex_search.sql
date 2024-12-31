CREATE DATABASE IF NOT EXISTS marvel;

CREATE OR REPLACE WAREHOUSE marvel_wh WITH
   WAREHOUSE_SIZE='X-SMALL';

CREATE OR REPLACE SCHEMA marvel.services;

ALTER TABLE EVENTS_COMBINED SET
  CHANGE_TRACKING = TRUE;

CREATE OR REPLACE CORTEX SEARCH SERVICE marvel_search_service
  ON combined_text
  ATTRIBUTES title
  WAREHOUSE = marvel_wh
  TARGET_LAG = '1 day'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
  AS (
    SELECT
        combined_text,
        title
    FROM marvel.public.EVENTS_COMBINED
);

//GRANT USAGE ON DATABASE marvel TO ROLE ACCOUNTADMIN;
//GRANT USAGE ON SCHEMA services TO ROLE ACCOUNTADMIN;

//GRANT USAGE ON CORTEX SEARCH SERVICE marvel_search_service TO ROLE ACCOUNTADMIN;

//USE ROLE ACCOUNTADMIN;

SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'marvel.services.marvel_search_service',
      '{
        "query": "spiderman",
        "columns":[
            "combined_text",
            "title"
        ],
        "limit":1
      }'
  )
)['results'] as results;

SELECT * 
FROM SNOWFLAKE.CORTEX.SEARCH_SERVICES;

SHOW FEATURES;



