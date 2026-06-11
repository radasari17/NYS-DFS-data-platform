USE DATABASE NYS_DFS_RETAIL;
USE SCHEMA STAGE;

-- Create the internal loading dock for our Master Data CSVs
CREATE STAGE csv_stage
    FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format
    DIRECTORY = (ENABLE = TRUE);
Step 2: The Git Push (Locking in the Blueprint)
Open Notepad on your Windows computer.
Paste all of the SQL code you ran today into Notepad. It should look exactly like this:
CREATE DATABASE NYS_DFS_RETAIL;
USE DATABASE NYS_DFS_RETAIL;

CREATE SCHEMA STAGE;       
CREATE SCHEMA CLEAN;       
CREATE SCHEMA CONSUMPTION; 
CREATE SCHEMA COMMON;      

USE SCHEMA COMMON;
CREATE FILE FORMAT csv_file_format
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

USE SCHEMA STAGE;
CREATE STAGE csv_stage
    FILE_FORMAT = NYS_DFS_RETAIL.COMMON.csv_file_format
    DIRECTORY = (ENABLE = TRUE);