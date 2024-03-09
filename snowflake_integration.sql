-- Creating Database
CREATE OR REPLACE DATABASE spotify_db;

-- Creating Schemas
CREATE OR REPLACE SCHEMA SPOTIFY_DB.tables;

CREATE OR REPLACE SCHEMA SPOTIFY_DB.external_stages;

CREATE OR REPLACE SCHEMA SPOTIFY_DB.file_formats;

CREATE OR REPLACE SCHEMA SPOTIFY_DB.pipes;

-- Creating Tables

-- Album
CREATE OR REPLACE TABLE SPOTIFY_DB.TABLES.albums(
    album_id STRING,
    album_name STRING,
    album_release_date DATE,
    album_total_tracks INT,
    album_external_url STRING
);

-- Artist
CREATE OR REPLACE TABLE SPOTIFY_DB.TABLES.artists(
    artist_id STRING,
    artsit_name STRING,
    artist_external_url STRING
);

-- Song
CREATE OR REPLACE TABLE SPOTIFY_DB.TABLES.songs(
    song_id STRING,
    song_name STRING,
    song_duration_in_ms INT,
    song_url STRING,
    song_popularity INT,
    song_added TIMESTAMP_TZ,
    album_id STRING,
    artist_id STRING
);

-- Creating Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION s3_init
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = '<Your Role ARN>'
STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-snowflake-project')
    COMMENT = 'Connecting S3 with Snowflake';

DESC STORAGE INTEGRATION s3_init;


-- Creating File Format
CREATE OR REPLACE FILE FORMAT SPOTIFY_DB.FILE_FORMATS.csv_file_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL','null')
EMPTY_FIELD_AS_NULL = TRUE;


-- Creating STAGE
CREATE OR REPLACE STAGE SPOTIFY_DB.EXTERNAL_STAGES.aws_stage
URL = 's3://spotify-etl-snowflake-project/transformed_data/'
STORAGE_INTEGRATION = s3_init;

-- Checking the files inside stage
LIST @SPOTIFY_DB.EXTERNAL_STAGES.aws_stage/album_data;


-- Creating Pipes

-- Album
CREATE OR REPLACE PIPE SPOTIFY_DB.PIPES.album_pipe
AUTO_INGEST = TRUE
AS
COPY INTO SPOTIFY_DB.TABLES.albums
FROM @SPOTIFY_DB.EXTERNAL_STAGES.aws_stage/album_data
FILE_FORMAT = SPOTIFY_DB.FILE_FORMATS.csv_file_format;

-- Artist
CREATE OR REPLACE PIPE SPOTIFY_DB.PIPES.artist_pipe
AUTO_INGEST = TRUE
AS
COPY INTO SPOTIFY_DB.TABLES.artists
FROM @SPOTIFY_DB.EXTERNAL_STAGES.aws_stage/artist_data
FILE_FORMAT = SPOTIFY_DB.FILE_FORMATS.csv_file_format;

-- Song
CREATE OR REPLACE PIPE SPOTIFY_DB.PIPES.song_pipe
AUTO_INGEST = TRUE
AS
COPY INTO SPOTIFY_DB.TABLES.SONGS
FROM @SPOTIFY_DB.EXTERNAL_STAGES.aws_stage/song_data
FILE_FORMAT = SPOTIFY_DB.FILE_FORMATS.csv_file_format;

-- Checking for notification channel to create Event Notification in S3 
DESC PIPE SPOTIFY_DB.PIPES.ALBUM_PIPE;
DESC PIPE SPOTIFY_DB.PIPES.ARTIST_PIPE;
DESC PIPE SPOTIFY_DB.PIPES.SONG_PIPE;

-- Checking pipe status
SELECT SYSTEM$PIPE_STATUS('SPOTIFY_DB.PIPES.SONG_PIPE');

-- For checking COPY command and truncating data after checking
TRUNCATE SPOTIFY_DB.TABLES.albums;
TRUNCATE SPOTIFY_DB.TABLES.artists;
TRUNCATE SPOTIFY_DB.TABLES.songs;


-- Quering Tables
SELECT * FROM SPOTIFY_DB.TABLES.ALBUMS;
SELECT * FROM SPOTIFY_DB.TABLES.ARTISTS;
SELECT * FROM SPOTIFY_DB.TABLES.SONGS;