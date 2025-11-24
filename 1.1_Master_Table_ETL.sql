------ MASTER TABLE(5 Datasets) ETL

--Creating Function to Load Data from Staging Tables to Master Table

CREATE OR REPLACE PROCEDURE "Public".load_master_table()
LANGUAGE plpgsql
AS $$
BEGIN
---CHECKING FOR DUPLICATE TABLE

DROP TABLE IF EXISTS "Public".master_table;

--- CREATING MASTER TABLE

CREATE TABLE IF NOT EXISTS "Public".master_table(
index_no SERIAL PRIMARY KEY ,

--Cognito Data
gender TEXT,
birthdate DATE,
city TEXT,
state TEXT,

-- User Data
learner_id VARCHAR,
institution VARCHAR,
country TEXT,
degree TEXT,
major TEXT,

--Learner Opportunity Data
status INTEGER,
apply_date TIMESTAMPTZ,

--Cohort Data
cohort_code VARCHAR,
start_date TIMESTAMP,
end_date TIMESTAMP,
size INTEGER,

--Opportunity Data
opportunity_name VARCHAR,
category VARCHAR 
);

--Insertion of data in MASTER TABLE from stage tables
INSERT INTO "Public".master_table(

--Cognito Data
gender,
birthdate,
city,
state,

-- User Data
learner_id,
country,
degree,
institution,
major,

--Learner Opportunity Data
status,
apply_date,

--Cohort Data
cohort_code,
start_date,
end_date,
size,

--Opportunity Data
opportunity_name,
category
)

---INSERTING VALUES
select 
c.gender, c.birthdate, c.city, c.state,             			-----cognito data
u.learner_id,u.country, u.degree, u.institution, u.major,       -----user data
lo.status, lo.apply_date,     					                -----Learner Opportunity Data
cd.cohort_code, cd.start_date, cd.end_date, cd.size,		    -----Cohort Data
o.opportunity_name, o.category                    			    -----Opportunity Data
from "Public".user_data_staging as u
join "Public".cognito_raw_staging as c on  u.learner_id = c.user_id
join "Public".learner_opportunity_staging as lo on c.user_id = lo.enrollment_id
join "Public".cohort_data_staging as cd on lo.assigned_cohort= cd.cohort_code
join "Public".opportunity_staging as o on lo.learner_id = o.opportunity_id;
END;
$$;
--------------------------------------------------------------------------------------------------------
-- ETL Store Procedure cognito data
CREATE OR REPLACE PROCEDURE "Public".ETL_cognito_data()
LANGUAGE plpgsql
AS $$
DECLARE
    mode_gender TEXT;
    mode_city TEXT;
    mode_zip BIGINT;
    mode_state TEXT;
    median_birthdate DATE;
BEGIN
-- Clear the cleaned table first
DROP TABLE IF EXISTS "Public".cognito_raw_staging;
  -- Staging table

CREATE TABLE "Public".cognito_raw_staging (
user_id VARCHAR PRIMARY KEY,
email VARCHAR,
gender TEXT,
user_create_date TIMESTAMP,
user_last_modified_date TIMESTAMP,
birthdate DATE,
city TEXT,
zip BIGINT,
state TEXT
);
    -- Get mode (most common) gender
    SELECT UPPER(gender) INTO mode_gender
    FROM "Public".cognito_raw
    WHERE gender IS NOT NULL AND LOWER(TRIM(gender)) <> 'null'
    GROUP BY gender
    ORDER BY COUNT(*) DESC
    LIMIT 1;

    -- Get mode (most common) city
    SELECT UPPER(city) INTO mode_city
    FROM "Public".cognito_raw
    WHERE city IS NOT NULL AND LOWER(TRIM(city)) <>  'null'
    GROUP BY city
    ORDER BY COUNT(*) DESC
    LIMIT 1;

    -- Get mode (most common) zip
    SELECT zip INTO mode_zip
    FROM "Public".cognito_raw
    WHERE zip IS NOT NULL AND TRIM(zip) ~ '^\d+$'
    GROUP BY zip
    ORDER BY COUNT(*) DESC
    LIMIT 1;

    -- Get mode (most common) state
    SELECT UPPER(state) INTO mode_state
    FROM "Public".cognito_raw
    WHERE state IS NOT NULL AND LOWER(TRIM(state)) <> 'null'
    GROUP BY state
    ORDER BY COUNT(*) DESC
    LIMIT 1;

  -- Get median of birthdate
	WITH ranked_birthdates AS (
        SELECT birthdate,
               ROW_NUMBER() OVER (ORDER BY birthdate) AS row_num,
               COUNT(*) OVER () AS total_rows
        FROM "Public".cognito_raw
        WHERE birthdate IS NOT NULL
    )
    SELECT birthdate INTO median_birthdate
    FROM ranked_birthdates
    WHERE row_num = FLOOR((total_rows + 1) / 2)
    LIMIT 1;

    -- Insert cleaned data
    INSERT INTO "Public".cognito_raw_staging (
        user_id,
        email,
        gender,
        user_create_date,
        user_last_modified_date,
        birthdate,
        city,
        zip,
        state
    )
    SELECT
        TRIM(user_id),
        COALESCE(NULLIF(UPPER(TRIM(email)), ''), 'UNKNOWN'),
       COALESCE(
    CASE
        WHEN UPPER(TRIM(gender)) = 'DON%27T WANT TO SPECIFY' THEN 'DO NOT WANT TO SPECIFY'
        ELSE NULLIF(UPPER(TRIM(gender)), 'NULL')
    END,
    mode_gender
),
        TO_TIMESTAMP(user_create_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        TO_TIMESTAMP(user_last_modified_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
		
       	COALESCE(
    CASE
        WHEN birthdate ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
        THEN TO_DATE(birthdate, 'DD-MM-YYYY')
        ELSE NULL
    END, median_birthdate),
        COALESCE(NULLIF(UPPER(TRIM(city)), ''), mode_city),
        CASE
  WHEN TRIM(zip) ~ '^\d+$' THEN CAST(TRIM(zip) AS BIGINT)
  ELSE mode_zip
END,
	COALESCE(NULLIF(UPPER(TRIM(state)), ''), mode_state)
    FROM "Public".cognito_raw
    WHERE NOT (
        TRIM(gender) IN ('NULL','', 'null') AND
        TRIM(city) IN ('NULL','', 'null') AND
        TRIM(zip) IN ('NULL','', 'null') AND
        TRIM(state) IN ('NULL', '', 'null')
    );
END;
$$;
------------------------------------------------------------------------------------------------------
-- ETL store Procedure user_data
CREATE OR REPLACE PROCEDURE "Public".ETL_user_data()
LANGUAGE plpgsql
AS $$
DECLARE
    mode_degree TEXT;
    mode_institution TEXT;
    mode_major TEXT;

BEGIN
  -- Dropping the last executed table first
  
DROP TABLE IF EXISTS "Public".user_data_staging;

--Created Stage Table

CREATE TABLE "Public".user_data_staging (
    learner_id VARCHAR PRIMARY KEY,
    degree TEXT,
    institution TEXT,
    major TEXT,
    country TEXT
	);
-- checking most frequent (mode) values
SELECT upper(degree) INTO mode_degree
FROM "Public".user_data
WHERE TRIM(degree) NOT IN ('NULL', '')
GROUP BY degree
ORDER BY COUNT(*) DESC
LIMIT 1;

SELECT upper(institution) INTO mode_institution
FROM "Public".user_data
WHERE TRIM(institution) NOT IN ('NULL', '')
GROUP BY institution
ORDER BY COUNT(*) DESC
LIMIT 1;

SELECT Upper(major) INTO mode_major
FROM "Public".user_data
WHERE TRIM(major) NOT IN ('NULL', '')
GROUP BY major
ORDER BY COUNT(*) DESC
LIMIT 1;

-- Insertion of transformed records into staging table
INSERT INTO "Public".user_data_staging (
learner_id,
degree,
institution,
major,
country)
SELECT SPLIT_PART(learner_id, '#', 2),
COALESCE(NULLIF(UPPER(TRIM(degree)), 'NULL'), mode_degree),
COALESCE(NULLIF(UPPER(TRIM(institution)), 'NULL'), mode_institution),
COALESCE(
CASE 
WHEN LOWER(TRIM(major)) IN ('na','n a','nil','not','none','no','nou','ntg','xyz','job','hii','hey', 'null') THEN 'UNKNOWN'
ELSE UPPER(TRIM(major))
END,
mode_major),
COALESCE(NULLIF(UPPER(TRIM(country)), 'NULL'), 'UNKNOWN')
FROM "Public".user_data;
END;
$$;
-------------------------------------------------------------------------------------------------------
-- ETL store Procedure Learner Opportunity

CREATE OR REPLACE PROCEDURE "Public".ETL_learner_opportunity()
LANGUAGE plpgsql
AS $$
DECLARE
    mode_cohort VARCHAR;
    mode_status INTEGER; 
    latest_apply_date TIMESTAMPTZ;
BEGIN
DROP TABLE IF EXISTS "Public".learner_opportunity_staging;

--Created Stage Table
CREATE TABLE "Public".learner_opportunity_staging (
    enrollment_id VARCHAR,
    learner_id VARCHAR,
    assigned_cohort VARCHAR,
    apply_date TIMESTAMPTZ,
    status INTEGER
);
    -- Get most frequent assigned_cohort
    SELECT UPPER(assigned_cohort)
    INTO mode_cohort
    FROM "Public".learner_opportunity_raw
    WHERE assigned_cohort IS NOT NULL AND TRIM(assigned_cohort) <> ''
    GROUP BY assigned_cohort
    ORDER BY COUNT(*) DESC 
    LIMIT 1;

    -- Get most frequent status (cast to INTEGER)
    SELECT status
    INTO mode_status
    FROM "Public".learner_opportunity_raw
    WHERE status IS NOT NULL
    GROUP BY status
    ORDER BY COUNT(*) DESC 
    LIMIT 1;

    -- Get latest apply_date (ensure it is a TIMESTAMP)

    SELECT MAX(apply_date::TIMESTAMPTZ)
    INTO latest_apply_date
    FROM "Public".learner_opportunity_raw
    WHERE apply_date IS NOT NULL AND UPPER(TRIM(apply_date::TEXT)) <> 'NULL';

    -- Insert cleaned/transformed data into the staging table

    INSERT INTO "Public".learner_opportunity_staging (
        enrollment_id,
        learner_id,
        assigned_cohort,
        apply_date,
        status
    )
    SELECT
        SPLIT_PART(enrollment_id, '#', 2),
        TRIM(learner_id),
        COALESCE(NULLIF(TRIM(assigned_cohort), ''), mode_cohort),
        CASE 
    WHEN apply_date IS NOT NULL AND UPPER(TRIM(apply_date::TEXT)) <> 'NULL'
    THEN apply_date::TIMESTAMPTZ
    ELSE latest_apply_date
END,
        COALESCE(NULLIF(TRIM(NULLIF(status, 'NULL')), '')::INTEGER, mode_status)
    FROM "Public".learner_opportunity_raw
    WHERE NOT (
        (enrollment_id IS NULL OR TRIM(enrollment_id) = 'NULL') AND
        (learner_id IS NULL OR TRIM(learner_id) = 'NULL') AND
        (assigned_cohort IS NULL OR TRIM(assigned_cohort) = 'NULL') AND
        (apply_date IS NULL OR UPPER(TRIM(apply_date::TEXT)) = 'NULL') AND
        (status IS NULL OR TRIM(status) = '' OR LOWER(TRIM(status)) = 'NULL')
    );
END;
$$;
-------------------------------------------------------------------------------------------------------
--ETL store Procedure cohort_data
CREATE OR REPLACE PROCEDURE "Public".ETL_cohort_data() 
LANGUAGE plpgsql 
AS $$ 
BEGIN 
--Drop Table 
DROP TABLE IF EXISTS "Public".cohort_data_staging;
-- Create Staging Table 


CREATE TABLE "Public".cohort_data_staging ( 
	cohort_id VARCHAR, 
   	cohort_code VARCHAR PRIMARY KEY, 
   	start_date TIMESTAMP, 
   	end_date TIMESTAMP, 
   	size INTEGER
   	); 
-- Insert cleaned records into cleaned table 
INSERT INTO "Public".cohort_data_staging ( 
        cohort_id,
		cohort_code,
		start_date,
		end_date,
		size 
    ) 
    SELECT  cohort_id,
			cohort_code,
        	TO_TIMESTAMP(CAST(start_date AS DOUBLE PRECISION) / 1000), 
			TO_TIMESTAMP(CAST(end_date AS DOUBLE PRECISION) / 1000), 
			cast(size as INT)
			FROM "Public".cohort_raw
    WHERE NOT ( 
        (NULLIF(LOWER(TRIM(cohort_id)), 'Null') IS NULL OR TRIM(cohort_id) = '') AND 
        (NULLIF(LOWER(TRIM(cohort_code)), 'Null') IS NULL OR TRIM(cohort_code) = '') AND 
        start_date IS NULL AND
		end_date IS NULL AND
		size IS NULL); 
END; 
$$;
------------------------------------------------------------------------------------------------------
-- ETL store Procedure Opportunity Data
CREATE OR REPLACE PROCEDURE "Public".ETL_opportunity_data()
LANGUAGE plpgsql 
AS $$ 
Declare
mode_opportunity_name VARCHAR;
mode_category VARCHAR;

BEGIN 
Select Upper(opportunity_name) into mode_opportunity_name
from "Public".opportunity_data
WHERE opportunity_name IS NOT NULL AND TRIM(opportunity_name) <> ''
group by opportunity_name
order by count(*) desc
limit 1;

Select Upper(category) into mode_category
from "Public".opportunity_data
WHERE category IS NOT NULL AND TRIM(category) <> ''
group by category
order by count(*) desc
limit 1;

-- Drop Table
 DROP TABLE IF EXISTS "Public".opportunity_staging;
-- Stage Table
CREATE TABLE "Public".opportunity_staging (
    opportunity_id VARCHAR PRIMARY KEY, 
    opportunity_name VARCHAR,
    category VARCHAR,
    opportunity_code VARCHAR,
    tracking_questions TEXT
);
    -- Insert cleaned data into staging 
    INSERT INTO "Public".opportunity_staging( 
        opportunity_id,
		opportunity_name,
		category,
		opportunity_code,
		tracking_questions) 
    SELECT 
        opportunity_id, 
        COALESCE(NULLIF(UPPER(TRIM(opportunity_name)), 'null'), mode_opportunity_name), 
		COALESCE(NULLIF(UPPER(TRIM(category)), 'null'), mode_category),
		COALESCE(NULLIF(UPPER(TRIM(opportunity_code)), 'null'),'UNKNOWN'),
        COALESCE(NULLIF(UPPER(TRIM(tracking_questions)), 'null'),'UNKNOWN')
   	FROM "Public".opportunity_data 
    WHERE ((opportunity_id IS NOT NULL AND TRIM(opportunity_id) <> '') AND
	(opportunity_name IS NOT NULL AND TRIM(opportunity_name) <> '')AND
	(category IS NOT NULL AND TRIM(category) <> '') AND
	(tracking_questions IS NOT NULL AND TRIM(tracking_questions) <> '')); 
END; 
$$;
------------------------------------------------------------------------------------------------------
--Create Meta-ETL Function
CREATE OR REPLACE PROCEDURE "Public".meta_etl()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL "Public".etl_cognito_data();
    CALL "Public".etl_user_data();
    CALL "Public".etl_learner_opportunity();
    CALL "Public".etl_cohort_data();
    CALL "Public".etl_opportunity_data();
    CALL "Public".load_master_table();
END;
$$;
--------CALL THIS TOGETHER to Execute
CALL "Public".meta_etl();
select * from "Public".master_table;