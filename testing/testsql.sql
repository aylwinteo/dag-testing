-- Replace your_dataset with your dataset name
-- Creates events_20260406 to events_20260410
DECLARE start_date DATE DEFAULT DATE '2024-04-06';
DECLARE end_date DATE DEFAULT DATE '2024-04-10';

-- Loop over dates
FOR d IN (SELECT start_date + INTERVAL x DAY AS dt
          FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS x) DO

  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `quarantine-project.your_dataset.events_%s` AS
    WITH fake_data AS (
      SELECT
        GENERATE_UUID() AS id,
        CAST(FLOOR(RAND()*4) AS INT64) AS event_type_id,
        CASE CAST(FLOOR(RAND()*4) AS INT64)
          WHEN 0 THEN 'login'
          WHEN 1 THEN 'logout'
          WHEN 2 THEN 'purchase'
          ELSE 'click'
        END AS event_type,
        CONCAT('Test description ', CAST(FLOOR(RAND()*1000) AS STRING)) AS description,
        CURRENT_TIMESTAMP() AS last_modified
      FROM UNNEST(GENERATE_ARRAY(1, 100)) AS x
    )
    SELECT * FROM fake_data;
  """, FORMAT_DATE('%Y%m%d', d.dt));

END FOR;