-- ===============================================
-- BigQuery Test Setup for Quarantine Transfer DAG
-- ===============================================

DECLARE today DATE DEFAULT DATE '2026-04-10';

-- -----------------------------
-- 1. Create tables for last 4 days + today
-- -----------------------------
DECLARE day_offsets ARRAY<INT64> DEFAULT [4,3,2,1,0];

FOR offset IN UNNEST(day_offsets) DO
  DECLARE table_date STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(today, INTERVAL offset DAY));
  DECLARE table_name STRING DEFAULT CONCAT('events_', table_date);
  
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `quarantine-project.your_dataset.%s` (
      id STRING,
      event_type_id INT64,
      event_type STRING,
      description STRING,
      last_modified TIMESTAMP
    )
  """, table_name);
END FOR;

-- -----------------------------
-- 2. Insert initial fake data
-- -----------------------------
-- events_20260407 (oldest day)
INSERT INTO `quarantine-project.your_dataset.events_20260407` (id, event_type_id, event_type, description, last_modified)
SELECT
  GENERATE_UUID(),
  CAST(FLOOR(RAND()*5) AS INT64),
  CASE CAST(FLOOR(RAND()*5) AS INT64)
    WHEN 0 THEN 'login'
    WHEN 1 THEN 'logout'
    WHEN 2 THEN 'purchase'
    WHEN 3 THEN 'click'
    ELSE 'view'
  END,
  CONCAT('Initial event ', CAST(ROW_NUMBER() OVER() AS STRING)),
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 DAY)
FROM UNNEST(GENERATE_ARRAY(1, 5));

-- events_20260408 → simulate missing table by dropping after creation
DROP TABLE IF EXISTS `quarantine-project.your_dataset.events_20260408`;

-- events_20260409 (normal table)
INSERT INTO `quarantine-project.your_dataset.events_20260409` (id, event_type_id, event_type, description, last_modified)
SELECT
  GENERATE_UUID(),
  CAST(FLOOR(RAND()*5) AS INT64),
  CASE CAST(FLOOR(RAND()*5) AS INT64)
    WHEN 0 THEN 'login'
    WHEN 1 THEN 'logout'
    WHEN 2 THEN 'purchase'
    WHEN 3 THEN 'click'
    ELSE 'view'
  END,
  CONCAT('Event ', CAST(ROW_NUMBER() OVER() AS STRING)),
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
FROM UNNEST(GENERATE_ARRAY(1, 5));

-- events_20260410 (today)
INSERT INTO `quarantine-project.your_dataset.events_20260410` (id, event_type_id, event_type, description, last_modified)
SELECT
  GENERATE_UUID(),
  CAST(FLOOR(RAND()*5) AS INT64),
  CASE CAST(FLOOR(RAND()*5) AS INT64)
    WHEN 0 THEN 'login'
    WHEN 1 THEN 'logout'
    WHEN 2 THEN 'purchase'
    WHEN 3 THEN 'click'
    ELSE 'view'
  END,
  CONCAT('Today event ', CAST(ROW_NUMBER() OVER() AS STRING)),
  TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL CAST(FLOOR(RAND()*10) AS INT64) MINUTE)
FROM UNNEST(GENERATE_ARRAY(1, 5));

-- -----------------------------
-- 3. Update a row for merge testing (events_20260409)
-- -----------------------------
UPDATE `quarantine-project.your_dataset.events_20260409`
SET description = 'Updated description for testing'
WHERE id IN (SELECT id FROM `quarantine-project.your_dataset.events_20260409` LIMIT 1);

-- -----------------------------
-- 4. Add a new column for schema change testing (events_20260410)
-- -----------------------------
ALTER TABLE `quarantine-project.your_dataset.events_20260410`
ADD COLUMN extra_info STRING;

-- Populate extra_info with some data
UPDATE `quarantine-project.your_dataset.events_20260410`
SET extra_info = CONCAT('Extra_', CAST(FLOOR(RAND()*1000) AS STRING))
WHERE TRUE
LIMIT 3;

-- -----------------------------
-- 5. Insert old events to test timestamp filtering (events_20260410)
-- -----------------------------
INSERT INTO `quarantine-project.your_dataset.events_20260410` (id, event_type_id, event_type, description, last_modified)
VALUES
  (GENERATE_UUID(), 1, 'logout', 'Old logout', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY)),
  (GENERATE_UUID(), 2, 'purchase', 'Old purchase', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY));