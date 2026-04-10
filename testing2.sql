-- =========================================================
-- CONTEXT
-- Run this on: 2026-04-13
-- This generates D-1 to D-4 datasets for your DAG lookback
-- =========================================================


-- =========================================================
-- D-1 (2026-04-12)
-- FULL OVERWRITE TEST (direct_transfer path)
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260412` AS
SELECT TIMESTAMP("2026-04-12 10:00:00") AS event_time, "click" AS event_name, "A" AS value
UNION ALL
SELECT TIMESTAMP("2026-04-12 10:05:00"), "view", "B"
UNION ALL
SELECT TIMESTAMP("2026-04-12 10:10:00"), "scroll", "C";


-- =========================================================
-- D-2 (2026-04-11)
-- NORMAL + UPDATE SIMULATION
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260411` AS
SELECT TIMESTAMP("2026-04-11 09:00:00") AS event_time, "click" AS event_name, "A" AS value
UNION ALL
SELECT TIMESTAMP("2026-04-11 09:05:00"), "view", "B"
UNION ALL
SELECT TIMESTAMP("2026-04-11 09:00:00"), "click", "A_UPDATED";


-- =========================================================
-- D-3 (2026-04-10)
-- DUPLICATE EVENT_TIME TEST (MERGE collision stress)
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260410` AS
SELECT TIMESTAMP("2026-04-10 08:00:00") AS event_time, "click" AS event_name, "X1" AS value
UNION ALL
SELECT TIMESTAMP("2026-04-10 08:00:00"), "click", "X2"
UNION ALL
SELECT TIMESTAMP("2026-04-10 08:10:00"), "view", "Y";


-- =========================================================
-- D-4 (2026-04-09)
-- SCHEMA DRIFT + LATE UPDATE TEST
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260409` AS
SELECT
  TIMESTAMP("2026-04-09 07:00:00") AS event_time,
  "click" AS event_name,
  "OLD_VALUE" AS value,
  "EXTRA_FIELD_1" AS new_col
UNION ALL
SELECT
  TIMESTAMP("2026-04-09 07:00:00"),
  "click",
  "NEW_VALUE",
  "EXTRA_FIELD_1";


-- =========================================================
-- D-5 (2026-04-08)
-- NORMAL BASELINE LOAD
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260408` AS
SELECT TIMESTAMP("2026-04-08 06:00:00") AS event_time, "view" AS event_name, "A"
UNION ALL
SELECT TIMESTAMP("2026-04-08 06:10:00"), "click", "B";


-- =========================================================
-- D-6 (2026-04-07)
-- HEAVY DUPLICATE + UPDATE MIX (edge GA case)
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260407` AS
SELECT TIMESTAMP("2026-04-07 05:00:00") AS event_time, "click" AS event_name, "OLD"
UNION ALL
SELECT TIMESTAMP("2026-04-07 05:00:00"), "click", "UPDATED"
UNION ALL
SELECT TIMESTAMP("2026-04-07 05:05:00"), "view", "B";


-- =========================================================
-- D-7 (2026-04-06)
-- INTENTIONALLY SIMPLE CLEAN DATA
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260406` AS
SELECT TIMESTAMP("2026-04-06 04:00:00") AS event_time, "click" AS event_name, "A"
UNION ALL
SELECT TIMESTAMP("2026-04-06 04:10:00"), "view", "B";


-- =========================================================
-- INTENTIONAL GAP (BACKFILL TEST)
-- DO NOT CREATE THIS TABLE
-- =========================================================
-- events_20260405 (missing on purpose)


-- =========================================================
-- EXTRA EDGE CASE (optional stress)
-- MASS DUPLICATE EVENT_TIME BURST
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260404` AS
SELECT TIMESTAMP("2026-04-04 03:00:00") AS event_time, "click" AS event_name, "A1"
UNION ALL
SELECT TIMESTAMP("2026-04-04 03:00:00"), "click", "A2"
UNION ALL
SELECT TIMESTAMP("2026-04-04 03:00:00"), "click", "A3"
UNION ALL
SELECT TIMESTAMP("2026-04-04 03:05:00"), "view", "B";
