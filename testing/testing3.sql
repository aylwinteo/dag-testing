-- =====================================================
-- DATASETS
-- =====================================================

CREATE SCHEMA IF NOT EXISTS `ac-project-485203.analytics_514446807`;
CREATE SCHEMA IF NOT EXISTS `team-485203.analytics_514446807`;

-- =====================================================
-- CLEAN SLATE (OPTIONAL RESET FOR TEST RUN)
-- =====================================================

DROP TABLE IF EXISTS `ac-project-485203.analytics_514446807.events_20260412`;
DROP TABLE IF EXISTS `ac-project-485203.analytics_514446807.events_20260411`;
DROP TABLE IF EXISTS `ac-project-485203.analytics_514446807.events_20260410`;
DROP TABLE IF EXISTS `ac-project-485203.analytics_514446807.events_20260409`;
DROP TABLE IF EXISTS `ac-project-485203.analytics_514446807.events_20260408`;

DROP TABLE IF EXISTS `team-485203.analytics_514446807.events_20260412`;

-- =====================================================
-- NORMAL GA RAW DATA (D-1 TO D-4)
-- =====================================================

CREATE TABLE `ac-project-485203.analytics_514446807.events_20260412` AS
SELECT TIMESTAMP('2026-04-12 10:00:00') AS event_time
UNION ALL SELECT TIMESTAMP('2026-04-12 11:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-12 12:00:00');

CREATE TABLE `ac-project-485203.analytics_514446807.events_20260411` AS
SELECT TIMESTAMP('2026-04-11 09:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-11 10:00:00');

CREATE TABLE `ac-project-485203.analytics_514446807.events_20260410` AS
SELECT TIMESTAMP('2026-04-10 08:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-10 09:00:00');

CREATE TABLE `ac-project-485203.analytics_514446807.events_20260409` AS
SELECT TIMESTAMP('2026-04-09 07:00:00');

-- =====================================================
-- MISSING DAY TEST (CRITICAL EDGE CASE)
-- events_20260408 is intentionally NOT created
-- =====================================================

-- (no table created here on purpose)

-- =====================================================
-- LATE ARRIVING DATA SIMULATION (D-2 UPDATE)
-- =====================================================

CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260411` AS
SELECT TIMESTAMP('2026-04-11 09:00:00') AS event_time
UNION ALL SELECT TIMESTAMP('2026-04-11 10:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-11 11:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-11 12:30:00');  -- late correction

-- =====================================================
-- SCHEMA DRIFT TEST (D-3)
-- =====================================================

ALTER TABLE `ac-project-485203.analytics_514446807.events_20260410`
ADD COLUMN user_id STRING;

UPDATE `ac-project-485203.analytics_514446807.events_20260410`
SET user_id = 'user_test'
WHERE TRUE;

-- =====================================================
-- PARTIAL TARGET SIMULATION (D-1 already synced earlier)
-- =====================================================

CREATE OR REPLACE TABLE `team-485203.analytics_514446807.events_20260412` AS
SELECT TIMESTAMP('2026-04-12 10:00:00') AS event_time;

-- =====================================================
-- EXTRA STRESS TEST (OPTIONAL BURST DATA)
-- =====================================================

CREATE TABLE `ac-project-485203.analytics_514446807.events_20260413` AS
SELECT TIMESTAMP('2026-04-13 00:00:01') AS event_time
UNION ALL SELECT TIMESTAMP('2026-04-13 00:10:00')
UNION ALL SELECT TIMESTAMP('2026-04-13 00:20:00');
