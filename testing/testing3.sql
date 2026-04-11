-- =====================================================
-- DATASETS (SAFE CREATE)
-- =====================================================

CREATE SCHEMA IF NOT EXISTS `ac-project-485203.analytics_514446807`;
CREATE SCHEMA IF NOT EXISTS `team-485203.analytics_514446807`;

-- =====================================================
-- SOURCE TABLES (GA RAW STYLE)
-- Running assumption: DAG executed on 2026-04-13
-- =====================================================

-- D-1 → 20260412
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260412` AS
SELECT TIMESTAMP('2026-04-12 10:00:00') AS event_time
UNION ALL SELECT TIMESTAMP('2026-04-12 11:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-12 12:00:00');

-- D-2 → 20260411
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260411` AS
SELECT TIMESTAMP('2026-04-11 09:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-11 10:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-11 11:00:00');

-- D-3 → 20260410
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260410` AS
SELECT TIMESTAMP('2026-04-10 08:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-10 09:00:00');

-- D-4 → 20260409
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260409` AS
SELECT TIMESTAMP('2026-04-09 07:00:00');

-- =====================================================
-- SIMULATE LATE ARRIVING DATA (IMPORTANT FOR TESTING)
-- modifies D-2 after initial creation
-- =====================================================

CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260411` AS
SELECT TIMESTAMP('2026-04-11 09:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-11 10:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-11 11:00:00')
UNION ALL SELECT TIMESTAMP('2026-04-11 12:30:00');  -- late event

-- =====================================================
-- SCHEMA DRIFT TEST (D-3)
-- =====================================================

ALTER TABLE `ac-project-485203.analytics_514446807.events_20260410`
ADD COLUMN user_id STRING;

UPDATE `ac-project-485203.analytics_514446807.events_20260410`
SET user_id = 'test_user'
WHERE TRUE;

-- =====================================================
-- OPTIONAL TARGET PRE-EXISTENCE TEST
-- (simulate partial sync already happened)
-- =====================================================

CREATE OR REPLACE TABLE `team-485203.analytics_514446807.events_20260412` AS
SELECT TIMESTAMP('2026-04-12 10:00:00') AS event_time;
