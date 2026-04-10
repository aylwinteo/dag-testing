-- =========================================================
-- FULL TEST SETUP FOR DAG RUN ON 2026-04-13
-- LOOKBACK: 4 DAYS (D-1 to D-4)
-- =========================================================


-- =========================================================
-- D-1 → 2026-04-12 (FULL OVERWRITE)
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260412` AS
SELECT TIMESTAMP("2026-04-12 10:00:00") AS event_time, "click" AS event_name, "A" AS value
UNION ALL
SELECT TIMESTAMP("2026-04-12 10:05:00"), "view", "B"
UNION ALL
SELECT TIMESTAMP("2026-04-12 10:10:00"), "scroll", "C";


-- =========================================================
-- D-2 → 2026-04-11 (NORMAL + LATE UPDATE)
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260411` AS
SELECT TIMESTAMP("2026-04-11 09:00:00") AS event_time, "click" AS event_name, "A"
UNION ALL
SELECT TIMESTAMP("2026-04-11 09:05:00"), "view", "B"
UNION ALL
SELECT TIMESTAMP("2026-04-11 09:00:00"), "click", "A_UPDATED";


-- =========================================================
-- D-3 → 2026-04-10 (DUPLICATE COLLISION TEST)
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260410` AS
SELECT TIMESTAMP("2026-04-10 08:00:00") AS event_time, "click" AS event_name, "X1"
UNION ALL
SELECT TIMESTAMP("2026-04-10 08:00:00"), "click", "X2"
UNION ALL
SELECT TIMESTAMP("2026-04-10 08:10:00"), "view", "Y";


-- =========================================================
-- D-4 → 2026-04-09 (SCHEMA DRIFT + UPDATE)
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260409` AS
SELECT
  TIMESTAMP("2026-04-09 07:00:00") AS event_time,
  "click" AS event_name,
  "OLD_VALUE" AS value,
  "NEW_COL" AS extra_field
UNION ALL
SELECT
  TIMESTAMP("2026-04-09 07:00:00"),
  "click",
  "NEW_VALUE",
  "NEW_COL";


-- =========================================================
-- EXTRA BACKFILL TEST (OLDER DAY WITH DATA)
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260408` AS
SELECT TIMESTAMP("2026-04-08 06:00:00") AS event_time, "view", "A"
UNION ALL
SELECT TIMESTAMP("2026-04-08 06:10:00"), "click", "B";


-- =========================================================
-- EXTRA DUPLICATE BURST TEST
-- =========================================================
CREATE OR REPLACE TABLE `ac-project-485203.analytics_514446807.events_20260407` AS
SELECT TIMESTAMP("2026-04-07 05:00:00") AS event_time, "click", "OLD"
UNION ALL
SELECT TIMESTAMP("2026-04-07 05:00:00"), "click", "UPDATED"
UNION ALL
SELECT TIMESTAMP("2026-04-07 05:00:00"), "click", "UPDATED_2";


-- =========================================================
-- MISSING DAY TEST (DO NOT CREATE THIS)
-- =========================================================
-- events_20260406 intentionally missing


-- =========================================================
-- FINAL NOTE
-- =========================================================
-- After running this script:
-- 1. Trigger your DAG on 2026-04-13
-- 2. Observe logs:
--    - D-1 → direct_transfer
--    - D-2 → merge
--    - D-3 → merge (duplicate handling)
--    - D-4 → merge + schema drift
--    - Missing day → skipped / backfill handled
