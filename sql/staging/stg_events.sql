-- stg_events: staging layer for Splendor trial event data

-- =============================================================================
-- Model:       stg_events
-- Layer:       Staging
-- Description: Cleans and enriches the raw trial event data. All downstream
--              mart models depend on this staging layer. No mart model should
--              ever read directly from the raw source.
-- Grain:       One row per event (organisation + activity + timestamp)
-- Dependencies: raw_events
-- =============================================================================

WITH

-- ── Step 1: Cast all columns to correct data types ───────────────────────────
typed AS (
    SELECT
        organization_id,
        activity_name,
        CAST(timestamp   AS TIMESTAMP) AS event_timestamp,
        CAST(trial_start AS TIMESTAMP) AS trial_start,
        CAST(trial_end   AS TIMESTAMP) AS trial_end,
        converted,
        TRY_CAST(converted_at AS TIMESTAMP) AS converted_at
    FROM raw_events
    WHERE organization_id IS NOT NULL
      AND activity_name   IS NOT NULL
      AND timestamp       IS NOT NULL
),

-- ── Step 2: Calculate derived time fields ────────────────────────────────────
with_time_fields AS (
    SELECT
        *,
        DATEDIFF('day',
            DATE_TRUNC('day', trial_start),
            DATE_TRUNC('day', event_timestamp)
        ) AS day_of_trial,
        CASE
            WHEN converted_at IS NOT NULL
            THEN DATEDIFF('day',
                DATE_TRUNC('day', trial_end),
                DATE_TRUNC('day', converted_at)
            )
            ELSE NULL
        END AS days_after_trial_end
    FROM typed
),

-- ── Step 3: Filter to valid trial window ─────────────────────────────────────
within_window AS (
    SELECT *
    FROM with_time_fields
    WHERE day_of_trial BETWEEN 0 AND 30
),

-- ── Step 4: Assign activity buckets ──────────────────────────────────────────
with_buckets AS (
    SELECT
        *,
        CASE activity_name
            WHEN 'Scheduling.Shift.Created'                THEN 1
            WHEN 'Scheduling.Availability.Set'             THEN 1
            WHEN 'Scheduling.Template.ApplyModal.Applied'  THEN 1
            WHEN 'Mobile.Schedule.Loaded'                  THEN 1
            WHEN 'Shift.View.Opened'                       THEN 1
            WHEN 'ShiftDetails.View.Opened'                THEN 1
            WHEN 'Scheduling.ShiftSwap.Created'            THEN 2
            WHEN 'Scheduling.ShiftSwap.Accepted'           THEN 2
            WHEN 'Scheduling.ShiftHandover.Created'        THEN 2
            WHEN 'Scheduling.ShiftHandover.Accepted'       THEN 2
            WHEN 'Scheduling.OpenShiftRequest.Created'     THEN 2
            WHEN 'Scheduling.OpenShiftRequest.Approved'    THEN 2
            WHEN 'Scheduling.ShiftAssignmentChanged'       THEN 2
            WHEN 'PunchClock.PunchedIn'                    THEN 2
            WHEN 'PunchClock.PunchedOut'                   THEN 2
            WHEN 'Break.Activate.Started'                  THEN 2
            WHEN 'Break.Activate.Finished'                 THEN 2
            WHEN 'PunchClockStartNote.Add.Completed'       THEN 2
            WHEN 'PunchClockEndNote.Add.Completed'         THEN 2
            WHEN 'PunchClock.Entry.Edited'                 THEN 2
            WHEN 'Absence.Request.Created'                 THEN 2
            WHEN 'Absence.Request.Approved'                THEN 2
            WHEN 'Absence.Request.Rejected'                THEN 2
            WHEN 'Scheduling.Shift.Approved'               THEN 3
            WHEN 'Timesheets.BulkApprove.Confirmed'        THEN 3
            WHEN 'Integration.Xero.PayrollExport.Synced'   THEN 3
            WHEN 'Revenue.Budgets.Created'                 THEN 3
            WHEN 'Communication.Message.Created'           THEN 3
            ELSE NULL
        END AS activity_bucket,
        CASE
            WHEN NOT converted
            THEN 'not_converted'
            WHEN days_after_trial_end <= 0
            THEN 'within_trial'
            WHEN days_after_trial_end BETWEEN 1 AND 7
            THEN 'post_trial_short'
            ELSE 'post_trial_long'
        END AS conversion_timing
    FROM within_window
),

-- ── Step 5: Remove same-second duplicates for instrumentation events ──────────
deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY organization_id, activity_name, event_timestamp
                ORDER BY event_timestamp
            ) AS row_num
        FROM with_buckets
        WHERE activity_name IN (
            'PunchClock.PunchedIn',
            'Break.Activate.Started',
            'Break.Activate.Finished',
            'Scheduling.Availability.Set',
            'PunchClock.Entry.Edited',
            'Communication.Message.Created',
            'Mobile.Schedule.Loaded',
            'ShiftDetails.View.Opened',
            'Scheduling.Shift.Approved'
        )
    )
    WHERE row_num = 1

    UNION ALL

    SELECT *, 1 AS row_num
    FROM with_buckets
    WHERE activity_name NOT IN (
        'PunchClock.PunchedIn',
        'Break.Activate.Started',
        'Break.Activate.Finished',
        'Scheduling.Availability.Set',
        'PunchClock.Entry.Edited',
        'Communication.Message.Created',
        'Mobile.Schedule.Loaded',
        'ShiftDetails.View.Opened',
        'Scheduling.Shift.Approved'
    )
)

-- ── Final Select ──────────────────────────────────────────────────────────────
SELECT
    organization_id,
    activity_name,
    activity_bucket,
    event_timestamp,
    day_of_trial,
    converted,
    converted_at,
    converted_at IS NOT NULL               AS has_converted_at,
    days_after_trial_end,
    conversion_timing,
    trial_start,
    trial_end,
    DATE_TRUNC('day', trial_start)         AS trial_start_date,
    DATE_TRUNC('day', trial_end)           AS trial_end_date,
    DATE_TRUNC('month', trial_start)       AS trial_cohort_month
FROM deduplicated
