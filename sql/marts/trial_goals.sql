-- trial_goals: data marts for Splendor trial event data

-- =============================================================================
-- Model:       trial_goals
-- Layer:       Marts
-- Description: Tracks whether each organisation completed each of the four
--              trial goals defined in the activation framework. Used by the
--              product team to monitor goal-level progress for every trialist.
-- Grain:       One row per organisation per goal (4 rows per organisation)
-- Dependencies: stg_events
-- =============================================================================

WITH

-- ── Base: one row per organisation with metadata ──────────────────────────────
org_meta AS (
    SELECT DISTINCT
        organization_id,
        converted,
        converted_at,
        trial_start,
        trial_end,
        trial_cohort_month,
        conversion_timing
    FROM stg_events
),

-- ── Goal 1: Schedule Published ────────────────────────────────────────────────
-- Definition: Organisation created at least one shift during their trial
goal_1 AS (
    SELECT
        organization_id,
        'goal_1_schedule_published'         AS goal_name,
        TRUE                                AS is_completed,
        MIN(event_timestamp)                AS completed_at
    FROM stg_events
    WHERE activity_name = 'Scheduling.Shift.Created'
    GROUP BY organization_id
),

-- ── Goal 2: Team Operational ──────────────────────────────────────────────────
-- Definition: At least one employee clocked in during the trial,
--             indicating the team is actively using the platform
goal_2 AS (
    SELECT
        organization_id,
        'goal_2_team_operational'           AS goal_name,
        TRUE                                AS is_completed,
        MIN(event_timestamp)                AS completed_at
    FROM stg_events
    WHERE activity_name = 'PunchClock.PunchedIn'
    GROUP BY organization_id
),

-- ── Goal 3: Management Decision Recorded ─────────────────────────────────────
-- Definition: Manager performed at least one deliberate approval or
--             absence decision through the platform
goal_3 AS (
    SELECT
        organization_id,
        'goal_3_management_decision'        AS goal_name,
        TRUE                                AS is_completed,
        MIN(event_timestamp)                AS completed_at
    FROM stg_events
    WHERE activity_name IN (
        'Scheduling.Shift.Approved',
        'Absence.Request.Approved',
        'Absence.Request.Rejected'
    )
    GROUP BY organization_id
),

-- ── Goal 4: Sustained Engagement ─────────────────────────────────────────────
-- Definition: Organisation was active on at least 3 distinct calendar days,
--             indicating recurring platform use beyond initial exploration
goal_4 AS (
    SELECT
        organization_id,
        'goal_4_sustained_engagement'       AS goal_name,
        TRUE                                AS is_completed,
        MIN(event_timestamp)                AS completed_at
    FROM (
        SELECT
            organization_id,
            event_timestamp,
            COUNT(DISTINCT DATE_TRUNC('day', event_timestamp))
                OVER (PARTITION BY organization_id) AS distinct_days
        FROM stg_events
    ) day_counts
    WHERE distinct_days >= 3
    GROUP BY organization_id
),

-- ── Combine all goals ─────────────────────────────────────────────────────────
all_goals AS (
    SELECT * FROM goal_1
    UNION ALL
    SELECT * FROM goal_2
    UNION ALL
    SELECT * FROM goal_3
    UNION ALL
    SELECT * FROM goal_4
),

-- ── Cross join orgs with goal names to create full long-format table ──────────
-- This ensures every organisation has exactly 4 rows regardless of completion
goal_spine AS (
    SELECT
        o.organization_id,
        g.goal_name
    FROM org_meta o
    CROSS JOIN (
        SELECT 'goal_1_schedule_published'  AS goal_name UNION ALL
        SELECT 'goal_2_team_operational'                 UNION ALL
        SELECT 'goal_3_management_decision'              UNION ALL
        SELECT 'goal_4_sustained_engagement'
    ) g
),

-- ── Join completions onto spine ───────────────────────────────────────────────
goals_with_completion AS (
    SELECT
        s.organization_id,
        s.goal_name,
        COALESCE(a.is_completed, FALSE)     AS is_completed,
        a.completed_at
    FROM goal_spine s
    LEFT JOIN all_goals a
        ON  s.organization_id = a.organization_id
        AND s.goal_name       = a.goal_name
)

-- ── Final Select ──────────────────────────────────────────────────────────────
SELECT
    g.organization_id,
    g.goal_name,
    g.is_completed,
    g.completed_at,
    -- Days into trial when goal was completed
    CASE
        WHEN g.completed_at IS NOT NULL
        THEN DATEDIFF('day',
            DATE_TRUNC('day', m.trial_start),
            DATE_TRUNC('day', g.completed_at)
        )
        ELSE NULL
    END                                     AS day_goal_completed,
    -- Organisation metadata
    m.converted,
    m.converted_at,
    m.trial_start,
    m.trial_end,
    m.trial_cohort_month,
    m.conversion_timing
FROM goals_with_completion g
LEFT JOIN org_meta m
    ON g.organization_id = m.organization_id
ORDER BY
    g.organization_id,
    g.goal_name
