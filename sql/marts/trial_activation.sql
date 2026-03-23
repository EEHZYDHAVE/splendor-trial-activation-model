-- trial_activation.sql: data marts for Splendor trial event data

-- =============================================================================
-- Model:       trial_activation
-- Layer:       Marts
-- Description: Summary table tracking whether each organisation achieved
--              full trial activation by completing all four trial goals.
--              This is the primary business-facing model for monitoring
--              trial health and conversion outcomes.
-- Grain:       One row per organisation
-- Dependencies: trial_goals, stg_events
-- =============================================================================

WITH

-- ── Goal completion summary per organisation ──────────────────────────────────
goal_summary AS (
    SELECT
        organization_id,
        COUNT(*)                                            AS total_goals,
        SUM(CASE WHEN is_completed THEN 1 ELSE 0 END)      AS goals_completed,
        -- Activation date is when the last goal was completed
        MAX(completed_at)                                   AS activation_date,
        -- Individual goal flags for easy querying
        MAX(CASE WHEN goal_name = 'goal_1_schedule_published'
            AND is_completed THEN 1 ELSE 0 END)            AS goal_1_completed,
        MAX(CASE WHEN goal_name = 'goal_2_team_operational'
            AND is_completed THEN 1 ELSE 0 END)            AS goal_2_completed,
        MAX(CASE WHEN goal_name = 'goal_3_management_decision'
            AND is_completed THEN 1 ELSE 0 END)            AS goal_3_completed,
        MAX(CASE WHEN goal_name = 'goal_4_sustained_engagement'
            AND is_completed THEN 1 ELSE 0 END)            AS goal_4_completed,
        -- Day each goal was completed
        MAX(CASE WHEN goal_name = 'goal_1_schedule_published'
            THEN day_goal_completed END)                    AS goal_1_day,
        MAX(CASE WHEN goal_name = 'goal_2_team_operational'
            THEN day_goal_completed END)                    AS goal_2_day,
        MAX(CASE WHEN goal_name = 'goal_3_management_decision'
            THEN day_goal_completed END)                    AS goal_3_day,
        MAX(CASE WHEN goal_name = 'goal_4_sustained_engagement'
            THEN day_goal_completed END)                    AS goal_4_day
    FROM trial_goals
    GROUP BY organization_id
),

-- ── Organisation metadata from staging ───────────────────────────────────────
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

-- ── Combine and derive activation flag ───────────────────────────────────────
activation AS (
    SELECT
        m.organization_id,
        m.trial_start,
        m.trial_end,
        m.trial_cohort_month,
        m.converted,
        m.converted_at,
        m.conversion_timing,
        g.total_goals,
        g.goals_completed,
        -- Activated when all 4 goals are completed
        CASE WHEN g.goals_completed = g.total_goals
            THEN TRUE ELSE FALSE
        END                                                 AS is_activated,
        g.activation_date,
        -- Days into trial when activation was achieved
        CASE
            WHEN g.goals_completed = g.total_goals
            THEN DATEDIFF('day',
                DATE_TRUNC('day', m.trial_start),
                DATE_TRUNC('day', g.activation_date)
            )
            ELSE NULL
        END                                                 AS day_activated,
        -- Individual goal flags
        g.goal_1_completed,
        g.goal_2_completed,
        g.goal_3_completed,
        g.goal_4_completed,
        -- Individual goal completion days
        g.goal_1_day,
        g.goal_2_day,
        g.goal_3_day,
        g.goal_4_day
    FROM org_meta m
    LEFT JOIN goal_summary g
        ON m.organization_id = g.organization_id
)

-- ── Final Select ──────────────────────────────────────────────────────────────
SELECT
    organization_id,
    trial_start,
    trial_end,
    trial_cohort_month,
    converted,
    converted_at,
    conversion_timing,
    is_activated,
    activation_date,
    day_activated,
    goals_completed,
    total_goals,
    goal_1_completed,
    goal_2_completed,
    goal_3_completed,
    goal_4_completed,
    goal_1_day,
    goal_2_day,
    goal_3_day,
    goal_4_day
FROM activation
ORDER BY organization_id
