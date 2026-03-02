/* ============================================================================
    GROWMETRICA FLOWWW BI - SQL STANDARD TEMPLATE

    EXECUTION ENVIRONMENT
    - Runtime target: Flowww MySQL 5.7.
    - Query execution context: Growmetrica BI via Classic ASP
    - Engine constraints (Single statement):
        * No CREATE, no TEMPORARY TABLE
        * No CTE, no WITH
        * No window functions
        * No multi-statement execution
    - Design implication: query must remain single-statement and MySQL 5.7-safe.

    EXECUTION MODES
    - Production (Classic ASP): Engine* values are injected before execution.
    - Debug (Heidi/local): EngineClinicIDs stays empty and Debug* values are used.

    OUTPUT CONTRACT
    - Output must always be one generic renderable table.
    - Headers must be SQL output column names/aliases.
    - Avoid non-tabular outputs or multiple result sets.

    CORE RULE
    - Business logic must only use p.Effective* fields.
    - NEVER USE Engine* OR Debug* OUT IN BUSINESS LOGIC // OUT OF PARAM BLOCK.

    BLOCK POLICY
    - [SAFE TO MODIFY]   Output SELECT block
    - [DO NOT MODIFY]    Param Resolution block (nested t + p)
    - [SAFE TO MODIFY]   Business joins / filters / calculations
============================================================================ */

/* [SAFE TO MODIFY] BLOCK 1 - OUTPUT SELECT */
SELECT
    IFNULL(u.UserName, '-')                                  AS `Nombre`,
    IFNULL(u.UserEmail, '-')                                 AS `Email`,
    IFNULL(u.UserPhone1, '-')                                AS `Telefono`,
    IFNULL(
        DATE_FORMAT(u.UserBirthDate, '%d-%b'),
        '-'
    )                                                        AS `Cumpleanos`,
    IFNULL(
        NULLIF(
            GROUP_CONCAT(DISTINCT uc.UserClassDesc ORDER BY uc.UserClassDesc SEPARATOR ', '),
            ''
        ),
        '-'
    )                                                        AS `Funcion`
FROM __x_config_users_view u

/* [DO NOT MODIFY] BLOCK 2 - PARAM RESOLUTION (t + p) */
CROSS JOIN (
    SELECT
        t.*,

        /* Debug switch */
        (t.EngineClinicIDs IS NULL OR t.EngineClinicIDs = '') AS UseDebug,

        /* Effective parameter resolution */
        IF(t.EngineUserID IS NULL,    t.DebugUserID, t.EngineUserID)       AS EffectiveUserID,
        IF(t.EngineClinicIDs = '',    t.DebugClinicIDs, t.EngineClinicIDs) AS EffectiveClinicIDs,
        IF(t.EngineStartDate IS NULL, t.DebugStartDate, t.EngineStartDate) AS EffectiveStartDate,
        IF(t.EngineEndDate IS NULL,   t.DebugEndDate, t.EngineEndDate)     AS EffectiveEndDate,
        IF(t.EngineFilter1 IS NULL,   t.DebugFilter1, t.EngineFilter1)     AS EffectiveFilter1,
        IF(t.EngineFilter2 IS NULL,   t.DebugFilter2, t.EngineFilter2)     AS EffectiveFilter2,
        IF(t.EngineFilter3 IS NULL,   t.DebugFilter3, t.EngineFilter3)     AS EffectiveFilter3

    FROM (
        SELECT

        /* Production placeholders (replaced by ASP engine) */
            NULL AS EngineUserID,
            ''   AS EngineClinicIDs,
            NULL AS EngineStartDate,
            NULL AS EngineEndDate,
            NULL AS EngineFilter1,
            NULL AS EngineFilter2,
            NULL AS EngineFilter3,

        /* Debug defaults (used automatically in local execution) */
            231                                 AS DebugUserID,
            '1,2,3,4,5,6,12,8,7,13,9,10'        AS DebugClinicIDs,
            DATE_SUB(CURDATE(), INTERVAL 7 DAY) AS DebugStartDate,
            CURDATE()                           AS DebugEndDate,
            1                                   AS DebugFilter1,
            1                                   AS DebugFilter2,
            1                                   AS DebugFilter3

    ) t
) p

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
LEFT JOIN __x_config_workplaces_view w
    ON w.WorkPlaceUserID = u.UserID
    AND (
        w.WorkPlaceClinicID = 0
        OR FIND_IN_SET(w.WorkPlaceClinicID, p.EffectiveClinicIDs)
    )

LEFT JOIN x_config_userclasses uc
    ON uc.UserClassID = CAST(w.WorkPlaceUserClassID AS UNSIGNED)

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
WHERE u.UserDisabled NOT IN (-1)
/* AND u.UserID = p.EffectiveUserID */
/* AND u.SomeDate BETWEEN p.EffectiveStartDate AND p.EffectiveEndDate */
/* AND u.SomeField = p.EffectiveFilter1 */

/* Collapse 1:N workplaces to one row per user */
GROUP BY
    u.UserID,
    u.UserName,
    u.UserEmail,
    u.UserPhone1,
    u.UserBirthDate

/* ORDER apply here */
ORDER BY u.UserName;
