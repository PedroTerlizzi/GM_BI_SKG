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
    - [DO NOT MODIFY]    Corporate Order block (corp)
    - [SAFE TO MODIFY]   Business filters / joins / calculations
============================================================================ */

/* [SAFE TO MODIFY] BLOCK 1 - OUTPUT SELECT */
SELECT
    IFNULL(c.ClinicCommercialName, '-') AS `Sucursal`,
    IFNULL(c.ClinicAddress, '-')        AS `Direccion`,
    IFNULL(c.ClinicCity, '-')           AS `Ciudad`,
    IFNULL(c.ClinicPostCode, '-')       AS `Codigo Postal`,
    IFNULL(c.ClinicPhone, '-')          AS `Tel.Fijo`,
    IFNULL(c.ClinicPhone2, '-')         AS `Tel.WA`,
    IFNULL(c.ClinicNotes, '-')          AS `RRSS`,
    IFNULL(c.ClinicEMail, '-')          AS `Email`,
    IFNULL(c.ClinicCIF, '-')            AS `RFC`,
    IFNULL(c.ClinicTimeStart, '-')      AS `Apertura L-V`,
    IFNULL(c.ClinicTimeEnd, '-')        AS `Cierre L-V`,
    CASE WHEN c.ClinicSAT = -1 THEN 'Si'
        WHEN c.ClinicSAT = 0 THEN 'No'
        ELSE '-' END                    AS `Abre Sabado`,
    CASE WHEN c.ClinicSUN = -1 THEN 'Si'
        WHEN c.ClinicSUN = 0 THEN 'No'
        ELSE '-' END                    AS `Abre Domingo`,
    IFNULL(c.ClinicSATTimeStart, '-')   AS `Apertura S/D`,
    IFNULL(c.ClinicSATTimeEnd, '-')     AS `Cierre S/D`,
    CASE WHEN c.ClinicFWAEnabled = -1 THEN 'Si'
        WHEN c.ClinicFWAEnabled = 0 THEN 'No'
        ELSE '-' END                    AS `App`
FROM x_config_clinics c

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

/* [DO NOT MODIFY] BLOCK 3 - CORPORATE ORDER MAP */
INNER JOIN (
    SELECT 1 AS ClinicID, 1 AS CorpPos UNION ALL
    SELECT 2, 2 UNION ALL
    SELECT 3, 3 UNION ALL
    SELECT 4, 4 UNION ALL
    SELECT 5, 5 UNION ALL
    SELECT 6, 6 UNION ALL
    SELECT 12,7 UNION ALL
    SELECT 8, 8 UNION ALL
    SELECT 7, 9 UNION ALL
    SELECT 13,10 UNION ALL
    SELECT 9, 11 UNION ALL
    SELECT 10,12
) corp ON c.ClinicID = corp.ClinicID

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
WHERE FIND_IN_SET(c.ClinicID, p.EffectiveClinicIDs)
/* AND c.SomeUserID = p.EffectiveUserID */
/* AND c.SomeDate BETWEEN p.EffectiveStartDate AND p.EffectiveEndDate */
/* AND c.SomeField = p.EffectiveFilter1 */

/* ORDER apply here, keeping corp.CorpPos first */
ORDER BY corp.CorpPos;