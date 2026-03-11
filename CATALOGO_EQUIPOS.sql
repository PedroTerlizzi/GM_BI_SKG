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
    s.Equipo AS `Equipo`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 1  THEN s.SucursalValor END), 'N/D') AS `SKG Tamazunchale`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 2  THEN s.SucursalValor END), 'N/D') AS `SKG Amazonas`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 3  THEN s.SucursalValor END), 'N/D') AS `SKG Punto Aura`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 4  THEN s.SucursalValor END), 'N/D') AS `SKG Cumbres`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 5  THEN s.SucursalValor END), 'N/D') AS `SKG Chihuahua`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 6  THEN s.SucursalValor END), 'N/D') AS `SKG Juriquilla`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 12 THEN s.SucursalValor END), 'N/D') AS `SKG Campa`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 8  THEN s.SucursalValor END), 'N/D') AS `SKG Coapa`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 7  THEN s.SucursalValor END), 'N/D') AS `SKG Aguilas`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 13 THEN s.SucursalValor END), 'N/D') AS `PD San Nico #1`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 9  THEN s.SucursalValor END), 'N/D') AS `PD San Nico #2`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 10 THEN s.SucursalValor END), 'N/D') AS `PD Miguel Aleman`
FROM (
    SELECT
        e.EquipmentEquipmentID AS EquipmentID,
        IFNULL(eq.EquipmentName, CONCAT('Equipo ', e.EquipmentEquipmentID)) AS Equipo,
        e.EquipmentClinicID AS ClinicID,
        GROUP_CONCAT(
            CONCAT(
                IFNULL(NULLIF(TRIM(e.EquipmentCode), ''), '-'),
                ', SN ',
                IFNULL(NULLIF(TRIM(e.EquipmentSN), ''), '-')
            )
            ORDER BY
                IFNULL(NULLIF(TRIM(e.EquipmentCode), ''), '-'),
                IFNULL(NULLIF(TRIM(e.EquipmentSN), ''), '-')
            SEPARATOR '\n'
        ) AS SucursalValor
    FROM equipments e

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
            '1,2,3,4,5,6,12,8,7,13,9,10'       AS DebugClinicIDs,
            DATE_SUB(CURDATE(), INTERVAL 7 DAY) AS DebugStartDate,
            CURDATE()                           AS DebugEndDate,
            1                                   AS DebugFilter1,
            1                                   AS DebugFilter2,
            1                                   AS DebugFilter3

    ) t
) p

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
LEFT JOIN x_config_equipment eq
    ON eq.EquipmentID = e.EquipmentEquipmentID

LEFT JOIN x_config_clinics c
    ON c.ClinicID = e.EquipmentClinicID

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
    WHERE IFNULL(e.EquipmentDisabled, 0) = 0
      AND FIND_IN_SET(e.EquipmentClinicID, p.EffectiveClinicIDs)
    GROUP BY
        e.EquipmentEquipmentID,
        IFNULL(eq.EquipmentName, CONCAT('Equipo ', e.EquipmentEquipmentID)),
        e.EquipmentClinicID
) s
GROUP BY
    s.EquipmentID,
    s.Equipo

/* ORDER apply here */
ORDER BY
    s.Equipo;
