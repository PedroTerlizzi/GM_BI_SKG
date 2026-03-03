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
    IFNULL(NULLIF(CONCAT(IFNULL(bg.BudgetGYear, ''), '/', IFNULL(bg.BudgetGNumber, '')),'/'),'-') AS `Prescripción`,
    DATE(COALESCE(bg.BudgetGStamp, bg.BudgetGDate)) AS `Fecha`,
    TIME_FORMAT(COALESCE(bg.BudgetGStamp, bg.BudgetGDate), '%H:%i:%s') AS `Hora`,
    IFNULL(cl.ClinicCommercialName, '-') AS `Sucursal`,
    IFNULL(NULLIF(TRIM(CONCAT_WS(' ', c.ClientName, c.ClientSurname1, c.ClientSurname2)),''),'-') AS `Paciente`,
    IFNULL(fam.FamilyName, '-') AS `Família`,
    IFNULL(prod.ProductDesc, '-') AS `Producto Recetado`,
    GREATEST(CAST(IFNULL((
        SELECT sr.StockRUnits
        FROM stock_registry sr
        WHERE sr.StockRProductID = bd.BudgetProductID
            AND sr.StockRClinicID = bg.BudgetGClinicID
            AND TIMESTAMP(sr.StockRDate, sr.StockRTime) < COALESCE(bg.BudgetGStamp, bg.BudgetGDate)
        ORDER BY sr.StockRDate DESC, sr.StockRTime DESC
        LIMIT 1),0) AS SIGNED),0) AS `Disponibilidad`,
    IFNULL((
        SELECT SUM(td.TicketUnits)
        FROM tickets_gen tg
        INNER JOIN tickets_det td
            ON td.TicketGID = tg.TicketGID
        WHERE tg.TicketGClientID = bg.BudgetGClientID
            AND tg.TicketGDate = bg.BudgetGDate
            AND td.TicketProductID = bd.BudgetProductID
            AND tg.TicketGClosed = -1
            AND tg.TicketGErased = 0
            AND tg.TicketGCancelled = 0
        ),0) AS `Comprado`
FROM budgets_gen bg

/* [DO NOT MODIFY] BLOCK 2 - PARAM RESOLUTION (t + p) */
CROSS JOIN (
    SELECT
        t.*,

        /* Debug switch */
        (t.EngineClinicIDs IS NULL OR t.EngineClinicIDs = '') AS UseDebug,

        /* Effective parameter resolution */
        IF(t.EngineUserID IS NULL, t.DebugUserID, t.EngineUserID) AS EffectiveUserID,
        IF(t.EngineClinicIDs = '', t.DebugClinicIDs, t.EngineClinicIDs) AS EffectiveClinicIDs,
        IF(t.EngineStartDate IS NULL, t.DebugStartDate, t.EngineStartDate) AS EffectiveStartDate,
        IF(t.EngineEndDate IS NULL, t.DebugEndDate, t.EngineEndDate) AS EffectiveEndDate,
        IF(t.EngineClinicIDs = '', t.DebugFilter1, t.EngineFilter1CSV) AS EffectiveFilter1,
        IF(t.EngineClinicIDs = '', t.DebugFilter2, t.EngineFilter2CSV) AS EffectiveFilter2,
        IF(t.EngineClinicIDs = '', t.DebugFilter3, t.EngineFilter3CSV) AS EffectiveFilter3

    FROM (
        SELECT

        /* Production placeholders (replaced by ASP engine) */
            NULL AS EngineUserID,
            ''   AS EngineClinicIDs,
            NULL AS EngineStartDate,
            NULL AS EngineEndDate,
            NULL AS EngineFilter1CSV,
            NULL AS EngineFilter2CSV,
            NULL AS EngineFilter3CSV,

        /* Debug defaults (used automatically in local execution) */
            255                                   AS DebugUserID,
            '1,2,3,4,5,6,12,8,7,13,9,10'          AS DebugClinicIDs,
            DATE_SUB(CURDATE(), INTERVAL 180 DAY) AS DebugStartDate,
            CURDATE()                             AS DebugEndDate,
            '325,327,326,324,323'                 AS DebugFilter1,
            '0,1,2'                               AS DebugFilter2,
            '-1,0'                                AS DebugFilter3

    ) t
) p

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
INNER JOIN budgets_det bd
    ON bd.BudgetGID = bg.BudgetGID

LEFT JOIN x_config_products_det prod
    ON prod.ProductID = bd.BudgetProductID

LEFT JOIN x_config_products_fam fam
    ON fam.FamilyID = prod.ProductFamilyID

LEFT JOIN x_config_clinics cl
    ON cl.ClinicID = bg.BudgetGClinicID

LEFT JOIN clients c
    ON c.ClientID = bg.BudgetGClientID

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
WHERE bg.BudgetGUserID = p.EffectiveUserID
  AND bg.BudgetGPrescription = -1
  AND bg.BudgetGDeleted = 0
  AND FIND_IN_SET(bg.BudgetGClinicID, p.EffectiveClinicIDs)
  AND bg.BudgetGDate >= p.EffectiveStartDate
  AND bg.BudgetGDate < DATE_ADD(p.EffectiveEndDate, INTERVAL 1 DAY)
  AND prod.ProductType = 3
  AND (
      p.EffectiveFilter1 IS NULL
      OR p.EffectiveFilter1 = ''
      OR FIND_IN_SET(fam.FamilyParentID, p.EffectiveFilter1)
  )
  AND (
      p.EffectiveFilter2 IS NULL
      OR p.EffectiveFilter2 = ''
      OR FIND_IN_SET(prod.ProductProviderID, p.EffectiveFilter2)
  )
  AND (
      p.EffectiveFilter3 IS NULL
      OR p.EffectiveFilter3 = ''
      OR FIND_IN_SET(prod.ProductRequiresTraceability, p.EffectiveFilter3)
  )

/* ORDER apply here */
ORDER BY
    `Fecha` DESC,
    `Hora` DESC,
    bg.BudgetGID,
    bd.BudgetProductID;
