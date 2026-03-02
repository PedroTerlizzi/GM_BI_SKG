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
    IFNULL(cl.ClinicCommercialName, '-')                      AS `Sucursal`,
    DATE(COALESCE(bg.BudgetGStamp, bg.BudgetGDate))           AS `Fecha`,
    TIME(COALESCE(bg.BudgetGStamp, bg.BudgetGDate))           AS `Hora`,
    IFNULL(
        NULLIF(
            CONCAT(IFNULL(bg.BudgetGYear, ''), '/', IFNULL(bg.BudgetGNumber, '')),
            '/'
        ),
        '-'
    )                                                         AS `Prescripcion`,
    IFNULL(
        NULLIF(
            TRIM(CONCAT_WS(' ', c.ClientName, c.ClientSurname1, c.ClientSurname2)),
            ''
        ),
        '-'
    )                                                         AS `Paciente`,
    IFNULL(prod.ProductDesc, '-')                             AS `Producto Recetado`,
    GREATEST(
        CAST(
            IFNULL(
                (
                    SELECT sr.StockRUnits
                    FROM stock_registry sr
                    WHERE sr.StockRProductID = bd.BudgetProductID
                      AND sr.StockRClinicID = bg.BudgetGClinicID
                      AND TIMESTAMP(sr.StockRDate, sr.StockRTime) < COALESCE(bg.BudgetGStamp, bg.BudgetGDate)
                    ORDER BY sr.StockRDate DESC, sr.StockRTime DESC
                    LIMIT 1
                ),
                0
            ) AS SIGNED
        ),
        0
    )                                                         AS `Disponibilidad en Farmacia`,
    IFNULL(
        (
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
        ),
        0
    )                                                         AS `Cantidad Comprada`
FROM budgets_gen bg

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
            255                                 AS DebugUserID,
            '1,2,3,4,5,6,12,8,7,13,9,10'        AS DebugClinicIDs,
            DATE_SUB(CURDATE(), INTERVAL 180 DAY) AS DebugStartDate,
            CURDATE()                           AS DebugEndDate,
            1                                   AS DebugFilter1,
            1                                   AS DebugFilter2,
            1                                   AS DebugFilter3

    ) t
) p

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
INNER JOIN budgets_det bd
    ON bd.BudgetGID = bg.BudgetGID

LEFT JOIN x_config_products_det prod
    ON prod.ProductID = bd.BudgetProductID

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

/* ORDER apply here */
ORDER BY
    `Fecha` DESC,
    `Hora` DESC,
    bg.BudgetGID,
    bd.BudgetProductID;
