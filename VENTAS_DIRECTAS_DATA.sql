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
    IFNULL(tg.TicketGNumber, '-') AS `Ticket`,
    DATE(tg.TicketGDate) AS `Fecha`,
    TIME_FORMAT(tg.TicketGTime, '%H:%i:%s') AS `Hora`,
    IFNULL(cl.ClinicCommercialName, '-') AS `Sucursal`,
    IFNULL(NULLIF(TRIM(CONCAT_WS(' ', c.ClientName, c.ClientSurname1, c.ClientSurname2)), ''), '-') AS `Paciente`,
    IFNULL(fam_parent.FamilyName, 'Otras') AS `Familia`,
    IFNULL(prod_line.ProductDesc, '-') AS `Descripcion Producto/Servicio`,
    CASE
        WHEN IFNULL(td.TicketLaserGID, 0) <> 0 THEN 'Servicios en sesion'
        WHEN IFNULL(tg.TicketGBudgetID, 0) <> 0 THEN 'Conversion receta (inmediata)'
        ELSE 'Origen no registrada'
    END AS `Origen`,
    CASE
        WHEN IFNULL(td.TicketLaserGID, 0) <> 0 THEN CONCAT('LaserGID ', td.TicketLaserGID)
        WHEN IFNULL(tg.TicketGBudgetID, 0) <> 0 THEN CONCAT('BudgetID ', tg.TicketGBudgetID)
        ELSE '-'
    END AS `Trazabilidad`,
    CASE
        WHEN IFNULL(prod_line.ProductPrice, 0) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(prod_line.ProductPrice, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(prod_line.ProductPrice, 0), 2))
    END AS `Tarifa Actual`,
    CONCAT(
        FORMAT(
            CASE
                WHEN ABS(IFNULL(prod_line.ProductVAT, 0)) <= 1 THEN IFNULL(prod_line.ProductVAT, 0) * 100
                ELSE IFNULL(prod_line.ProductVAT, 0)
            END,
            0
        ),
        '%'
    ) AS `IVA`,
    CAST(ROUND(IFNULL(td.TicketUnits, 0), 0) AS SIGNED) AS `Cantidad`,
    CASE
        WHEN IFNULL(td.TicketPriceVAT, 0) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(td.TicketPriceVAT, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(td.TicketPriceVAT, 0), 2))
    END AS `Precio Unitario`,
    CASE
        WHEN (IFNULL(td.TicketUnits, 0) * IFNULL(td.TicketDiscountUnitAmountVAT, 0)) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(td.TicketUnits, 0) * IFNULL(td.TicketDiscountUnitAmountVAT, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(td.TicketUnits, 0) * IFNULL(td.TicketDiscountUnitAmountVAT, 0), 2))
    END AS `Descuento`,
    CASE
        WHEN IFNULL(td.TicketTotalAmount, 0) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(td.TicketTotalAmount, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(td.TicketTotalAmount, 0), 2))
    END AS `Precio Final en Ticket`
FROM tickets_det td

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
            '1,2,3,4,5,6,7,9,10,11,12,323,324,325,326,327,999' AS DebugFilter1,
            '1,2,3'                               AS DebugFilter2,
            ''                                    AS DebugFilter3

    ) t
) p

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
INNER JOIN tickets_gen tg
    ON tg.TicketGID = td.TicketGID

LEFT JOIN x_config_clinics cl
    ON cl.ClinicID = tg.TicketGClinicID

LEFT JOIN clients c
    ON c.ClientID = tg.TicketGClientID

LEFT JOIN x_config_products_det prod_line
    ON prod_line.ProductID = td.TicketProductID

LEFT JOIN x_config_products_det prod_parent
    ON prod_parent.ProductID = CASE
        WHEN IFNULL(prod_line.ProductParentID, 0) > 0 THEN prod_line.ProductParentID
        ELSE prod_line.ProductID
    END

LEFT JOIN x_config_products_fam fam_leaf
    ON fam_leaf.FamilyID = prod_parent.ProductFamilyID

LEFT JOIN x_config_products_fam fam_parent
    ON fam_parent.FamilyID = fam_leaf.FamilyParentID

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
WHERE IFNULL(td.TicketUnits, 0) <> 0
  AND tg.TicketGErased = 0
  AND tg.TicketGCancelled = 0
  AND tg.TicketGSimulated = 0
  AND tg.TicketGClosed = -1
  AND tg.TicketGUserID = p.EffectiveUserID
  AND FIND_IN_SET(tg.TicketGClinicID, p.EffectiveClinicIDs)
  AND tg.TicketGDate >= p.EffectiveStartDate
  AND tg.TicketGDate < DATE_ADD(p.EffectiveEndDate, INTERVAL 1 DAY)
  AND (
      p.EffectiveFilter1 IS NULL
      OR p.EffectiveFilter1 = ''
      OR (
          (IFNULL(fam_parent.FamilyID, 0) IN (1,2,3,4,5,6,7,9,10,11,12,323,324,325,326,327)
           AND FIND_IN_SET(fam_parent.FamilyID, p.EffectiveFilter1))
          OR
          ((IFNULL(fam_parent.FamilyID, 0) = 0
            OR NOT FIND_IN_SET(IFNULL(fam_parent.FamilyID, 0), '1,2,3,4,5,6,7,9,10,11,12,323,324,325,326,327'))
           AND FIND_IN_SET(999, p.EffectiveFilter1))
      )
  )
  AND (
      p.EffectiveFilter2 IS NULL
      OR p.EffectiveFilter2 = ''
      OR FIND_IN_SET(
            CASE
                WHEN IFNULL(td.TicketLaserGID, 0) <> 0 THEN 1
                WHEN IFNULL(tg.TicketGBudgetID, 0) <> 0 THEN 2
                ELSE 3
            END,
            p.EffectiveFilter2
        )
  )

/* ORDER apply here */
ORDER BY
    tg.TicketGDate DESC,
    tg.TicketGTime DESC,
    tg.TicketGID DESC,
    td.TicketID DESC;
