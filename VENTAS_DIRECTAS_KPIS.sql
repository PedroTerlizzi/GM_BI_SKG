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
============================================================================ */

SELECT
    k.KPIID AS `KPIID`,
    k.KPIName AS `KPIName`,
    CONCAT(
        CASE
            WHEN (
                CASE k.KPIID
                    WHEN 1 THEN IFNULL(a.TotalVentas, 0)
                    WHEN 2 THEN IFNULL(a.VentasServicioSesion, 0)
                    WHEN 3 THEN IFNULL(a.VentasConversionReceta, 0)
                    WHEN 4 THEN IFNULL(a.VentasOrigenNoRegistrada, 0)
                    WHEN 5 THEN IFNULL(a.TotalDescuentos, 0)
                    ELSE 0
                END
            ) < 0 THEN '-$'
            ELSE '$'
        END,
        FORMAT(
            ABS(
                CASE k.KPIID
                    WHEN 1 THEN IFNULL(a.TotalVentas, 0)
                    WHEN 2 THEN IFNULL(a.VentasServicioSesion, 0)
                    WHEN 3 THEN IFNULL(a.VentasConversionReceta, 0)
                    WHEN 4 THEN IFNULL(a.VentasOrigenNoRegistrada, 0)
                    WHEN 5 THEN IFNULL(a.TotalDescuentos, 0)
                    ELSE 0
                END
            ),
            0
        )
    ) AS `KPIValue`,
    k.KPIScheme AS `KPIScheme`
FROM (
    SELECT
        IFNULL(SUM(td.TicketTotalAmount), 0) AS TotalVentas,
        IFNULL(SUM(CASE WHEN IFNULL(td.TicketLaserGID, 0) <> 0 THEN td.TicketTotalAmount ELSE 0 END), 0) AS VentasServicioSesion,
        IFNULL(SUM(CASE WHEN IFNULL(td.TicketLaserGID, 0) = 0 AND IFNULL(tg.TicketGBudgetID, 0) <> 0 THEN td.TicketTotalAmount ELSE 0 END), 0) AS VentasConversionReceta,
        IFNULL(SUM(CASE WHEN IFNULL(td.TicketLaserGID, 0) = 0 AND IFNULL(tg.TicketGBudgetID, 0) = 0 THEN td.TicketTotalAmount ELSE 0 END), 0) AS VentasOrigenNoRegistrada,
        IFNULL(SUM(IFNULL(td.TicketUnits, 0) * IFNULL(td.TicketDiscountUnitAmountVAT, 0)), 0) AS TotalDescuentos
    FROM tickets_det td

/* [DO NOT MODIFY] BLOCK 2 - PARAM RESOLUTION (t + p) */
CROSS JOIN (
    SELECT
        t.*,
        (t.EngineClinicIDs IS NULL OR t.EngineClinicIDs = '') AS UseDebug,
        IF(t.EngineUserID IS NULL, t.DebugUserID, t.EngineUserID) AS EffectiveUserID,
        IF(t.EngineClinicIDs = '', t.DebugClinicIDs, t.EngineClinicIDs) AS EffectiveClinicIDs,
        IF(t.EngineStartDate IS NULL, t.DebugStartDate, t.EngineStartDate) AS EffectiveStartDate,
        IF(t.EngineEndDate IS NULL, t.DebugEndDate, t.EngineEndDate) AS EffectiveEndDate,
        IF(t.EngineClinicIDs = '', t.DebugFilter1, t.EngineFilter1CSV) AS EffectiveFilter1,
        IF(t.EngineClinicIDs = '', t.DebugFilter2, t.EngineFilter2CSV) AS EffectiveFilter2,
        IF(t.EngineClinicIDs = '', t.DebugFilter3, t.EngineFilter3CSV) AS EffectiveFilter3
    FROM (
        SELECT
            NULL AS EngineUserID,
            ''   AS EngineClinicIDs,
            NULL AS EngineStartDate,
            NULL AS EngineEndDate,
            NULL AS EngineFilter1CSV,
            NULL AS EngineFilter2CSV,
            NULL AS EngineFilter3CSV,
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
) a
CROSS JOIN (
    SELECT 1 AS KPIID, 'Total Ventas' AS KPIName, TRUE AS KPIScheme
    UNION ALL SELECT 2, 'Servicios en sesion', TRUE
    UNION ALL SELECT 3, 'Conversion receta (inmediata)', TRUE
    UNION ALL SELECT 4, 'Origen no registrada', TRUE
    UNION ALL SELECT 5, 'Total Descuentos', FALSE
) k
ORDER BY k.KPIID;
