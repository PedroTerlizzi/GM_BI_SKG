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

    OUTPUT CONTRACT
    - OUT columns:
        LabelID (INT), LabelTitle (TEXT), SeriesID (INT), SeriesTitle (TEXT),
        Value1 (DECIMAL), Value2 (NULLABLE DECIMAL)
============================================================================ */

SELECT
    l.LabelID AS `LabelID`,
    l.LabelTitle AS `LabelTitle`,
    s.SeriesID AS `SeriesID`,
    s.SeriesTitle AS `SeriesTitle`,
    CAST(IFNULL(a.TotalMonto, 0) AS DECIMAL(18,2)) AS `Value1`,
    CAST(IFNULL(a.TotalUnidades, 0) AS DECIMAL(18,0)) AS `Value2`
FROM (
    SELECT 1 AS LabelID, 'Previo (30-45d)' AS LabelTitle
    UNION ALL SELECT 2, 'Anterior (15-30d)'
    UNION ALL SELECT 3, 'Ultimos 15d'
) l
CROSS JOIN (
    SELECT 1 AS SeriesID, 'Servicio en sesion' AS SeriesTitle
    UNION ALL SELECT 2, 'Conversion receta (inmediata)'
    UNION ALL SELECT 3, 'Origen no registrada'
) s
LEFT JOIN (
    SELECT
        CASE
            WHEN tg.TicketGDate >= DATE_SUB(CURDATE(), INTERVAL 44 DAY)
             AND tg.TicketGDate < DATE_SUB(CURDATE(), INTERVAL 29 DAY) THEN 1
            WHEN tg.TicketGDate >= DATE_SUB(CURDATE(), INTERVAL 29 DAY)
             AND tg.TicketGDate < DATE_SUB(CURDATE(), INTERVAL 14 DAY) THEN 2
            WHEN tg.TicketGDate >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
             AND tg.TicketGDate < DATE_ADD(CURDATE(), INTERVAL 1 DAY) THEN 3
            ELSE 0
        END AS LabelID,
        CASE
            WHEN IFNULL(td.TicketLaserGID, 0) <> 0 THEN 1
            WHEN IFNULL(tg.TicketGBudgetID, 0) <> 0 OR IFNULL(bmatch.MatchedBudgetID, 0) <> 0 THEN 2
            ELSE 3
        END AS SeriesID,
        IFNULL(SUM(td.TicketTotalAmount), 0) AS TotalMonto,
        IFNULL(SUM(td.TicketUnits), 0) AS TotalUnidades
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

LEFT JOIN (
    SELECT
        bg.BudgetGClinicID,
        bg.BudgetGUserID,
        bg.BudgetGClientID,
        bg.BudgetGDate,
        bd.BudgetProductID,
        MIN(bg.BudgetGID) AS MatchedBudgetID
    FROM budgets_gen bg
    INNER JOIN budgets_det bd
        ON bd.BudgetGID = bg.BudgetGID
    GROUP BY
        bg.BudgetGClinicID,
        bg.BudgetGUserID,
        bg.BudgetGClientID,
        bg.BudgetGDate,
        bd.BudgetProductID
) bmatch
    ON bmatch.BudgetGClinicID = tg.TicketGClinicID
   AND bmatch.BudgetGUserID = tg.TicketGUserID
   AND bmatch.BudgetGClientID = tg.TicketGClientID
   AND bmatch.BudgetGDate = tg.TicketGDate
   AND bmatch.BudgetProductID = td.TicketProductID

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
WHERE IFNULL(td.TicketUnits, 0) <> 0
  AND tg.TicketGErased = 0
  AND tg.TicketGCancelled = 0
  AND tg.TicketGSimulated = 0
  AND tg.TicketGClosed = -1
  AND tg.TicketGUserID = p.EffectiveUserID
  AND FIND_IN_SET(tg.TicketGClinicID, p.EffectiveClinicIDs)
  AND tg.TicketGDate >= DATE_SUB(CURDATE(), INTERVAL 44 DAY)
  AND tg.TicketGDate < DATE_ADD(CURDATE(), INTERVAL 1 DAY)
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
                WHEN IFNULL(tg.TicketGBudgetID, 0) <> 0 OR IFNULL(bmatch.MatchedBudgetID, 0) <> 0 THEN 2
                ELSE 3
            END,
            p.EffectiveFilter2
        )
  )
GROUP BY
    CASE
        WHEN tg.TicketGDate >= DATE_SUB(CURDATE(), INTERVAL 44 DAY)
         AND tg.TicketGDate < DATE_SUB(CURDATE(), INTERVAL 29 DAY) THEN 1
        WHEN tg.TicketGDate >= DATE_SUB(CURDATE(), INTERVAL 29 DAY)
         AND tg.TicketGDate < DATE_SUB(CURDATE(), INTERVAL 14 DAY) THEN 2
        WHEN tg.TicketGDate >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
         AND tg.TicketGDate < DATE_ADD(CURDATE(), INTERVAL 1 DAY) THEN 3
        ELSE 0
    END,
    CASE
        WHEN IFNULL(td.TicketLaserGID, 0) <> 0 THEN 1
        WHEN IFNULL(tg.TicketGBudgetID, 0) <> 0 OR IFNULL(bmatch.MatchedBudgetID, 0) <> 0 THEN 2
        ELSE 3
    END
) a
    ON a.LabelID = l.LabelID
   AND a.SeriesID = s.SeriesID
ORDER BY
    l.LabelID,
    s.SeriesID;
