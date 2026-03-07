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
    - OUT columns:
        LabelID (INT), LabelTitle (TEXT), SeriesID (INT), SeriesTitle (TEXT),
        Value1 (DECIMAL), Value2 (NULLABLE DECIMAL)
============================================================================ */

/* [SAFE TO MODIFY] BLOCK 1 - OUTPUT SELECT */
SELECT
    lbl.LabelID AS `LabelID`,
    lbl.LabelTitle AS `LabelTitle`,
    ser.SeriesID AS `SeriesID`,
    ser.SeriesTitle AS `SeriesTitle`,
    CAST(
        CASE lbl.LabelID
            WHEN 1 THEN IFNULL(agg.ValorInicial, 0)
            WHEN 2 THEN -IFNULL(agg.ValorSalidas, 0)
            WHEN 3 THEN IFNULL(agg.ValorEntradas, 0)
            WHEN 4 THEN IFNULL(agg.ValorFinal, 0)
            ELSE 0
        END AS DECIMAL(18,2)
    ) AS `Value1`,
    CAST(
        CASE lbl.LabelID
            WHEN 1 THEN 0
            WHEN 2 THEN IFNULL(agg.ValorInicial, 0)
            WHEN 3 THEN IFNULL(agg.ValorInicial, 0) - IFNULL(agg.ValorSalidas, 0)
            WHEN 4 THEN 0
            ELSE 0
        END AS DECIMAL(18,2)
    ) AS `Value2`
FROM (
    SELECT 1 AS LabelID, 'Inventario Inicial' AS LabelTitle
    UNION ALL SELECT 2, 'Salidas'
    UNION ALL SELECT 3, 'Entradas'
    UNION ALL SELECT 4, 'Inventario Final'
) lbl
CROSS JOIN (
    SELECT 1 AS SeriesID, 325 AS FamilyParentID, 'MP Prescripcion' AS SeriesTitle
    UNION ALL SELECT 2, 327, 'MP Libre'
    UNION ALL SELECT 3, 326, 'PAT Controlado'
    UNION ALL SELECT 4, 324, 'PAT Prescripcion'
    UNION ALL SELECT 5, 323, 'PAT Libre'
) ser
LEFT JOIN (
    SELECT
        d.FamilyParentID,
        IFNULL(SUM(d.ValorInicial), 0) AS ValorInicial,
        IFNULL(SUM(d.ValorSalidas), 0) AS ValorSalidas,
        IFNULL(SUM(d.ValorEntradas), 0) AS ValorEntradas,
        IFNULL(SUM(d.ValorFinal), 0) AS ValorFinal
    FROM (
        SELECT
            c.*,
            CASE
                WHEN c.DiasInventarioVentasNum IS NULL AND c.DiasInventarioDemandaNum IS NULL THEN NULL
                WHEN c.DiasInventarioVentasNum IS NULL THEN c.DiasInventarioDemandaNum
                WHEN c.DiasInventarioDemandaNum IS NULL THEN c.DiasInventarioVentasNum
                ELSE LEAST(c.DiasInventarioVentasNum, c.DiasInventarioDemandaNum)
            END AS DiasInventarioPonderadoNum,
            CASE
                WHEN
                    CASE
                        WHEN c.DiasInventarioVentasNum IS NULL AND c.DiasInventarioDemandaNum IS NULL THEN NULL
                        WHEN c.DiasInventarioVentasNum IS NULL THEN c.DiasInventarioDemandaNum
                        WHEN c.DiasInventarioDemandaNum IS NULL THEN c.DiasInventarioVentasNum
                        ELSE LEAST(c.DiasInventarioVentasNum, c.DiasInventarioDemandaNum)
                    END IS NULL THEN 0
                WHEN
                    CASE
                        WHEN c.DiasInventarioVentasNum IS NULL AND c.DiasInventarioDemandaNum IS NULL THEN NULL
                        WHEN c.DiasInventarioVentasNum IS NULL THEN c.DiasInventarioDemandaNum
                        WHEN c.DiasInventarioDemandaNum IS NULL THEN c.DiasInventarioVentasNum
                        ELSE LEAST(c.DiasInventarioVentasNum, c.DiasInventarioDemandaNum)
                    END < 30 THEN 1
                WHEN
                    CASE
                        WHEN c.DiasInventarioVentasNum IS NULL AND c.DiasInventarioDemandaNum IS NULL THEN NULL
                        WHEN c.DiasInventarioVentasNum IS NULL THEN c.DiasInventarioDemandaNum
                        WHEN c.DiasInventarioDemandaNum IS NULL THEN c.DiasInventarioVentasNum
                        ELSE LEAST(c.DiasInventarioVentasNum, c.DiasInventarioDemandaNum)
                    END <= 60 THEN 2
                WHEN
                    CASE
                        WHEN c.DiasInventarioVentasNum IS NULL AND c.DiasInventarioDemandaNum IS NULL THEN NULL
                        WHEN c.DiasInventarioVentasNum IS NULL THEN c.DiasInventarioDemandaNum
                        WHEN c.DiasInventarioDemandaNum IS NULL THEN c.DiasInventarioVentasNum
                        ELSE LEAST(c.DiasInventarioVentasNum, c.DiasInventarioDemandaNum)
                    END <= 120 THEN 3
                WHEN
                    CASE
                        WHEN c.DiasInventarioVentasNum IS NULL AND c.DiasInventarioDemandaNum IS NULL THEN NULL
                        WHEN c.DiasInventarioVentasNum IS NULL THEN c.DiasInventarioDemandaNum
                        WHEN c.DiasInventarioDemandaNum IS NULL THEN c.DiasInventarioVentasNum
                        ELSE LEAST(c.DiasInventarioVentasNum, c.DiasInventarioDemandaNum)
                    END <= 180 THEN 4
                ELSE 5
            END AS ClasificacionID
        FROM (
            SELECT
                b.*,
                DATEDIFF(b.EffectiveEndDate, b.EffectiveStartDate) AS PeriodDays,
                (b.StockUnitsNow - b.QtyAfterEnd) AS CantidadFinal,
                ((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) AS CantidadInicial,
                (((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) * b.CostoUnitario) AS ValorInicial,
                ((b.QtyEntradasManuales + b.QtyOtrasEntradas) * b.CostoUnitario) AS ValorEntradas,
                (ABS(b.QtySalidasVentas + b.QtyOtrasSalidas) * b.CostoUnitario) AS ValorSalidas,
                ((b.StockUnitsNow - b.QtyAfterEnd) * b.CostoUnitario) AS ValorFinal,
                (
                    ((((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) * b.CostoUnitario) +
                    (((b.StockUnitsNow - b.QtyAfterEnd) * b.CostoUnitario))) / 2
                ) AS InventarioPromedio,
                (ABS(b.QtySalidasVentas) * b.CostoUnitario) AS CostoVentas,
                (b.QtyRecetada * b.CostoUnitario) AS ValorRecetado,
                CASE
                    WHEN DATEDIFF(b.EffectiveEndDate, b.EffectiveStartDate) <= 0
                         OR (ABS(b.QtySalidasVentas) * b.CostoUnitario) = 0 THEN NULL
                    ELSE (
                        (
                            ((((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) * b.CostoUnitario) +
                            (((b.StockUnitsNow - b.QtyAfterEnd) * b.CostoUnitario))) / 2
                        ) * DATEDIFF(b.EffectiveEndDate, b.EffectiveStartDate)
                    ) / (ABS(b.QtySalidasVentas) * b.CostoUnitario)
                END AS DiasInventarioVentasNum,
                CASE
                    WHEN DATEDIFF(b.EffectiveEndDate, b.EffectiveStartDate) <= 0
                         OR (b.QtyRecetada * b.CostoUnitario) = 0 THEN NULL
                    ELSE (
                        (
                            ((((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) * b.CostoUnitario) +
                            (((b.StockUnitsNow - b.QtyAfterEnd) * b.CostoUnitario))) / 2
                        ) * DATEDIFF(b.EffectiveEndDate, b.EffectiveStartDate)
                    ) / (b.QtyRecetada * b.CostoUnitario)
                END AS DiasInventarioDemandaNum
            FROM (
                SELECT
                    p.EffectiveStartDate,
                    p.EffectiveEndDate,
                    p.EffectiveClinicIDs,
                    p.EffectiveFilter1,
                    p.EffectiveFilter2,
                    p.EffectiveFilter3,
                    IFNULL(fam_leaf.FamilyParentID, 0) AS FamilyParentID,
                    IFNULL(s.StockUnits, 0) AS StockUnitsNow,
                    IFNULL(s.StockAveragePrice, 0) AS CostoUnitario,
                    IFNULL(SUM(CASE
                        WHEN sr.StockRDate >= p.EffectiveStartDate
                         AND sr.StockRDate < DATE_ADD(p.EffectiveEndDate, INTERVAL 1 DAY)
                        THEN sr.StockRAmount ELSE 0 END), 0) AS QtyMovPeriodo,
                    IFNULL(SUM(CASE
                        WHEN sr.StockRDate >= p.EffectiveStartDate
                         AND sr.StockRDate < DATE_ADD(p.EffectiveEndDate, INTERVAL 1 DAY)
                         AND sr.StockRAmount > 0
                         AND IFNULL(sr.StockRClass, '') = 'M'
                        THEN sr.StockRAmount ELSE 0 END), 0) AS QtyEntradasManuales,
                    IFNULL(SUM(CASE
                        WHEN sr.StockRDate >= p.EffectiveStartDate
                         AND sr.StockRDate < DATE_ADD(p.EffectiveEndDate, INTERVAL 1 DAY)
                         AND sr.StockRAmount > 0
                         AND IFNULL(sr.StockRClass, '') <> 'M'
                        THEN sr.StockRAmount ELSE 0 END), 0) AS QtyOtrasEntradas,
                    IFNULL(SUM(CASE
                        WHEN sr.StockRDate >= p.EffectiveStartDate
                         AND sr.StockRDate < DATE_ADD(p.EffectiveEndDate, INTERVAL 1 DAY)
                         AND sr.StockRAmount < 0
                         AND IFNULL(sr.StockRClass, '') = 'S'
                        THEN sr.StockRAmount ELSE 0 END), 0) AS QtySalidasVentas,
                    IFNULL(SUM(CASE
                        WHEN sr.StockRDate >= p.EffectiveStartDate
                         AND sr.StockRDate < DATE_ADD(p.EffectiveEndDate, INTERVAL 1 DAY)
                         AND sr.StockRAmount < 0
                         AND IFNULL(sr.StockRClass, '') <> 'S'
                        THEN sr.StockRAmount ELSE 0 END), 0) AS QtyOtrasSalidas,
                    IFNULL(SUM(CASE
                        WHEN sr.StockRDate > p.EffectiveEndDate
                         AND sr.StockRDate < DATE_ADD(CURDATE(), INTERVAL 1 DAY)
                        THEN sr.StockRAmount ELSE 0 END), 0) AS QtyAfterEnd,
                    IFNULL((
                        SELECT SUM(IFNULL(bd.BudgetUnits, 0))
                        FROM budgets_gen bg
                        INNER JOIN budgets_det bd
                            ON bd.BudgetGID = bg.BudgetGID
                        WHERE bg.BudgetGClinicID = s.StockClinicID
                          AND bd.BudgetProductID = s.StockProductID
                          AND bg.BudgetGDate >= p.EffectiveStartDate
                          AND bg.BudgetGDate < DATE_ADD(p.EffectiveEndDate, INTERVAL 1 DAY)
                          AND bg.BudgetGDeleted = 0
                    ), 0) AS QtyRecetada
                FROM stock s

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
            '1,2,3,4,5'                           AS DebugFilter1,
            '325,327,326,324,323'                 AS DebugFilter2,
            '0,1,2'                               AS DebugFilter3

    ) t
) p

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
LEFT JOIN x_config_products_det prod
    ON prod.ProductID = s.StockProductID

LEFT JOIN x_config_products_fam fam_leaf
    ON fam_leaf.FamilyID = prod.ProductFamilyID

LEFT JOIN stock_registry sr
    ON sr.StockRClinicID = s.StockClinicID
   AND sr.StockRProductID = s.StockProductID
   AND IFNULL(sr.StockRProductLot, '') = IFNULL(s.StockProductLot, '')
   AND (
        (sr.StockRExpiryDate IS NULL AND s.StockExpiryDate IS NULL)
        OR DATE(sr.StockRExpiryDate) = DATE(s.StockExpiryDate)
   )

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
                WHERE FIND_IN_SET(s.StockClinicID, p.EffectiveClinicIDs)
                  AND prod.ProductType = 3
                  AND (
                      p.EffectiveFilter2 IS NULL
                      OR p.EffectiveFilter2 = ''
                      OR FIND_IN_SET(fam_leaf.FamilyParentID, p.EffectiveFilter2)
                  )
                  AND (
                      p.EffectiveFilter3 IS NULL
                      OR p.EffectiveFilter3 = ''
                      OR FIND_IN_SET(IFNULL(prod.ProductProviderID, 0), p.EffectiveFilter3)
                  )
                GROUP BY
                    p.EffectiveStartDate,
                    p.EffectiveEndDate,
                    p.EffectiveClinicIDs,
                    p.EffectiveFilter1,
                    p.EffectiveFilter2,
                    p.EffectiveFilter3,
                    IFNULL(fam_leaf.FamilyParentID, 0),
                    IFNULL(s.StockUnits, 0),
                    IFNULL(s.StockAveragePrice, 0),
                    s.StockClinicID,
                    s.StockProductID,
                    IFNULL(s.StockProductLot, ''),
                    s.StockExpiryDate
            ) b
        ) c
    ) d
    WHERE
        (
            ABS(IFNULL(d.CantidadInicial, 0))
            + ABS(IFNULL(d.QtyEntradasManuales, 0))
            + ABS(IFNULL(d.QtyOtrasEntradas, 0))
            + ABS(IFNULL(d.QtySalidasVentas, 0))
            + ABS(IFNULL(d.QtyOtrasSalidas, 0))
            + ABS(IFNULL(d.CantidadFinal, 0))
        ) <> 0
        AND (
            d.EffectiveFilter1 IS NULL
            OR d.EffectiveFilter1 = ''
            OR FIND_IN_SET(d.ClasificacionID, d.EffectiveFilter1)
        )
    GROUP BY
        d.FamilyParentID
) agg
    ON agg.FamilyParentID = ser.FamilyParentID

/* ORDER apply here */
ORDER BY
    lbl.LabelID,
    ser.SeriesID;
