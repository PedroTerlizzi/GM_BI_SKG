/* ============================================================================
    GROWMETRICA FLOWWW BI - SQL STANDARD TEMPLATE
============================================================================ */

SELECT
    g.LabelID AS `LabelID`,
    g.LabelTitle AS `LabelTitle`,
    g.SeriesID AS `SeriesID`,
    g.SeriesTitle AS `SeriesTitle`,
    CAST(g.Value1 AS DECIMAL(18,2)) AS `Value1`,
    CAST(g.Value2 AS DECIMAL(18,2)) AS `Value2`
FROM (
    SELECT 1 AS LabelID, 'Inventario Inicial' AS LabelTitle, 1 AS SeriesID, 'Base' AS SeriesTitle, 0 AS Value1, 0 AS Value2
    UNION ALL
    SELECT 1, 'Inventario Inicial', 2, 'Flujo', a.InventarioInicial, a.InventarioInicial

    UNION ALL
    SELECT 2, 'Entradas', 1, 'Base', a.InventarioInicial, 0
    UNION ALL
    SELECT 2, 'Entradas', 2, 'Flujo', a.Entradas, a.Entradas

    UNION ALL
    SELECT 3, 'Salidas', 1, 'Base', a.InventarioFinal, 0
    UNION ALL
    SELECT 3, 'Salidas', 2, 'Flujo', a.Salidas, -a.Salidas

    UNION ALL
    SELECT 4, 'Inventario Final', 1, 'Base', 0, 0
    UNION ALL
    SELECT 4, 'Inventario Final', 2, 'Flujo', a.InventarioFinal, a.InventarioFinal
    FROM (
        SELECT
            IFNULL(SUM(d.ValorInicial), 0) AS InventarioInicial,
            IFNULL(SUM(d.ValorEntradas), 0) AS Entradas,
            IFNULL(SUM(d.ValorSalidas), 0) AS Salidas,
            IFNULL(SUM(d.ValorFinal), 0) AS InventarioFinal
        FROM (
            SELECT
                (((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) * b.CostoUnitario) AS ValorInicial,
                ((b.QtyEntradasManuales + b.QtyOtrasEntradas) * b.CostoUnitario) AS ValorEntradas,
                (ABS(b.QtySalidasVentas + b.QtyOtrasSalidas) * b.CostoUnitario) AS ValorSalidas,
                ((b.StockUnitsNow - b.QtyAfterEnd) * b.CostoUnitario) AS ValorFinal,
                (b.StockUnitsNow - b.QtyAfterEnd) AS CantidadFinal,
                ((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) AS CantidadInicial,
                b.QtyEntradasManuales AS CantidadEntradasManuales,
                b.QtyOtrasEntradas AS CantidadOtrasEntradas,
                b.QtySalidasVentas AS CantidadSalidasVentas,
                b.QtyOtrasSalidas AS CantidadOtrasSalidas
            FROM (
                SELECT
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
                        THEN sr.StockRAmount ELSE 0 END), 0) AS QtyAfterEnd
                FROM stock s

CROSS JOIN (
    SELECT
        t.*,
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
            '325,327,326,324,323'                 AS DebugFilter1,
            '0,1,2'                               AS DebugFilter2,
            '-1,0'                                AS DebugFilter3
    ) t
) p

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

                WHERE FIND_IN_SET(s.StockClinicID, p.EffectiveClinicIDs)
                  AND prod.ProductType = 3
                  AND (
                      p.EffectiveFilter1 IS NULL
                      OR p.EffectiveFilter1 = ''
                      OR FIND_IN_SET(fam_leaf.FamilyParentID, p.EffectiveFilter1)
                  )
                  AND (
                      p.EffectiveFilter2 IS NULL
                      OR p.EffectiveFilter2 = ''
                      OR FIND_IN_SET(IFNULL(prod.ProductProviderID, 0), p.EffectiveFilter2)
                  )
                  AND (
                      p.EffectiveFilter3 IS NULL
                      OR p.EffectiveFilter3 = ''
                      OR FIND_IN_SET(IFNULL(prod.ProductRequiresTraceability, 0), p.EffectiveFilter3)
                  )
                GROUP BY
                    IFNULL(s.StockUnits, 0),
                    IFNULL(s.StockAveragePrice, 0),
                    s.StockClinicID,
                    s.StockProductID,
                    IFNULL(s.StockProductLot, ''),
                    s.StockExpiryDate
            ) b
        ) d
        WHERE
            (
                ABS(IFNULL(d.CantidadInicial, 0))
                + ABS(IFNULL(d.CantidadEntradasManuales, 0))
                + ABS(IFNULL(d.CantidadOtrasEntradas, 0))
                + ABS(IFNULL(d.CantidadSalidasVentas, 0))
                + ABS(IFNULL(d.CantidadOtrasSalidas, 0))
                + ABS(IFNULL(d.CantidadFinal, 0))
            ) <> 0
    ) a
) g
ORDER BY g.LabelID, g.SeriesID;
