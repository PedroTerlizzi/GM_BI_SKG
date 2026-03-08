/* ============================================================================
    GROWMETRICA FLOWWW BI - SQL STANDARD TEMPLATE

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
    CAST(
        CASE s.SeriesID
            WHEN 1 THEN CASE l.LabelID WHEN 2 THEN t.TotalInicial WHEN 3 THEN t.TotalFinal ELSE 0 END
            WHEN 2 THEN CASE l.LabelID WHEN 1 THEN t.Inicial325 WHEN 2 THEN t.Entradas325 WHEN 3 THEN ABS(t.Salidas325) WHEN 4 THEN t.Final325 ELSE 0 END
            WHEN 3 THEN CASE l.LabelID WHEN 1 THEN t.Inicial327 WHEN 2 THEN t.Entradas327 WHEN 3 THEN ABS(t.Salidas327) WHEN 4 THEN t.Final327 ELSE 0 END
            WHEN 4 THEN CASE l.LabelID WHEN 1 THEN t.Inicial326 WHEN 2 THEN t.Entradas326 WHEN 3 THEN ABS(t.Salidas326) WHEN 4 THEN t.Final326 ELSE 0 END
            WHEN 5 THEN CASE l.LabelID WHEN 1 THEN t.Inicial324 WHEN 2 THEN t.Entradas324 WHEN 3 THEN ABS(t.Salidas324) WHEN 4 THEN t.Final324 ELSE 0 END
            WHEN 6 THEN CASE l.LabelID WHEN 1 THEN t.Inicial323 WHEN 2 THEN t.Entradas323 WHEN 3 THEN ABS(t.Salidas323) WHEN 4 THEN t.Final323 ELSE 0 END
            ELSE 0
        END
    AS DECIMAL(18,2)) AS `Value1`,
    CAST(
        CASE s.SeriesID
            WHEN 1 THEN 0
            WHEN 2 THEN CASE l.LabelID WHEN 1 THEN t.CantInicial325 WHEN 2 THEN t.CantEntradas325 WHEN 3 THEN t.CantSalidas325 WHEN 4 THEN t.CantFinal325 ELSE 0 END
            WHEN 3 THEN CASE l.LabelID WHEN 1 THEN t.CantInicial327 WHEN 2 THEN t.CantEntradas327 WHEN 3 THEN t.CantSalidas327 WHEN 4 THEN t.CantFinal327 ELSE 0 END
            WHEN 4 THEN CASE l.LabelID WHEN 1 THEN t.CantInicial326 WHEN 2 THEN t.CantEntradas326 WHEN 3 THEN t.CantSalidas326 WHEN 4 THEN t.CantFinal326 ELSE 0 END
            WHEN 5 THEN CASE l.LabelID WHEN 1 THEN t.CantInicial324 WHEN 2 THEN t.CantEntradas324 WHEN 3 THEN t.CantSalidas324 WHEN 4 THEN t.CantFinal324 ELSE 0 END
            WHEN 6 THEN CASE l.LabelID WHEN 1 THEN t.CantInicial323 WHEN 2 THEN t.CantEntradas323 WHEN 3 THEN t.CantSalidas323 WHEN 4 THEN t.CantFinal323 ELSE 0 END
            ELSE 0
        END
    AS DECIMAL(18,2)) AS `Value2`
FROM (
    SELECT
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 325 THEN d.ValorInicial ELSE 0 END), 0) AS Inicial325,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 327 THEN d.ValorInicial ELSE 0 END), 0) AS Inicial327,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 326 THEN d.ValorInicial ELSE 0 END), 0) AS Inicial326,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 324 THEN d.ValorInicial ELSE 0 END), 0) AS Inicial324,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 323 THEN d.ValorInicial ELSE 0 END), 0) AS Inicial323,

        IFNULL(SUM(CASE WHEN d.FamilyParentID = 325 THEN d.ValorEntradas ELSE 0 END), 0) AS Entradas325,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 327 THEN d.ValorEntradas ELSE 0 END), 0) AS Entradas327,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 326 THEN d.ValorEntradas ELSE 0 END), 0) AS Entradas326,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 324 THEN d.ValorEntradas ELSE 0 END), 0) AS Entradas324,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 323 THEN d.ValorEntradas ELSE 0 END), 0) AS Entradas323,

        IFNULL(SUM(CASE WHEN d.FamilyParentID = 325 THEN d.ValorSalidas ELSE 0 END), 0) AS Salidas325,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 327 THEN d.ValorSalidas ELSE 0 END), 0) AS Salidas327,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 326 THEN d.ValorSalidas ELSE 0 END), 0) AS Salidas326,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 324 THEN d.ValorSalidas ELSE 0 END), 0) AS Salidas324,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 323 THEN d.ValorSalidas ELSE 0 END), 0) AS Salidas323,

        IFNULL(SUM(CASE WHEN d.FamilyParentID = 325 THEN d.ValorFinal ELSE 0 END), 0) AS Final325,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 327 THEN d.ValorFinal ELSE 0 END), 0) AS Final327,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 326 THEN d.ValorFinal ELSE 0 END), 0) AS Final326,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 324 THEN d.ValorFinal ELSE 0 END), 0) AS Final324,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 323 THEN d.ValorFinal ELSE 0 END), 0) AS Final323,

        IFNULL(SUM(CASE WHEN d.FamilyParentID = 325 THEN d.CantidadInicial ELSE 0 END), 0) AS CantInicial325,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 327 THEN d.CantidadInicial ELSE 0 END), 0) AS CantInicial327,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 326 THEN d.CantidadInicial ELSE 0 END), 0) AS CantInicial326,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 324 THEN d.CantidadInicial ELSE 0 END), 0) AS CantInicial324,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 323 THEN d.CantidadInicial ELSE 0 END), 0) AS CantInicial323,

        IFNULL(SUM(CASE WHEN d.FamilyParentID = 325 THEN (d.CantidadEntradasManuales + d.CantidadOtrasEntradas) ELSE 0 END), 0) AS CantEntradas325,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 327 THEN (d.CantidadEntradasManuales + d.CantidadOtrasEntradas) ELSE 0 END), 0) AS CantEntradas327,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 326 THEN (d.CantidadEntradasManuales + d.CantidadOtrasEntradas) ELSE 0 END), 0) AS CantEntradas326,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 324 THEN (d.CantidadEntradasManuales + d.CantidadOtrasEntradas) ELSE 0 END), 0) AS CantEntradas324,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 323 THEN (d.CantidadEntradasManuales + d.CantidadOtrasEntradas) ELSE 0 END), 0) AS CantEntradas323,

        IFNULL(SUM(CASE WHEN d.FamilyParentID = 325 THEN (d.CantidadSalidasVentas + d.CantidadOtrasSalidas) ELSE 0 END), 0) AS CantSalidas325,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 327 THEN (d.CantidadSalidasVentas + d.CantidadOtrasSalidas) ELSE 0 END), 0) AS CantSalidas327,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 326 THEN (d.CantidadSalidasVentas + d.CantidadOtrasSalidas) ELSE 0 END), 0) AS CantSalidas326,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 324 THEN (d.CantidadSalidasVentas + d.CantidadOtrasSalidas) ELSE 0 END), 0) AS CantSalidas324,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 323 THEN (d.CantidadSalidasVentas + d.CantidadOtrasSalidas) ELSE 0 END), 0) AS CantSalidas323,

        IFNULL(SUM(CASE WHEN d.FamilyParentID = 325 THEN d.CantidadFinal ELSE 0 END), 0) AS CantFinal325,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 327 THEN d.CantidadFinal ELSE 0 END), 0) AS CantFinal327,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 326 THEN d.CantidadFinal ELSE 0 END), 0) AS CantFinal326,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 324 THEN d.CantidadFinal ELSE 0 END), 0) AS CantFinal324,
        IFNULL(SUM(CASE WHEN d.FamilyParentID = 323 THEN d.CantidadFinal ELSE 0 END), 0) AS CantFinal323,

        IFNULL(SUM(d.ValorInicial), 0) AS TotalInicial,
        IFNULL(SUM(d.ValorEntradas), 0) AS TotalEntradas,
        IFNULL(SUM(d.ValorFinal), 0) AS TotalFinal
    FROM (
        SELECT
            b.FamilyParentID,
            (((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) * b.CostoUnitario) AS ValorInicial,
            ((b.QtyEntradasManuales + b.QtyOtrasEntradas) * b.CostoUnitario) AS ValorEntradas,
            ((b.QtySalidasVentas + b.QtyOtrasSalidas) * b.CostoUnitario) AS ValorSalidas,
            ((b.StockUnitsNow - b.QtyAfterEnd) * b.CostoUnitario) AS ValorFinal,
            (b.StockUnitsNow - b.QtyAfterEnd) AS CantidadFinal,
            ((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) AS CantidadInicial,
            b.QtyEntradasManuales AS CantidadEntradasManuales,
            b.QtyOtrasEntradas AS CantidadOtrasEntradas,
            b.QtySalidasVentas AS CantidadSalidasVentas,
            b.QtyOtrasSalidas AS CantidadOtrasSalidas
        FROM (
            SELECT
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
                IFNULL(fam_leaf.FamilyParentID, 0),
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
) t
CROSS JOIN (
    SELECT 1 AS LabelID, 'Inventario Inicial' AS LabelTitle
    UNION ALL SELECT 2, 'Entradas'
    UNION ALL SELECT 3, 'Salidas'
    UNION ALL SELECT 4, 'Inventario Final'
) l
CROSS JOIN (
    SELECT 1 AS SeriesID, 'Base' AS SeriesTitle
    UNION ALL SELECT 2, 'MP Prescripcion'
    UNION ALL SELECT 3, 'MP Libre'
    UNION ALL SELECT 4, 'PAT Controlado'
    UNION ALL SELECT 5, 'PAT Prescripcion'
    UNION ALL SELECT 6, 'PAT Libre'
) s
ORDER BY l.LabelID, s.SeriesID;
