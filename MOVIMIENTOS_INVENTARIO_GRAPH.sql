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
            WHEN 1 THEN IFNULL(agg.Monto01, 0)
            WHEN 2 THEN IFNULL(agg.Monto02, 0)
            WHEN 3 THEN IFNULL(agg.Monto03, 0)
            WHEN 4 THEN IFNULL(agg.Monto04, 0)
            ELSE 0
        END AS DECIMAL(18,2)
    ) AS `Value1`,
    CAST(
        CASE lbl.LabelID
            WHEN 1 THEN IFNULL(agg.Cantidad01, 0)
            WHEN 2 THEN IFNULL(agg.Cantidad02, 0)
            WHEN 3 THEN IFNULL(agg.Cantidad03, 0)
            WHEN 4 THEN IFNULL(agg.Cantidad04, 0)
            ELSE 0
        END AS DECIMAL(18,2)
    ) AS `Value2`
FROM (
    SELECT 1 AS LabelID, CONCAT('Entradas', CHAR(10), 'Manuales') AS LabelTitle
    UNION ALL SELECT 2, CONCAT('Otras', CHAR(10), 'entradas')
    UNION ALL SELECT 3, CONCAT('Salidas por', CHAR(10), 'Ventas')
    UNION ALL SELECT 4, CONCAT('Otras', CHAR(10), 'Salidas')
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
        r.FamilyParentID,
        ABS(SUM(CASE WHEN r.TipoRegistroID = 1 THEN r.ValorMovimiento ELSE 0 END)) AS Monto01,
        ABS(SUM(CASE WHEN r.TipoRegistroID = 2 THEN r.ValorMovimiento ELSE 0 END)) AS Monto02,
        ABS(SUM(CASE WHEN r.TipoRegistroID = 3 THEN r.ValorMovimiento ELSE 0 END)) AS Monto03,
        ABS(SUM(CASE WHEN r.TipoRegistroID = 4 THEN r.ValorMovimiento ELSE 0 END)) AS Monto04,
        ABS(SUM(CASE WHEN r.TipoRegistroID = 1 THEN r.CantidadMovimiento ELSE 0 END)) AS Cantidad01,
        ABS(SUM(CASE WHEN r.TipoRegistroID = 2 THEN r.CantidadMovimiento ELSE 0 END)) AS Cantidad02,
        ABS(SUM(CASE WHEN r.TipoRegistroID = 3 THEN r.CantidadMovimiento ELSE 0 END)) AS Cantidad03,
        ABS(SUM(CASE WHEN r.TipoRegistroID = 4 THEN r.CantidadMovimiento ELSE 0 END)) AS Cantidad04
    FROM (
        SELECT
            IFNULL(fam_leaf.FamilyParentID, 0) AS FamilyParentID,
            CASE
                WHEN IFNULL(sr.StockRAmount, 0) > 0 AND IFNULL(sr.StockRClass, '') = 'M' THEN 1
                WHEN IFNULL(sr.StockRAmount, 0) > 0 THEN 2
                WHEN IFNULL(sr.StockRAmount, 0) < 0 AND IFNULL(sr.StockRClass, '') = 'S' THEN 3
                WHEN IFNULL(sr.StockRAmount, 0) < 0 THEN 4
                ELSE 0
            END AS TipoRegistroID,
            IFNULL(sr.StockRAmount, 0) AS CantidadMovimiento,
            (
                IFNULL(sr.StockRAmount, 0) *
                IFNULL((
                    SELECT s.StockAveragePrice
                    FROM stock s
                    WHERE s.StockClinicID = sr.StockRClinicID
                      AND s.StockProductID = sr.StockRProductID
                      AND IFNULL(s.StockProductLot, '') = IFNULL(sr.StockRProductLot, '')
                      AND (
                            (s.StockExpiryDate IS NULL AND sr.StockRExpiryDate IS NULL)
                            OR DATE(s.StockExpiryDate) = DATE(sr.StockRExpiryDate)
                      )
                    LIMIT 1
                ), 0)
            ) AS ValorMovimiento
        FROM stock_registry sr

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
            '1,2,3,4'                             AS DebugFilter1,
            '325,327,326,324,323'                 AS DebugFilter2,
            '0,1,2'                               AS DebugFilter3

    ) t
) p

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
LEFT JOIN x_config_products_det prod
    ON prod.ProductID = sr.StockRProductID

LEFT JOIN x_config_products_fam fam_leaf
    ON fam_leaf.FamilyID = prod.ProductFamilyID

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
        WHERE FIND_IN_SET(sr.StockRClinicID, p.EffectiveClinicIDs)
          AND sr.StockRDate >= p.EffectiveStartDate
          AND sr.StockRDate < DATE_ADD(p.EffectiveEndDate, INTERVAL 1 DAY)
          AND IFNULL(sr.StockRAmount, 0) <> 0
          AND prod.ProductType = 3
          AND (
              p.EffectiveFilter1 IS NULL
              OR p.EffectiveFilter1 = ''
              OR FIND_IN_SET(
                    CASE
                        WHEN IFNULL(sr.StockRAmount, 0) > 0 AND IFNULL(sr.StockRClass, '') = 'M' THEN 1
                        WHEN IFNULL(sr.StockRAmount, 0) > 0 THEN 2
                        WHEN IFNULL(sr.StockRAmount, 0) < 0 AND IFNULL(sr.StockRClass, '') = 'S' THEN 3
                        WHEN IFNULL(sr.StockRAmount, 0) < 0 THEN 4
                        ELSE 0
                    END,
                    p.EffectiveFilter1
              )
          )
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
    ) r
    GROUP BY
        r.FamilyParentID
) agg
    ON agg.FamilyParentID = ser.FamilyParentID

/* ORDER apply here */
ORDER BY
    lbl.LabelID,
    ser.SeriesID;
