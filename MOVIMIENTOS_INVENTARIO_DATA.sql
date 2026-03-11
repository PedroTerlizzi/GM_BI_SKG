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
    r.Sucursal AS `Sucursal`,
    r.StockRDate AS `Fecha`,
    r.StockRTime AS `Hora`,
    r.TipoRegistroDesc AS `Tipo de Registro`,
    r.ProductDesc AS `Descripción Producto`,
    r.StockRProductLot AS `Lote`,
    r.StockRExpiryDate AS `Caducidad`,
    CASE
        WHEN IFNULL(r.CostoUnitario, 0) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(r.CostoUnitario, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(r.CostoUnitario, 0), 2))
    END AS `Costo Un`,
    r.FamilyParentName AS `Família`,
    r.ProviderName AS `Proveedor`,
    CAST(r.CantidadInicial AS DECIMAL(18,2)) AS `Cantidad Inicial`,
    CAST(r.CantidadMovimiento AS DECIMAL(18,2)) AS `Cantidad Movimiento`,
    CAST(r.CantidadFinal AS DECIMAL(18,2)) AS `Cantidad Final`,
    CASE
        WHEN (IFNULL(r.CantidadInicial, 0) * IFNULL(r.CostoUnitario, 0)) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(r.CantidadInicial, 0) * IFNULL(r.CostoUnitario, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(r.CantidadInicial, 0) * IFNULL(r.CostoUnitario, 0), 2))
    END AS `Inicial`,
    CASE
        WHEN (IFNULL(r.CantidadMovimiento, 0) * IFNULL(r.CostoUnitario, 0)) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(r.CantidadMovimiento, 0) * IFNULL(r.CostoUnitario, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(r.CantidadMovimiento, 0) * IFNULL(r.CostoUnitario, 0), 2))
    END AS `Movimiento`,
    CASE
        WHEN (IFNULL(r.CantidadFinal, 0) * IFNULL(r.CostoUnitario, 0)) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(r.CantidadFinal, 0) * IFNULL(r.CostoUnitario, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(r.CantidadFinal, 0) * IFNULL(r.CostoUnitario, 0), 2))
    END AS `Final`
FROM (
    SELECT
        sr.StockRID,
        DATE(sr.StockRDate) AS StockRDate,
        TIME_FORMAT(sr.StockRTime, '%H:%i:%s') AS StockRTime,
        CASE
            WHEN IFNULL(sr.StockRAmount, 0) > 0 AND IFNULL(sr.StockRClass, '') = 'M' THEN 1
            WHEN IFNULL(sr.StockRAmount, 0) > 0 THEN 2
            WHEN IFNULL(sr.StockRAmount, 0) < 0 AND IFNULL(sr.StockRClass, '') = 'S' THEN 3
            WHEN IFNULL(sr.StockRAmount, 0) < 0 THEN 4
            ELSE 0
        END AS TipoRegistroID,
        CASE
            WHEN IFNULL(sr.StockRAmount, 0) > 0 AND IFNULL(sr.StockRClass, '') = 'M' THEN 'Entradas Manuales'
            WHEN IFNULL(sr.StockRAmount, 0) > 0 THEN 'Otras Entradas'
            WHEN IFNULL(sr.StockRAmount, 0) < 0 AND IFNULL(sr.StockRClass, '') = 'S' THEN 'Salidas por Ventas'
            WHEN IFNULL(sr.StockRAmount, 0) < 0 THEN 'Otras Salidas'
            ELSE 'Sin clasificar'
        END AS TipoRegistroDesc,
        IFNULL(cl.ClinicCommercialName, '-') AS Sucursal,
        IFNULL(prod.ProductDesc, '-') AS ProductDesc,
        IFNULL(NULLIF(sr.StockRProductLot, ''), '-') AS StockRProductLot,
        sr.StockRExpiryDate,
        IFNULL(fam_leaf.FamilyParentID, 0) AS FamilyParentID,
        IFNULL(fam_parent.FamilyName, '-') AS FamilyParentName,
        IFNULL(prod.ProductProviderID, 0) AS SourceID,
        IFNULL(prv.ProviderName, 'Proveedor no definido') AS ProviderName,
        (IFNULL(sr.StockRUnits, 0) - IFNULL(sr.StockRAmount, 0)) AS CantidadInicial,
        IFNULL(sr.StockRAmount, 0) AS CantidadMovimiento,
        IFNULL(sr.StockRUnits, 0) AS CantidadFinal,
        IFNULL(prod.ProductVAT, 0) AS IVA,
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
        ), 0) AS CostoUnitario
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
            DATE_SUB(CURDATE(), INTERVAL 30 DAY) AS DebugStartDate,
            CURDATE()                             AS DebugEndDate,
            '1,2,3,4'                             AS DebugFilter1,
            '325,327,326,324,323'                 AS DebugFilter2,
            '0,1,2'                               AS DebugFilter3

    ) t
) p

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
LEFT JOIN x_config_clinics cl
    ON cl.ClinicID = sr.StockRClinicID

LEFT JOIN x_config_products_det prod
    ON prod.ProductID = sr.StockRProductID

LEFT JOIN x_config_products_fam fam_leaf
    ON fam_leaf.FamilyID = prod.ProductFamilyID

LEFT JOIN x_config_products_fam fam_parent
    ON fam_parent.FamilyID = fam_leaf.FamilyParentID

LEFT JOIN x_config_providers prv
    ON prv.ProviderID = prod.ProductProviderID

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

/* ORDER apply here */
ORDER BY
    r.StockRDate DESC,
    r.StockRTime DESC,
    r.StockRID DESC;
