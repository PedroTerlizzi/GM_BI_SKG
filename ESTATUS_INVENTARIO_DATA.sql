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
    d.StockProductID AS `ID Producto`,
    d.StockProductLot AS `Lote`,
    d.StockExpiryDate AS `Caducidad`,
    CAST(ROUND(d.CantidadFinal, 0) AS SIGNED) AS `Cantidad Actual`,
    CASE
        WHEN IFNULL(d.CostoUnitario, 0) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(d.CostoUnitario, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(d.CostoUnitario, 0), 2))
    END AS `Costo Unitario (c/ IVA)`,
    d.Sucursal AS `Sucursal`,
    d.ProductDesc AS `Descripcion Producto`,
    d.FamilyParentID AS `FamilyParentID`,
    d.FamilyParentName AS `Familia de Productos`,
    d.SourceID AS `SourceID`,
    d.ProviderName AS `Proveedor`,
    CAST(ROUND(d.CantidadInicial, 0) AS SIGNED) AS `Cantidad Inicial`,
    CAST(ROUND(d.CantidadEntradasManuales, 0) AS SIGNED) AS `Cantidad Entradas Manuales`,
    CAST(ROUND(d.CantidadOtrasEntradas, 0) AS SIGNED) AS `Cantidad Otras Entradas`,
    CAST(ROUND(d.CantidadSalidasVentas, 0) AS SIGNED) AS `Cantidad Salidas por Ventas`,
    CAST(ROUND(d.CantidadOtrasSalidas, 0) AS SIGNED) AS `Cantidad Otras Salidas`,
    CAST(ROUND(d.CantidadFinal, 0) AS SIGNED) AS `Cantidad Final`,
    CASE
        WHEN IFNULL(d.ValorInicial, 0) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(d.ValorInicial, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(d.ValorInicial, 0), 2))
    END AS `Valor Inicial (a costo c/ IVA)`,
    CASE
        WHEN IFNULL(d.ValorEntradas, 0) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(d.ValorEntradas, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(d.ValorEntradas, 0), 2))
    END AS `Valor Entradas (a costo c/ IVA)`,
    CASE
        WHEN IFNULL(d.ValorSalidas, 0) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(d.ValorSalidas, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(d.ValorSalidas, 0), 2))
    END AS `Valor Salidas (a costo c/ IVA)`,
    CASE
        WHEN IFNULL(d.ValorFinal, 0) < 0 THEN CONCAT('-$', FORMAT(ABS(IFNULL(d.ValorFinal, 0)), 2))
        ELSE CONCAT('$', FORMAT(IFNULL(d.ValorFinal, 0), 2))
    END AS `Valor Final (a costo c/ IVA)`
FROM (
    SELECT
        b.StockProductID,
        b.StockProductLot,
        b.StockExpiryDate,
        b.CostoUnitario,
        b.Sucursal,
        b.ProductDesc,
        b.FamilyParentID,
        b.FamilyParentName,
        b.SourceID,
        b.ProviderName,
        b.QtyEntradasManuales AS CantidadEntradasManuales,
        b.QtyOtrasEntradas AS CantidadOtrasEntradas,
        b.QtySalidasVentas AS CantidadSalidasVentas,
        b.QtyOtrasSalidas AS CantidadOtrasSalidas,
        (b.StockUnitsNow - b.QtyAfterEnd) AS CantidadFinal,
        ((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) AS CantidadInicial,
        (((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) * b.CostoUnitario) AS ValorInicial,
        ((b.QtyEntradasManuales + b.QtyOtrasEntradas) * b.CostoUnitario) AS ValorEntradas,
        ((b.QtySalidasVentas + b.QtyOtrasSalidas) * b.CostoUnitario) AS ValorSalidas,
        ((b.StockUnitsNow - b.QtyAfterEnd) * b.CostoUnitario) AS ValorFinal
    FROM (
        SELECT
            s.StockProductID,
            IFNULL(NULLIF(s.StockProductLot, ''), '-') AS StockProductLot,
            s.StockExpiryDate,
            IFNULL(s.StockUnits, 0) AS StockUnitsNow,
            IFNULL(s.StockAveragePrice, 0) AS CostoUnitario,
            IFNULL(cl.ClinicCommercialName, '-') AS Sucursal,
            IFNULL(prod.ProductDesc, '-') AS ProductDesc,
            IFNULL(fam_leaf.FamilyParentID, 0) AS FamilyParentID,
            IFNULL(fam_parent.FamilyName, '-') AS FamilyParentName,
            IFNULL(prod.ProductProviderID, 0) AS SourceID,
            IFNULL(prv.ProviderName, 'Proveedor no definido') AS ProviderName,
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
LEFT JOIN x_config_clinics cl
    ON cl.ClinicID = s.StockClinicID

LEFT JOIN x_config_products_det prod
    ON prod.ProductID = s.StockProductID

LEFT JOIN x_config_products_fam fam_leaf
    ON fam_leaf.FamilyID = prod.ProductFamilyID

LEFT JOIN x_config_products_fam fam_parent
    ON fam_parent.FamilyID = fam_leaf.FamilyParentID

LEFT JOIN x_config_providers prv
    ON prv.ProviderID = prod.ProductProviderID

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
            s.StockProductID,
            IFNULL(NULLIF(s.StockProductLot, ''), '-'),
            s.StockExpiryDate,
            IFNULL(s.StockUnits, 0),
            IFNULL(s.StockAveragePrice, 0),
            IFNULL(cl.ClinicCommercialName, '-'),
            IFNULL(prod.ProductDesc, '-'),
            IFNULL(fam_leaf.FamilyParentID, 0),
            IFNULL(fam_parent.FamilyName, '-'),
            IFNULL(prod.ProductProviderID, 0),
            IFNULL(prv.ProviderName, 'Proveedor no definido')
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

/* ORDER apply here */
ORDER BY
    d.Sucursal,
    d.FamilyParentName,
    d.ProductDesc,
    d.StockProductID,
    d.StockProductLot;
