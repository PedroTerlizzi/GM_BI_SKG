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
    CAST(d.CantidadFinal AS DECIMAL(18,2)) AS `Cantidad Actual`,
    CAST(d.CostoUnitario AS DECIMAL(18,4)) AS `Costo Unitario (c/ IVA)`,
    d.Sucursal AS `Sucursal`,
    d.ProductDesc AS `Descripcion Producto`,
    d.FamilyParentID AS `FamilyParentID`,
    d.FamilyParentName AS `Familia de Productos`,
    d.SourceID AS `SourceID`,
    d.ProviderName AS `Proveedor`,
    CAST(d.CantidadInicial AS DECIMAL(18,2)) AS `Cantidad Inicial`,
    CAST(d.CantidadEntradasManuales AS DECIMAL(18,2)) AS `Cantidad Entradas Manuales`,
    CAST(d.CantidadOtrasEntradas AS DECIMAL(18,2)) AS `Cantidad Otras Entradas`,
    CAST(d.CantidadSalidasVentas AS DECIMAL(18,2)) AS `Cantidad Salidas por Ventas`,
    CAST(d.CantidadOtrasSalidas AS DECIMAL(18,2)) AS `Cantidad Otras Salidas`,
    CAST(d.CantidadFinal AS DECIMAL(18,2)) AS `Cantidad Final`,
    CAST(d.CantidadRecetada AS DECIMAL(18,2)) AS `Cantidad Recetada`,
    CAST(d.ValorInicial AS DECIMAL(18,2)) AS `Valor Inicial (a costo c/ IVA)`,
    CAST(d.ValorEntradas AS DECIMAL(18,2)) AS `Valor Entradas (a costo c/ IVA)`,
    CAST(d.ValorSalidas AS DECIMAL(18,2)) AS `Valor Salidas (a costo c/ IVA)`,
    CAST(d.ValorFinal AS DECIMAL(18,2)) AS `Valor Final (a costo c/ IVA)`,
    CAST(d.InventarioPromedio AS DECIMAL(18,2)) AS `Inventario Promedio`,
    CAST(d.CostoVentas AS DECIMAL(18,2)) AS `Costo de Ventas (c/ IVA)`,
    CAST(d.ValorRecetado AS DECIMAL(18,2)) AS `Valor Recetado (a costo c/ IVA)`,
    CASE
        WHEN d.CapturaDemandaNum IS NULL THEN '-'
        ELSE FORMAT(d.CapturaDemandaNum, 4)
    END AS `Captura de Demanda`,
    CASE
        WHEN d.DiasInventarioVentasNum IS NULL THEN '-'
        ELSE FORMAT(d.DiasInventarioVentasNum, 1)
    END AS `Dias de Inventario (ventas)`,
    CASE
        WHEN d.DiasInventarioDemandaNum IS NULL THEN '-'
        ELSE FORMAT(d.DiasInventarioDemandaNum, 1)
    END AS `Dias de Inventario (demanda clinica)`,
    CASE
        WHEN d.DiasInventarioPonderadoNum IS NULL THEN '-'
        ELSE FORMAT(d.DiasInventarioPonderadoNum, 1)
    END AS `Dias de Inventario (ponderado)`,
    d.Clasificacion AS `Clasificacion`
FROM (
    SELECT
        c.*,
        CASE
            WHEN c.ValorRecetado = 0 THEN NULL
            ELSE c.CostoVentas / c.ValorRecetado
        END AS CapturaDemandaNum,
        CASE
            WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL
            ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas
        END AS DiasInventarioVentasNum,
        CASE
            WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL
            ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
        END AS DiasInventarioDemandaNum,
        CASE
            WHEN
                (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                AND
                (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                THEN NULL
            WHEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                THEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END)
            WHEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                THEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END)
            ELSE LEAST(
                (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas,
                (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
            )
        END AS DiasInventarioPonderadoNum,
        CASE
            WHEN
                CASE
                    WHEN
                        (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        AND
                        (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN NULL
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END)
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END)
                    ELSE LEAST(
                        (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas,
                        (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
                    )
                END IS NULL THEN 0
            WHEN
                CASE
                    WHEN
                        (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        AND
                        (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN NULL
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END)
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END)
                    ELSE LEAST(
                        (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas,
                        (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
                    )
                END < 30 THEN 1
            WHEN
                CASE
                    WHEN
                        (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        AND
                        (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN NULL
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END)
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END)
                    ELSE LEAST(
                        (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas,
                        (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
                    )
                END <= 60 THEN 2
            WHEN
                CASE
                    WHEN
                        (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        AND
                        (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN NULL
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END)
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END)
                    ELSE LEAST(
                        (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas,
                        (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
                    )
                END <= 120 THEN 3
            WHEN
                CASE
                    WHEN
                        (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        AND
                        (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN NULL
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END)
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END)
                    ELSE LEAST(
                        (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas,
                        (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
                    )
                END <= 180 THEN 4
            ELSE 5
        END AS ClasificacionID,
        CASE
            WHEN
                CASE
                    WHEN
                        (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        AND
                        (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN NULL
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END)
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END)
                    ELSE LEAST(
                        (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas,
                        (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
                    )
                END IS NULL THEN '-'
            WHEN
                CASE
                    WHEN
                        (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        AND
                        (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN NULL
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END)
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END)
                    ELSE LEAST(
                        (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas,
                        (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
                    )
                END < 30 THEN 'Muy Rapido (<30d)'
            WHEN
                CASE
                    WHEN
                        (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        AND
                        (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN NULL
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END)
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END)
                    ELSE LEAST(
                        (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas,
                        (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
                    )
                END <= 60 THEN 'Saludable (30-60d)'
            WHEN
                CASE
                    WHEN
                        (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        AND
                        (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN NULL
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END)
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END)
                    ELSE LEAST(
                        (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas,
                        (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
                    )
                END <= 120 THEN 'Lento (60-120d)'
            WHEN
                CASE
                    WHEN
                        (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        AND
                        (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN NULL
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END)
                    WHEN (CASE WHEN c.PeriodDays <= 0 OR c.ValorRecetado = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado END) IS NULL
                        THEN (CASE WHEN c.PeriodDays <= 0 OR c.CostoVentas = 0 THEN NULL ELSE (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas END)
                    ELSE LEAST(
                        (c.InventarioPromedio * c.PeriodDays) / c.CostoVentas,
                        (c.InventarioPromedio * c.PeriodDays) / c.ValorRecetado
                    )
                END <= 180 THEN 'Riesgo (120-180d)'
            ELSE 'Critico (>180d)'
        END AS Clasificacion
    FROM (
        SELECT
            b.*,
            DATEDIFF(b.EffectiveEndDate, b.EffectiveStartDate) AS PeriodDays,
            (b.StockUnitsNow - b.QtyAfterEnd) AS CantidadFinal,
            ((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) AS CantidadInicial,
            b.QtyEntradasManuales AS CantidadEntradasManuales,
            b.QtyOtrasEntradas AS CantidadOtrasEntradas,
            b.QtySalidasVentas AS CantidadSalidasVentas,
            b.QtyOtrasSalidas AS CantidadOtrasSalidas,
            b.QtyRecetada AS CantidadRecetada,
            (((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) * b.CostoUnitario) AS ValorInicial,
            ((b.QtyEntradasManuales + b.QtyOtrasEntradas) * b.CostoUnitario) AS ValorEntradas,
            (ABS(b.QtySalidasVentas + b.QtyOtrasSalidas) * b.CostoUnitario) AS ValorSalidas,
            ((b.StockUnitsNow - b.QtyAfterEnd) * b.CostoUnitario) AS ValorFinal,
            (
                ((((b.StockUnitsNow - b.QtyAfterEnd) - b.QtyMovPeriodo) * b.CostoUnitario) +
                (((b.StockUnitsNow - b.QtyAfterEnd) * b.CostoUnitario))) / 2
            ) AS InventarioPromedio,
            (ABS(b.QtySalidasVentas) * b.CostoUnitario) AS CostoVentas,
            (b.QtyRecetada * b.CostoUnitario) AS ValorRecetado
        FROM (
            SELECT
                p.EffectiveStartDate,
                p.EffectiveEndDate,
                p.EffectiveFilter1,
                p.EffectiveFilter2,
                p.EffectiveFilter3,
                s.StockClinicID,
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
                p.EffectiveFilter1,
                p.EffectiveFilter2,
                p.EffectiveFilter3,
                s.StockClinicID,
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
    ) c
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
    AND (
        d.EffectiveFilter1 IS NULL
        OR d.EffectiveFilter1 = ''
        OR FIND_IN_SET(d.ClasificacionID, d.EffectiveFilter1)
    )

/* ORDER apply here */
ORDER BY
    d.Sucursal,
    d.FamilyParentName,
    d.ProductDesc,
    d.StockProductID,
    d.StockProductLot;
