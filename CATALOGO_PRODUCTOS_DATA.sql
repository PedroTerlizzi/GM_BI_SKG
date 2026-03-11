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
    s.FamilyName AS `Familia`,
    s.ProductName AS `Descripcion`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 1  THEN s.AttributeValue END), 'N/D') AS `SKG Tamazunchale`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 2  THEN s.AttributeValue END), 'N/D') AS `SKG Amazonas`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 3  THEN s.AttributeValue END), 'N/D') AS `SKG Punto Aura`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 4  THEN s.AttributeValue END), 'N/D') AS `SKG Cumbres`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 5  THEN s.AttributeValue END), 'N/D') AS `SKG Chihuahua`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 6  THEN s.AttributeValue END), 'N/D') AS `SKG Juriquilla`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 12 THEN s.AttributeValue END), 'N/D') AS `SKG Campa`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 8  THEN s.AttributeValue END), 'N/D') AS `SKG Coapa`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 7  THEN s.AttributeValue END), 'N/D') AS `SKG Aguilas`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 13 THEN s.AttributeValue END), 'N/D') AS `PD San Nico #1`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 9  THEN s.AttributeValue END), 'N/D') AS `PD San Nico #2`,
    IFNULL(MAX(CASE WHEN s.ClinicID = 10 THEN s.AttributeValue END), 'N/D') AS `PD Miguel Aleman`
FROM (
    SELECT
        parent.ProductID AS ParentProductID,
        IFNULL(fam_parent.FamilyName, '-') AS FamilyName,
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(IFNULL(parent.ProductDesc, '-'), '&amp;#60;', '<'),
                                '&amp;#62;', '>'
                            ),
                            '&#60;', '<'
                        ),
                        '&#62;', '>'
                    ),
                    '&lt;', '<'
                ),
                '&gt;', '>'
            ),
            '&amp;', '&'
        ) AS ProductName,
        corp.ClinicID AS ClinicID,
        CASE
            WHEN IFNULL(trf.HasTariff, 0) = 0 THEN CONVERT('-' USING utf8mb4) COLLATE utf8mb4_unicode_ci

            WHEN IFNULL(p.EffectiveFilter2, 2) = 1 THEN
                CONVERT(CAST(CAST(ROUND(IFNULL(stk.StockUnits, 0), 0) AS SIGNED) AS CHAR) USING utf8mb4) COLLATE utf8mb4_unicode_ci

            WHEN IFNULL(p.EffectiveFilter2, 2) = 2 THEN
                CONVERT(CONCAT('$', FORMAT(IFNULL(parent.ProductPrice, 0), 2)) USING utf8mb4) COLLATE utf8mb4_unicode_ci

            WHEN IFNULL(p.EffectiveFilter2, 2) = 3 THEN
                CONVERT(
                    CONCAT(
                        FORMAT(
                            CASE
                                WHEN ABS(IFNULL(parent.ProductVAT, 0)) <= 1 THEN IFNULL(parent.ProductVAT, 0) * 100
                                ELSE IFNULL(parent.ProductVAT, 0)
                            END,
                            0
                        ),
                        '%'
                    )
                    USING utf8mb4
                ) COLLATE utf8mb4_unicode_ci

            WHEN IFNULL(p.EffectiveFilter2, 2) = 4 THEN
                CONVERT(
                    CASE
                        WHEN IFNULL(parent.ProductRequiresTraceability, 0) IN (-1, 1) THEN 'Si'
                        ELSE 'No'
                    END
                    USING utf8mb4
                ) COLLATE utf8mb4_unicode_ci

            ELSE
                CONVERT(CONCAT('$', FORMAT(IFNULL(parent.ProductPrice, 0), 2)) USING utf8mb4) COLLATE utf8mb4_unicode_ci
        END AS AttributeValue
    FROM x_config_products_det parent

/* [DO NOT MODIFY] BLOCK 2 - PARAM RESOLUTION (t + p) */
CROSS JOIN (
    SELECT
        t.*,

        /* Debug switch */
        (t.EngineClinicIDs IS NULL OR t.EngineClinicIDs = '') AS UseDebug,

        /* Effective parameter resolution */
        IF(t.EngineUserID IS NULL,    t.DebugUserID, t.EngineUserID)       AS EffectiveUserID,
        IF(t.EngineClinicIDs = '',    t.DebugClinicIDs, t.EngineClinicIDs) AS EffectiveClinicIDs,
        IF(t.EngineStartDate IS NULL, t.DebugStartDate, t.EngineStartDate) AS EffectiveStartDate,
        IF(t.EngineEndDate IS NULL,   t.DebugEndDate, t.EngineEndDate)     AS EffectiveEndDate,
        IF(t.EngineFilter1CSV IS NULL OR t.EngineFilter1CSV = '', t.DebugFilter1, t.EngineFilter1CSV) AS EffectiveFilter1,
        IF(t.EngineFilter2 IS NULL,   t.DebugFilter2, t.EngineFilter2)     AS EffectiveFilter2,
        IF(t.EngineFilter3 IS NULL,   t.DebugFilter3, t.EngineFilter3)     AS EffectiveFilter3

    FROM (
        SELECT

        /* Production placeholders (replaced by ASP engine) */
            NULL AS EngineUserID,
            ''   AS EngineClinicIDs,
            NULL AS EngineStartDate,
            NULL AS EngineEndDate,
            NULL AS EngineFilter1CSV,
            NULL AS EngineFilter2,
            NULL AS EngineFilter3,

        /* Debug defaults (used automatically in local execution) */
            255                                   AS DebugUserID,
            '1,2,3,4,5,6,12,8,7,13,9,10'         AS DebugClinicIDs,
            DATE_SUB(CURDATE(), INTERVAL 7 DAY)  AS DebugStartDate,
            CURDATE()                             AS DebugEndDate,
            '323,324,325,326,327'                AS DebugFilter1,
            2                                     AS DebugFilter2,
            NULL                                  AS DebugFilter3

    ) t
) p

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
INNER JOIN x_config_products_fam fam_leaf
    ON fam_leaf.FamilyID = parent.ProductFamilyID

LEFT JOIN x_config_products_fam fam_parent
    ON fam_parent.FamilyID = fam_leaf.FamilyParentID

INNER JOIN (
    SELECT 1 AS ClinicID UNION ALL
    SELECT 2 UNION ALL
    SELECT 3 UNION ALL
    SELECT 4 UNION ALL
    SELECT 5 UNION ALL
    SELECT 6 UNION ALL
    SELECT 12 UNION ALL
    SELECT 8 UNION ALL
    SELECT 7 UNION ALL
    SELECT 13 UNION ALL
    SELECT 9 UNION ALL
    SELECT 10
) corp
    ON 1 = 1

LEFT JOIN (
    SELECT
        CASE
            WHEN IFNULL(d.ProductParentID, 0) > 0 THEN d.ProductParentID
            ELSE d.ProductID
        END AS ParentProductID,
        pg.ProductGClinicID AS ClinicID,
        1 AS HasTariff
    FROM x_config_products_det d
    INNER JOIN x_config_products_gen pg
        ON pg.ProductGID = d.ProductGID
       AND IFNULL(pg.ProductGDisabled, 0) = 0
    WHERE IFNULL(d.ProductDisabled, 0) = 0
      AND d.ProductType IN (3, 6)
    GROUP BY
        CASE
            WHEN IFNULL(d.ProductParentID, 0) > 0 THEN d.ProductParentID
            ELSE d.ProductID
        END,
        pg.ProductGClinicID
) trf
    ON trf.ParentProductID = parent.ProductID
   AND trf.ClinicID = corp.ClinicID

LEFT JOIN (
    SELECT
        CASE
            WHEN IFNULL(pd.ProductParentID, 0) > 0 THEN pd.ProductParentID
            ELSE s.StockProductID
        END AS ParentProductID,
        s.StockClinicID AS ClinicID,
        SUM(IFNULL(s.StockUnits, 0)) AS StockUnits
    FROM stock s
    LEFT JOIN x_config_products_det pd
        ON pd.ProductID = s.StockProductID
    GROUP BY
        CASE
            WHEN IFNULL(pd.ProductParentID, 0) > 0 THEN pd.ProductParentID
            ELSE s.StockProductID
        END,
        s.StockClinicID
) stk
    ON stk.ParentProductID = parent.ProductID
   AND stk.ClinicID = corp.ClinicID

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
    WHERE parent.ProductDisabled = 0
      AND parent.ProductID = parent.ProductParentID
      AND parent.ProductType IN (3, 6)
      AND fam_leaf.FamilyParentID IN (323,324,325,326,327)
      AND FIND_IN_SET(corp.ClinicID, p.EffectiveClinicIDs)
      AND (
          p.EffectiveFilter1 IS NULL
          OR p.EffectiveFilter1 = ''
          OR FIND_IN_SET(fam_leaf.FamilyParentID, p.EffectiveFilter1)
      )
) s
GROUP BY
    s.ParentProductID,
    s.FamilyName,
    s.ProductName

/* ORDER apply here */
ORDER BY
    s.FamilyName,
    s.ProductName;
