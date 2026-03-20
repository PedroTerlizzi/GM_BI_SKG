SELECT
    FilterLevel,
    FilterLevelTitle,
    FilterIDs,
    FilterName
FROM (
    -- ==========================================================
    -- NIVEL 1: FAMILIAS DE SERVICIOS
    -- ==========================================================
    SELECT
        1 AS SortOrder,
        1 AS FilterLevel,
        'Familia de servicios' AS FilterLevelTitle,
        '1,2,3,4,5,6,7,9,10,11,12' AS FilterIDs,
        'Todas las familias' AS FilterName

    UNION ALL

    SELECT
        100 + fams.FamilyParentID AS SortOrder,
        1 AS FilterLevel,
        'Familia de servicios' AS FilterLevelTitle,
        CAST(fams.FamilyParentID AS CHAR) AS FilterIDs,
        fams.FamilyParentName AS FilterName
    FROM (
        SELECT DISTINCT
            fam_leaf.FamilyParentID AS FamilyParentID,
            IFNULL(fam_parent.FamilyName, CONCAT('Familia ', fam_leaf.FamilyParentID)) AS FamilyParentName
        FROM x_config_products_det prod
        INNER JOIN x_config_products_fam fam_leaf
            ON fam_leaf.FamilyID = prod.ProductFamilyID
        LEFT JOIN x_config_products_fam fam_parent
            ON fam_parent.FamilyID = fam_leaf.FamilyParentID
        WHERE prod.ProductDisabled = 0
          AND prod.ProductID = prod.ProductParentID
          AND prod.ProductType IN (1, 4)
          AND fam_leaf.FamilyParentID IN (1,2,3,4,5,6,7,9,10,11,12)
    ) fams

    UNION ALL

    -- ==========================================================
    -- NIVEL 2: DOCTORES (Filtrado por Habilidades)
    -- ==========================================================
    SELECT
        2000 AS SortOrder,
        2 AS FilterLevel,
        'Doctor' AS FilterLevelTitle,
        '0' AS FilterIDs,
        'Todos los doctores' AS FilterName

    UNION ALL

    SELECT DISTINCT
        2000 + u.UserID AS SortOrder,
        2 AS FilterLevel,
        'Doctor' AS FilterLevelTitle,
        CAST(u.UserID AS CHAR) AS FilterIDs,
        u.UserName AS FilterName
    FROM __x_config_users_view u
    INNER JOIN x_config_users_products up
        ON up.UserProductUserID = u.UserID
    WHERE u.UserDisabled = 0
      AND u.UserID NOT IN (273, 299, 294)
      AND up.UserProductProductID <> 33968
) t
ORDER BY
    SortOrder ASC,
    FilterName ASC;

