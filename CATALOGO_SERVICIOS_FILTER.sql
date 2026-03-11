SELECT
    FilterLevel,
    FilterLevelTitle,
    FilterIDs,
    FilterName
FROM (
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

    UNION ALL SELECT 2001, 2, 'Atributo', '1', 'Precio c/ IVA'
    UNION ALL SELECT 2002, 2, 'Atributo', '2', 'Comision'
    UNION ALL SELECT 2003, 2, 'Atributo', '3', 'Duracion Std'
    UNION ALL SELECT 2004, 2, 'Atributo', '4', 'Exclusivo Dr.'
) t
ORDER BY SortOrder, FilterName;
