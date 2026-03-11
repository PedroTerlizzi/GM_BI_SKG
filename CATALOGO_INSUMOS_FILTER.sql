SELECT
    FilterLevel,
    FilterLevelTitle,
    FilterIDs,
    FilterName
FROM (
    SELECT
        1 AS SortOrder,
        1 AS FilterLevel,
        'Familia de Insumos' AS FilterLevelTitle,
        '124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139' AS FilterIDs,
        'Todas las familias' AS FilterName

    UNION ALL

    SELECT
        100 + fams.FamilyParentID AS SortOrder,
        1 AS FilterLevel,
        'Familia de Insumos' AS FilterLevelTitle,
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
          AND prod.ProductType IN (3, 6)
          AND fam_leaf.FamilyParentID IN (124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139)
    ) fams

    UNION ALL SELECT 2001, 2, 'Atributo', '2', 'Costo c/ IVA'
    UNION ALL SELECT 2002, 2, 'Atributo', '1', 'Cantidad'
    UNION ALL SELECT 2003, 2, 'Atributo', '3', 'IVA'
    UNION ALL SELECT 2004, 2, 'Atributo', '4', 'Requiere Trazabilidad'
    UNION ALL SELECT 2005, 2, 'Atributo', '5', 'Esta a la venta?'
) t
ORDER BY SortOrder, FilterName;
