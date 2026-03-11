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
        '1,2,3,4,5,6,7,9,10,11,12' AS FilterIDs,
        'Todas las familias' AS FilterName

    UNION ALL

    SELECT
        100 + fam.FamilyID AS SortOrder,
        1 AS FilterLevel,
        'Familia de Insumos' AS FilterLevelTitle,
        CAST(fam.FamilyID AS CHAR) AS FilterIDs,
        IFNULL(fam.FamilyName, CONCAT('Familia ', fam.FamilyID)) AS FilterName
    FROM x_config_products_fam fam
    WHERE fam.FamilyID IN (1,2,3,4,5,6,7,9,10,11,12)

    UNION ALL SELECT 2001, 2, 'Atributo', '1', 'Precio c/ IVA'
    UNION ALL SELECT 2002, 2, 'Atributo', '2', 'IVA'
) t
ORDER BY SortOrder, FilterName;
