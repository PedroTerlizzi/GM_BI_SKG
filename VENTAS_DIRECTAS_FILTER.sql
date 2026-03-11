SELECT
    FilterLevel,
    FilterLevelTitle,
    FilterIDs,
    FilterName
FROM (
    SELECT
        1 AS SortOrder,
        1 AS FilterLevel,
        'Familia' AS FilterLevelTitle,
        '1,2,3,4,5,6,7,9,10,11,12,323,324,325,326,327,999' AS FilterIDs,
        'Todas las Familias' AS FilterName

    UNION ALL

    SELECT
        100 + f.FamilyID AS SortOrder,
        1 AS FilterLevel,
        'Familia' AS FilterLevelTitle,
        CAST(f.FamilyID AS CHAR) AS FilterIDs,
        IFNULL(f.FamilyName, CONCAT('Familia ', f.FamilyID)) AS FilterName
    FROM x_config_products_fam f
    WHERE f.FamilyID IN (1,2,3,4,5,6,7,9,10,11,12,323,324,325,326,327)

    UNION ALL

    SELECT
        1999 AS SortOrder,
        1 AS FilterLevel,
        'Familia' AS FilterLevelTitle,
        '999' AS FilterIDs,
        'Otras' AS FilterName

    UNION ALL SELECT 2001, 2, 'Origen', '1,2,3', 'Todas origenes'
    UNION ALL SELECT 2002, 2, 'Origen', '1',     'Servicio en sesion'
    UNION ALL SELECT 2003, 2, 'Origen', '2',     'Conversion receta (inmediata)'
    UNION ALL SELECT 2004, 2, 'Origen', '3',     'Origen no registrada'
) t
ORDER BY SortOrder, FilterName;
