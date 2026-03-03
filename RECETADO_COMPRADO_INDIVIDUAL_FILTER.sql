SELECT
    FilterLevel,
    FilterLevelTitle,
    FilterIDs,
    FilterName
FROM (
    SELECT  1 AS SortOrder, 1 AS FilterLevel, 'Família de productos' AS FilterLevelTitle, '325,327,326,324,323' AS FilterIDs, 'Todos las famílias' AS FilterName
    UNION ALL SELECT  2, 1, 'Família de productos', '325',    'MP Prescripción'
    UNION ALL SELECT  3, 1, 'Família de productos', '327',    'MP Libre'
    UNION ALL SELECT  4, 1, 'Família de productos', '326',    'PAT Controlado'
    UNION ALL SELECT  5, 1, 'Família de productos', '324',    'PAT Prescripción'
    UNION ALL SELECT  6, 1, 'Família de productos', '323',    'PAT Libre'

    UNION ALL SELECT  7, 2, 'Proveedor',            '0,1,2',  'Todos los Proveedores'
    UNION ALL SELECT  8, 2, 'Proveedor',            '1',      'Neolabma de Mexico'
    UNION ALL SELECT  9, 2, 'Proveedor',            '2',      'Comercializadora Skinstore'
    UNION ALL SELECT 10, 2, 'Proveedor',            '0',      'Proveedor no definido'

    UNION ALL SELECT 11, 3, 'Trazabilidad',         '-1,0',   'Todas las opciones'
    UNION ALL SELECT 12, 3, 'Trazabilidad',         '-1',     'Requiere trazabilidad'
    UNION ALL SELECT 13, 3, 'Trazabilidad',         '0',      'No requiere trazabilidad'
) t
ORDER BY SortOrder;
