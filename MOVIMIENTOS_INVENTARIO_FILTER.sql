SELECT
    FilterLevel,
    FilterLevelTitle,
    FilterIDs,
    FilterName
FROM (
    SELECT  1 AS SortOrder, 1 AS FilterLevel, 'Tipo de movimiento'   AS FilterLevelTitle, '1,2,3,4'           AS FilterIDs, 'Todos los Movimientos'      AS FilterName
    UNION ALL SELECT  2, 1, 'Tipo de movimiento',                      '1',                 'Entradas Manuales'
    UNION ALL SELECT  3, 1, 'Tipo de movimiento',                      '2',                 'Otras entradas'
    UNION ALL SELECT  4, 1, 'Tipo de movimiento',                      '3',                 'Salidas por Ventas'
    UNION ALL SELECT  5, 1, 'Tipo de movimiento',                      '4',                 'Otras Salidas'

    UNION ALL SELECT  6, 2, 'Familia de productos',                    '325,327,326,324,323','Todos las Familias'
    UNION ALL SELECT  7, 2, 'Familia de productos',                    '325',               'MP Prescripcion'
    UNION ALL SELECT  8, 2, 'Familia de productos',                    '327',               'MP Libre'
    UNION ALL SELECT  9, 2, 'Familia de productos',                    '326',               'PAT Controlado'
    UNION ALL SELECT 10, 2, 'Familia de productos',                    '324',               'PAT Prescripcion'
    UNION ALL SELECT 11, 2, 'Familia de productos',                    '323',               'PAT Libre'

    UNION ALL SELECT 12, 3, 'Proveedor',                               '0,1,2',             'Todos los Proveedores'
    UNION ALL SELECT 13, 3, 'Proveedor',                               '1',                 'Neolabma de Mexico'
    UNION ALL SELECT 14, 3, 'Proveedor',                               '2',                 'Comercializadora Skinstore'
    UNION ALL SELECT 15, 3, 'Proveedor',                               '0',                 'Proveedor no definido'
) t
ORDER BY SortOrder;
