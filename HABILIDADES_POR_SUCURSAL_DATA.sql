/* ============================================================================
    GROWMETRICA FLOWWW BI - SQL STANDARD TEMPLATE

    REPORT_ID: 13
    REPORT_TITLE: Habilidades por Sucursal
    - Enfoque: Agrupación visual por Sucursal y Servicio. Expande Doctores.
    - Base: x_config_users_products
============================================================================ */

/* [SAFE TO MODIFY] BLOCK 1 - OUTPUT SELECT */
SELECT
    IFNULL(xc.ClinicCommercialName, 'Sucursal Desconocida') AS `Sucursal`,
    IFNULL(pf.FamilyName, 'Sin Familia') AS `Familia`,
    IFNULL(pd.ProductDesc, '-') AS `Servicio`,
    IFNULL(u.UserName, 'Doctor Desconocido') AS `Doctor`,
    'Si' AS `Capacitado`

FROM x_config_users_products up

/* [DO NOT MODIFY] BLOCK 2 - PARAM RESOLUTION (t + p) */
CROSS JOIN (
    SELECT
        t.*,
        (t.EngineClinicIDs IS NULL OR t.EngineClinicIDs = '') AS UseDebug,
        IF(t.EngineUserID IS NULL,    t.DebugUserID, t.EngineUserID)       AS EffectiveUserID,
        IF(t.EngineClinicIDs = '',    t.DebugClinicIDs, t.EngineClinicIDs) AS EffectiveClinicIDs,
        IF(t.EngineStartDate IS NULL, t.DebugStartDate, t.EngineStartDate) AS EffectiveStartDate,
        IF(t.EngineEndDate IS NULL,   t.DebugEndDate, t.EngineEndDate)     AS EffectiveEndDate,
        IF(t.EngineFilter1CSV IS NULL OR t.EngineFilter1CSV = '', t.DebugFilter1, t.EngineFilter1CSV) AS EffectiveFilter1,
        IF(t.EngineFilter2 IS NULL,   t.DebugFilter2, t.EngineFilter2)     AS EffectiveFilter2,
        IF(t.EngineFilter3 IS NULL,   t.DebugFilter3, t.EngineFilter3)     AS EffectiveFilter3
    FROM (
        SELECT
            NULL AS EngineUserID,
            ''   AS EngineClinicIDs,
            NULL AS EngineStartDate,
            NULL AS EngineEndDate,
            NULL AS EngineFilter2,
            NULL AS EngineFilter3,
            NULL AS EngineFilter1CSV,
            
            255                                   AS DebugUserID,
            '1,2,3,4,5,6,12,8,7,13,9,10'          AS DebugClinicIDs,
            DATE_SUB(CURDATE(), INTERVAL 30 DAY)  AS DebugStartDate, 
            CURDATE()                             AS DebugEndDate,
            ''                                    AS DebugFilter1,
            0                                     AS DebugFilter2,
            0                                     AS DebugFilter3
    ) t
) param

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
INNER JOIN x_config_products_det pd 
    ON pd.ProductID = up.UserProductProductID

LEFT JOIN x_config_products_fam pf
    ON pf.FamilyID = pd.ProductFamilyID

LEFT JOIN x_config_clinics xc 
    ON xc.ClinicID = up.UserProductClinicID

LEFT JOIN __x_config_users_view u 
    ON u.UserID = up.UserProductUserID

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
WHERE FIND_IN_SET(up.UserProductClinicID, param.EffectiveClinicIDs)
  AND u.UserDisabled = 0 -- FILTRO DE PERSONAL ACTIVO
  AND up.UserProductProductID <> 33968 -- EXCLUYE HABILIDAD ESPECÍFICA
  AND up.UserProductUserID NOT IN (273, 299) -- EXCLUYE DOCTORES/USUARIOS ESPECÍFICOS
  AND (
      param.EffectiveFilter1 IS NULL
      OR param.EffectiveFilter1 = ''
      OR FIND_IN_SET(pf.FamilyParentID, param.EffectiveFilter1)
  )
  AND (
      param.EffectiveFilter2 IS NULL
      OR param.EffectiveFilter2 = 0
      OR u.UserID = param.EffectiveFilter2
  )

/* ORDENADO PARA LEERSE POR SUCURSAL */
ORDER BY 
    `Sucursal` ASC,
    `Familia` ASC,
    `Servicio` ASC,
    `Doctor` ASC;
