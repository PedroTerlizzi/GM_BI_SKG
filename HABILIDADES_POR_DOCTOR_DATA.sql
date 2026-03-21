/* ============================================================================
    GROWMETRICA FLOWWW BI - SQL STANDARD TEMPLATE

    REPORT_ID: 14
    REPORT_TITLE: Habilidades por Doctor (Matriz Pivotada)
    - Enfoque: Agrupación visual por Doctor y Servicio (Hardcoded Pivot).
    - Base: x_config_users_products
    - Integración: Escucha Filter1 (FamilyParentID CSV) y Filter2 (UserID)
============================================================================ */

/* [SAFE TO MODIFY] BLOCK 1 - OUTPUT SELECT */
SELECT
    IFNULL(u.UserName, 'Doctor Desconocido') AS `Doctor`,
    IFNULL(pf.FamilyName, 'Sin Familia')     AS `Familia`,
    IFNULL(pd.ProductDesc, '-')              AS `Servicio`,
    
    -- Hardcoded Pivot para las sucursales (Basado en el ID de tu sistema)
    MAX(CASE WHEN up.UserProductClinicID = 1  THEN 'Sí' ELSE 'No' END) AS `SKG Tamazunchale`,
    MAX(CASE WHEN up.UserProductClinicID = 2  THEN 'Sí' ELSE 'No' END) AS `SKG Amazonas`,
    MAX(CASE WHEN up.UserProductClinicID = 3  THEN 'Sí' ELSE 'No' END) AS `SKG Punto Aura`,
    MAX(CASE WHEN up.UserProductClinicID = 4  THEN 'Sí' ELSE 'No' END) AS `SKG Cumbres`,
    MAX(CASE WHEN up.UserProductClinicID = 5  THEN 'Sí' ELSE 'No' END) AS `SKG Chihuahua`,
    MAX(CASE WHEN up.UserProductClinicID = 6  THEN 'Sí' ELSE 'No' END) AS `SKG Juriquilla`,
    MAX(CASE WHEN up.UserProductClinicID = 12 THEN 'Sí' ELSE 'No' END) AS `SKG Campa`,
    MAX(CASE WHEN up.UserProductClinicID = 8  THEN 'Sí' ELSE 'No' END) AS `SKG Coapa`,
    MAX(CASE WHEN up.UserProductClinicID = 7  THEN 'Sí' ELSE 'No' END) AS `SKG Aguilas`,
    MAX(CASE WHEN up.UserProductClinicID = 13 THEN 'Sí' ELSE 'No' END) AS `PD San Nico #1`,
    MAX(CASE WHEN up.UserProductClinicID = 9  THEN 'Sí' ELSE 'No' END) AS `PD San Nico #2`,
    MAX(CASE WHEN up.UserProductClinicID = 10 THEN 'Sí' ELSE 'No' END) AS `PD Miguel Aleman`

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

LEFT JOIN __x_config_users_view u 
    ON u.UserID = up.UserProductUserID

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
WHERE FIND_IN_SET(up.UserProductClinicID, param.EffectiveClinicIDs)
  AND u.UserDisabled = 0 
  AND up.UserProductProductID <> 33968 
  AND up.UserProductUserID NOT IN (273, 299)
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

/* AGRUPAMOS POR DOCTOR Y SERVICIO PARA COLAPSAR LA MATRIZ */
GROUP BY
    u.UserName,
    pf.FamilyName,
    pd.ProductDesc

/* ORDENADO PRIORIZANDO AL DOCTOR */
ORDER BY 
    `Doctor` ASC,
    `Familia` ASC,
    `Servicio` ASC;
