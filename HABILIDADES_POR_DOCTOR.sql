/* ============================================================================
    GROWMETRICA FLOWWW BI - SQL STANDARD TEMPLATE

    REPORT_ID: 14
    REPORT_TITLE: Habilidades por Doctor
    - Enfoque: Agrupación visual por Doctor y Servicio.
    - Base: x_config_users_products
============================================================================ */

/* [SAFE TO MODIFY] BLOCK 1 - OUTPUT SELECT */
SELECT
    IFNULL(u.UserName, 'Doctor Desconocido') AS `Doctor`, -- Protagonista al inicio
    IFNULL(pf.FamilyName, 'Sin Familia') AS `Familia`,
    pd.ProductDesc AS `Descripcion`,
    IFNULL(xc.ClinicCommercialName, 'Sucursal Desconocida') AS `Sucursal`,
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
        IF(t.EngineFilter1 IS NULL,   t.DebugFilter1, t.EngineFilter1)     AS EffectiveFilter1,
        IF(t.EngineFilter2 IS NULL,   t.DebugFilter2, t.EngineFilter2)     AS EffectiveFilter2,
        IF(t.EngineFilter3 IS NULL,   t.DebugFilter3, t.EngineFilter3)     AS EffectiveFilter3
    FROM (
        SELECT
            NULL AS EngineUserID,
            ''   AS EngineClinicIDs,
            NULL AS EngineStartDate,
            NULL AS EngineEndDate,
            NULL AS EngineFilter1,
            NULL AS EngineFilter2,
            NULL AS EngineFilter3,

            255                                   AS DebugUserID,
            '1,2,3,4,5,6,12,8,7,13,9,10'          AS DebugClinicIDs,
            DATE_SUB(CURDATE(), INTERVAL 30 DAY)  AS DebugStartDate,
            CURDATE()                             AS DebugEndDate,
            1                                     AS DebugFilter1,
            1                                     AS DebugFilter2,
            1                                     AS DebugFilter3
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
  AND u.UserDisabled = 0
  AND up.UserProductProductID <> 33968
  AND up.UserProductUserID NOT IN (273, 299)

/* ORDER apply here */
ORDER BY
    `Doctor` ASC,
    `Familia` ASC,
    `Descripcion` ASC,
    `Sucursal` ASC;

