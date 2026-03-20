/* ============================================================================
    GROWMETRICA FLOWWW BI - SQL STANDARD TEMPLATE

    REPORT_ID: 15
    REPORT_GROUP: Agenda
    REPORT_TITLE: Citas por confirmar
    - Enfoque: Listado operativo para Call Center / Recepción.
    - Base: diary_gen (Cabecera) + diary_det (Detalle de servicios)
    - Integración: Escucha Filter2 (UserID)
============================================================================ */

/* [SAFE TO MODIFY] BLOCK 1 - OUTPUT SELECT */
SELECT
    IFNULL(xc.ClinicCommercialName, 'Sucursal Desconocida') AS `Sucursal`,
    dg.DiaryGDate AS `Fecha`,
    dg.DiaryGStart AS `Hora`,
    CONCAT(IFNULL(c.ClientName, ''), ' ', IFNULL(c.ClientSurname1, '')) AS `Paciente`,
    IFNULL(c.ClientPhone1, 'Sin teléfono') AS `Telefono`,
    IFNULL(u.UserName, 'Sin asignar') AS `Doctor Asignado`,

    -- Agrupamos los servicios de la cita iterando sobre diary_det
    IFNULL(GROUP_CONCAT(pd.ProductDesc SEPARATOR ', '), 'Servicio no especificado') AS `Servicios`,

    -- Validación de la confirmación del Doctor
    IF(dg.DiaryGLaserDoctor = -1, 'Sí', 'No') AS `Confirmado Por Doctor`

FROM diary_gen dg

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
            CURDATE()                             AS DebugStartDate, -- Hoy
            DATE_ADD(CURDATE(), INTERVAL 7 DAY)   AS DebugEndDate,   -- Próximos 7 días
            ''                                    AS DebugFilter1,
            0                                     AS DebugFilter2,
            0                                     AS DebugFilter3
    ) t
) param

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
-- 1. Unimos al paciente para tener su nombre y teléfono
LEFT JOIN clients c
    ON c.ClientID = dg.DiaryGClientID

-- 2. Unimos la sucursal
LEFT JOIN x_config_clinics xc
    ON xc.ClinicID = dg.DiaryGClinicID

-- 3. Unimos al personal médico agendado
LEFT JOIN __x_config_users_view u
    ON u.UserID = dg.DiaryGUserID

-- 4. Conectamos la cabecera de la cita con su detalle y luego al catálogo
LEFT JOIN diary_det dd
    ON dd.DiaryGID = dg.DiaryGID
LEFT JOIN x_config_products_det pd
    ON pd.ProductID = dd.DiaryZoneID

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
WHERE FIND_IN_SET(dg.DiaryGClinicID, param.EffectiveClinicIDs)
  -- Ventana de tiempo de la agenda
  AND dg.DiaryGDate BETWEEN param.EffectiveStartDate AND param.EffectiveEndDate

  -- Filtros críticos de limpieza operativa
  AND dg.DiaryGLocked = 0       -- Ignora bloqueos de horario (ej. hora de comida)
  AND dg.DiaryGInvisible = 0    -- Ignora registros internos del sistema
  AND dg.DiaryGNotAttended = 0  -- Ignora citas canceladas/no asistidas

  -- El corazón del reporte: solo citas que NO han sido confirmadas
  AND dg.DiaryGConfirmed = 0

  -- Filtro de Doctor (FilterLevel 2)
  AND (
      param.EffectiveFilter2 IS NULL
      OR param.EffectiveFilter2 = 0
      OR dg.DiaryGUserID = param.EffectiveFilter2
  )

/* COMO USAMOS GROUP_CONCAT, DEBEMOS AGRUPAR POR CITA */
GROUP BY
    dg.DiaryGID,
    xc.ClinicCommercialName,
    dg.DiaryGDate,
    dg.DiaryGStart,
    c.ClientName,
    c.ClientSurname1,
    c.ClientPhone1,
    u.UserName,
    dg.DiaryGLaserDoctor

/* ORDENADO CRONOLÓGICAMENTE PARA EL CALL CENTER */
ORDER BY
    `Sucursal` ASC,
    `Fecha` ASC,
    `Hora` ASC;