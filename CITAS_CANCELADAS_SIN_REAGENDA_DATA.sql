/* ============================================================================
    GROWMETRICA FLOWWW BI - SQL STANDARD TEMPLATE

    REPORT_ID: 16
    REPORT_GROUP: Agenda
    REPORT_TITLE: Citas canceladas sin reagenda (Retención)
    - Enfoque: Detección de fuga de pacientes.
    - Base: diary_gen (Agenda) + diary_det (Servicios cancelados)
    - Integración: Escucha Filter2 (UserID) para filtrar por doctor.
============================================================================ */

/* [SAFE TO MODIFY] BLOCK 1 - OUTPUT SELECT */
SELECT
    IFNULL(xc.ClinicCommercialName, '-')                                     AS `Sucursal`,
    IFNULL(u.UserName, 'Sin Asignar')                                        AS `Doctor Asignado`,
    IFNULL(
        NULLIF(
            TRIM(CONCAT_WS(' ', c.ClientName, c.ClientSurname1, c.ClientSurname2)),
            ''
        ),
        '-'
    )                                                                        AS `Paciente`,
    COALESCE(
        NULLIF(c.ClientPhone1, ''),
        NULLIF(c.ClientPhone2, ''),
        NULLIF(c.ClientPhone3, ''),
        'Sin telefono'
    )                                                                        AS `Telefono`,

    -- Traemos el servicio (o servicios) de la cita
    IFNULL(GROUP_CONCAT(DISTINCT pd.ProductDesc SEPARATOR ', '), 'Servicio no especificado') AS `Servicio Cancelado`,

    DATE(d1.DiaryGDate)                                                      AS `Fecha Cancelacion`,
    IFNULL(NULLIF(d1.DiaryGNotAttendedReason, ''), 'Sin motivo registrado')  AS `Motivo Cancelacion`

FROM diary_gen d1

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
INNER JOIN clients c
    ON c.ClientID = d1.DiaryGClientID

INNER JOIN x_config_clinics xc
    ON xc.ClinicID = d1.DiaryGClinicID

LEFT JOIN __x_config_users_view u
    ON u.UserID = d1.DiaryGUserID

-- Puente para sacar el nombre del servicio cancelado
LEFT JOIN diary_det dd
    ON dd.DiaryGID = d1.DiaryGID
LEFT JOIN x_config_products_det pd
    ON pd.ProductID = dd.DiaryZoneID

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
WHERE FIND_IN_SET(d1.DiaryGClinicID, param.EffectiveClinicIDs)

  -- Ventana de tiempo (la búsqueda de las cancelaciones)
  AND d1.DiaryGDate BETWEEN param.EffectiveStartDate AND param.EffectiveEndDate

  -- El corazón operativo: citas canceladas o no asistidas
  AND d1.DiaryGNotAttended = -1

  -- Limpieza de ruidos (evitar bloqueos o citas invisibles)
  AND d1.DiaryGLocked = 0
  AND d1.DiaryGInvisible = 0

  -- Conexión con el filtro de Front-end (Doctores)
  AND (
      param.EffectiveFilter2 IS NULL
      OR param.EffectiveFilter2 = 0
      OR d1.DiaryGUserID = param.EffectiveFilter2
  )

  -- Lógica analítica "Core": excluir clientes que SÍ tienen citas futuras o reagendadas
  AND NOT EXISTS (
      SELECT 1
      FROM diary_gen d2
      WHERE d2.DiaryGClientID = d1.DiaryGClientID
        AND d2.DiaryGDate > d1.DiaryGDate
        AND d2.DiaryGNotAttended = 0
        AND d2.DiaryGLocked = 0
        AND d2.DiaryGInvisible = 0
  )

/* OBLIGATORIO: AGRUPACIÓN AL USAR GROUP_CONCAT */
GROUP BY
    d1.DiaryGID,
    xc.ClinicCommercialName,
    u.UserName,
    c.ClientName,
    c.ClientSurname1,
    c.ClientSurname2,
    c.ClientPhone1,
    c.ClientPhone2,
    c.ClientPhone3,
    DATE(d1.DiaryGDate),
    d1.DiaryGNotAttendedReason

/* ORDENAMIENTO */
ORDER BY
    `Sucursal` ASC,
    `Fecha Cancelacion` DESC;