/* ============================================================================
    GROWMETRICA FLOWWW BI - SQL STANDARD TEMPLATE

    REPORT_ID: 17
    REPORT_GROUP: CATALOGS
    REPORT_TITLE: Cartera Pasiva (Última Cita)
    - Enfoque: Detección de pacientes inactivos sin citas futuras.
    - Base: diary_gen (Agenda)
    - Integración: Escucha Filter2 (UserID) para filtrar por doctor de la última cita.
============================================================================ */

/* [SAFE TO MODIFY] BLOCK 1 - OUTPUT SELECT */
SELECT
    -- En caso de múltiples citas el último día, mostramos la clínica principal
    MAX(IFNULL(xc.ClinicCommercialName, 'Sin Sucursal'))                     AS `Sucursal`,
    IFNULL(
        NULLIF(
            TRIM(CONCAT_WS(' ', c.ClientName, c.ClientSurname1, c.ClientSurname2)),
            ''
        ),
        'Paciente Desconocido'
    )                                                                        AS `Paciente`,
    COALESCE(
        NULLIF(c.ClientPhone1, ''),
        NULLIF(c.ClientPhone2, ''),
        NULLIF(c.ClientPhone3, ''),
        'Sin teléfono'
    )                                                                        AS `Telefono`,

    -- Si lo atendieron 2 doctores el mismo día, los concatena para no duplicar la fila
    IFNULL(GROUP_CONCAT(DISTINCT u.UserName SEPARATOR ', '), 'Sin Asignar')  AS `Doctor Ultima Cita`,

    DATE(d1.DiaryGDate)                                                      AS `Ultima Cita`,

    -- Valor agregado: días exactos desde su última visita
    DATEDIFF(CURDATE(), d1.DiaryGDate)                                       AS `Dias Inactivos`

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
            DATE_SUB(CURDATE(), INTERVAL 180 DAY) AS DebugStartDate,
            DATE_SUB(CURDATE(), INTERVAL 90 DAY)  AS DebugEndDate,
            ''                                    AS DebugFilter1,
            0                                     AS DebugFilter2,
            0                                     AS DebugFilter3
    ) t
) param

/* [SAFE TO MODIFY] BLOCK 3 - BUSINESS JOINS */
INNER JOIN clients c
    ON c.ClientID = d1.DiaryGClientID

LEFT JOIN x_config_clinics xc
    ON xc.ClinicID = d1.DiaryGClinicID

-- Subconsulta 1: Doctores válidos y activos
INNER JOIN (
    SELECT DISTINCT u_view.UserID, u_view.UserName
    FROM __x_config_users_view u_view
    INNER JOIN x_config_users_products up
        ON up.UserProductUserID = u_view.UserID
    WHERE u_view.UserDisabled = 0
      AND u_view.UserID NOT IN (273, 299, 294)
      AND up.UserProductProductID <> 33968
) u ON u.UserID = d1.DiaryGUserID

-- Subconsulta 2: Aislamos la última cita absoluta de cada paciente
INNER JOIN (
    SELECT
        DiaryGClientID,
        MAX(DiaryGDate) AS MaxDate
    FROM diary_gen
    WHERE DiaryGNotAttended = 0
      AND DiaryGLocked = 0
      AND DiaryGInvisible = 0
      AND DiaryGDate <= CURDATE()
    GROUP BY DiaryGClientID
) ultima_fecha
    ON ultima_fecha.DiaryGClientID = d1.DiaryGClientID
   AND ultima_fecha.MaxDate = d1.DiaryGDate

/* [SAFE TO MODIFY] BLOCK 4 - BUSINESS FILTERS */
WHERE FIND_IN_SET(d1.DiaryGClinicID, param.EffectiveClinicIDs)

  -- Filtros base de la cita
  AND d1.DiaryGNotAttended = 0
  AND d1.DiaryGLocked = 0
  AND d1.DiaryGInvisible = 0

  -- Validamos que esta última cita absoluta caiga dentro del rango de búsqueda
  AND d1.DiaryGDate BETWEEN param.EffectiveStartDate AND param.EffectiveEndDate

  -- Conexión con el filtro de Front-end (Doctores)
  AND (
      param.EffectiveFilter2 IS NULL
      OR param.EffectiveFilter2 = 0
      OR d1.DiaryGUserID = param.EffectiveFilter2
  )

/* Agrupamos por cliente para garantizar una fila única */
GROUP BY
    c.ClientID,
    c.ClientName,
    c.ClientSurname1,
    c.ClientSurname2,
    c.ClientPhone1,
    c.ClientPhone2,
    c.ClientPhone3,
    d1.DiaryGDate

/* ORDENAMIENTO */
ORDER BY
    `Dias Inactivos` DESC,
    `Sucursal` ASC;