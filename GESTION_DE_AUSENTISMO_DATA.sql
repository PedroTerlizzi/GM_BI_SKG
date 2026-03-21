/* ============================================================================
    GROWMETRICA FLOWWW BI - SQL STANDARD TEMPLATE

    REPORT_ID: 18
    REPORT_GROUP: CATALOGS
    REPORT_TITLE: Gestion de Ausentismo
    - Enfoque: Indicadores operativos de agenda.
    - Base: diary_gen (Agenda)
============================================================================ */

/* [SAFE TO MODIFY] BLOCK 1 - OUTPUT SELECT */
SELECT
    m.IndicatorTitle AS `Indicadores`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 0 THEN m.MetricValue END), '-') AS `Total`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 1  THEN m.MetricValue END), '-') AS `SKG Tamazunchale`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 2  THEN m.MetricValue END), '-') AS `SKG Amazonas`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 3  THEN m.MetricValue END), '-') AS `SKG Punto Aura`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 4  THEN m.MetricValue END), '-') AS `SKG Cumbres`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 5  THEN m.MetricValue END), '-') AS `SKG Chihuahua`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 6  THEN m.MetricValue END), '-') AS `SKG Juriquilla`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 12 THEN m.MetricValue END), '-') AS `SKG Campa`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 8  THEN m.MetricValue END), '-') AS `SKG Coapa`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 7  THEN m.MetricValue END), '-') AS `SKG Aguilas`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 13 THEN m.MetricValue END), '-') AS `PD San Nico #1`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 9  THEN m.MetricValue END), '-') AS `PD San Nico #2`,
    IFNULL(MAX(CASE WHEN m.ClinicID = 10 THEN m.MetricValue END), '-') AS `PD Miguel Aleman`
FROM (
    SELECT
        ind.IndicatorOrder,
        ind.IndicatorTitle,
        a.ClinicID,
        CASE ind.IndicatorKey
            WHEN 'total_registros' THEN CAST(a.TotalRegistros AS CHAR)
            WHEN 'mov_1plus' THEN CAST(a.Movimientos1Plus AS CHAR)
            WHEN 'mov_same' THEN CAST(a.MovimientosSameDay AS CHAR)
            WHEN 'indice_retrabajo' THEN
                CASE
                    WHEN a.TotalRegistros = 0 THEN '-'
                    ELSE CONCAT(ROUND(100 * (a.Movimientos1Plus + a.MovimientosSameDay) / a.TotalRegistros, 1), '%')
                END
            WHEN 'citas_agendadas' THEN CAST(a.CitasAgendadas AS CHAR)
            WHEN 'cancel_4plus' THEN CAST(a.Canceladas4Plus AS CHAR)
            WHEN 'cancel_1_3' THEN CAST(a.Canceladas1a3 AS CHAR)
            WHEN 'cancel_same' THEN CAST(a.CanceladasSameDay AS CHAR)
            WHEN 'mala_gestion' THEN CAST(a.MalaGestion AS CHAR)
            WHEN 'indice_efectividad' THEN
                CASE
                    WHEN a.CanceladasTotal = 0 THEN '-'
                    ELSE CONCAT(ROUND(100 * a.Canceladas1a3 / a.CanceladasTotal, 1), '%')
                END
            WHEN 'indice_ausentismo' THEN
                CASE
                    WHEN a.CitasAgendadas = 0 THEN '-'
                    ELSE CONCAT(ROUND(100 * (a.CanceladasSameDay + a.MalaGestion) / a.CitasAgendadas, 1), '%')
                END
            WHEN 'concluidas' THEN CAST(a.Concluidas AS CHAR)
            WHEN 'rep_same' THEN CAST(a.RepechajeSameDay AS CHAR)
            WHEN 'rep_1_3' THEN CAST(a.Repechaje1a3 AS CHAR)
            WHEN 'rep_4plus' THEN CAST(a.Repechaje4Plus AS CHAR)
            WHEN 'indice_repechaje' THEN
                CASE
                    WHEN a.Canceladas1a3 = 0 THEN '-'
                    ELSE CONCAT(ROUND(100 * a.Repechaje1a3 / a.Canceladas1a3, 1), '%')
                END
            ELSE '-'
        END AS MetricValue
    FROM (
        SELECT
            ClinicID,
            TotalRegistros,
            Movimientos1Plus,
            MovimientosSameDay,
            CitasAgendadas,
            Canceladas4Plus,
            Canceladas1a3,
            CanceladasSameDay,
            CanceladasTotal,
            MalaGestion,
            Concluidas,
            RepechajeSameDay,
            Repechaje1a3,
            Repechaje4Plus
        FROM (
            SELECT
                b.DiaryGClinicID AS ClinicID,
                COUNT(*) AS TotalRegistros,
                SUM(CASE WHEN b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 THEN 1 ELSE 0 END) AS CitasAgendadas,
                SUM(CASE WHEN b.DiaryGDelayed = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays >= 1 THEN 1 ELSE 0 END) AS Movimientos1Plus,
                SUM(CASE WHEN b.DiaryGDelayed = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays <= 0 THEN 1 ELSE 0 END) AS MovimientosSameDay,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays >= 4 THEN 1 ELSE 0 END) AS Canceladas4Plus,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Canceladas1a3,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays <= 0 THEN 1 ELSE 0 END) AS CanceladasSameDay,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 THEN 1 ELSE 0 END) AS CanceladasTotal,
                SUM(CASE WHEN b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.DiaryGNotAttended = 0 AND (b.DiaryGLaserGID IS NULL OR b.DiaryGLaserGID = 0) AND b.DiaryGDate < CURDATE() THEN 1 ELSE 0 END) AS MalaGestion,
                SUM(CASE WHEN b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.DiaryGLaserGID > 0 THEN 1 ELSE 0 END) AS Concluidas,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays <= 0 AND b.HasFuture = 1 THEN 1 ELSE 0 END) AS RepechajeSameDay,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays BETWEEN 1 AND 3 AND b.HasFuture = 1 THEN 1 ELSE 0 END) AS Repechaje1a3,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays >= 4 AND b.HasFuture = 1 THEN 1 ELSE 0 END) AS Repechaje4Plus
            FROM (
                SELECT
                    d.*,
                    DATEDIFF(d.DiaryGDate, COALESCE(DATE(d.DiaryGTrackingStamp), DATE(d.DiaryGCreated), d.DiaryGDate)) AS TrackDays,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM diary_gen d2
                            WHERE d2.DiaryGClientID = d.DiaryGClientID
                              AND d2.DiaryGDate > d.DiaryGDate
                              AND d2.DiaryGNotAttended = 0
                              AND d2.DiaryGLocked = 0
                              AND d2.DiaryGInvisible = 0
                        ) THEN 1
                        ELSE 0
                    END AS HasFuture
                FROM diary_gen d

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
) p
                WHERE d.DiaryGDate BETWEEN p.EffectiveStartDate AND p.EffectiveEndDate
                  AND FIND_IN_SET(d.DiaryGClinicID, p.EffectiveClinicIDs)
            ) b
            GROUP BY b.DiaryGClinicID
        ) a1

        UNION ALL

        SELECT
            0 AS ClinicID,
            SUM(TotalRegistros) AS TotalRegistros,
            SUM(Movimientos1Plus) AS Movimientos1Plus,
            SUM(MovimientosSameDay) AS MovimientosSameDay,
            SUM(CitasAgendadas) AS CitasAgendadas,
            SUM(Canceladas4Plus) AS Canceladas4Plus,
            SUM(Canceladas1a3) AS Canceladas1a3,
            SUM(CanceladasSameDay) AS CanceladasSameDay,
            SUM(CanceladasTotal) AS CanceladasTotal,
            SUM(MalaGestion) AS MalaGestion,
            SUM(Concluidas) AS Concluidas,
            SUM(RepechajeSameDay) AS RepechajeSameDay,
            SUM(Repechaje1a3) AS Repechaje1a3,
            SUM(Repechaje4Plus) AS Repechaje4Plus
        FROM (
            SELECT
                b.DiaryGClinicID AS ClinicID,
                COUNT(*) AS TotalRegistros,
                SUM(CASE WHEN b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 THEN 1 ELSE 0 END) AS CitasAgendadas,
                SUM(CASE WHEN b.DiaryGDelayed = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays >= 1 THEN 1 ELSE 0 END) AS Movimientos1Plus,
                SUM(CASE WHEN b.DiaryGDelayed = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays <= 0 THEN 1 ELSE 0 END) AS MovimientosSameDay,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays >= 4 THEN 1 ELSE 0 END) AS Canceladas4Plus,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Canceladas1a3,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays <= 0 THEN 1 ELSE 0 END) AS CanceladasSameDay,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 THEN 1 ELSE 0 END) AS CanceladasTotal,
                SUM(CASE WHEN b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.DiaryGNotAttended = 0 AND (b.DiaryGLaserGID IS NULL OR b.DiaryGLaserGID = 0) AND b.DiaryGDate < CURDATE() THEN 1 ELSE 0 END) AS MalaGestion,
                SUM(CASE WHEN b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.DiaryGLaserGID > 0 THEN 1 ELSE 0 END) AS Concluidas,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays <= 0 AND b.HasFuture = 1 THEN 1 ELSE 0 END) AS RepechajeSameDay,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays BETWEEN 1 AND 3 AND b.HasFuture = 1 THEN 1 ELSE 0 END) AS Repechaje1a3,
                SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays >= 4 AND b.HasFuture = 1 THEN 1 ELSE 0 END) AS Repechaje4Plus
            FROM (
                SELECT
                    d.*,
                    DATEDIFF(d.DiaryGDate, COALESCE(DATE(d.DiaryGTrackingStamp), DATE(d.DiaryGCreated), d.DiaryGDate)) AS TrackDays,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM diary_gen d2
                            WHERE d2.DiaryGClientID = d.DiaryGClientID
                              AND d2.DiaryGDate > d.DiaryGDate
                              AND d2.DiaryGNotAttended = 0
                              AND d2.DiaryGLocked = 0
                              AND d2.DiaryGInvisible = 0
                        ) THEN 1
                        ELSE 0
                    END AS HasFuture
                FROM diary_gen d

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
) p
                WHERE d.DiaryGDate BETWEEN p.EffectiveStartDate AND p.EffectiveEndDate
                  AND FIND_IN_SET(d.DiaryGClinicID, p.EffectiveClinicIDs)
            ) b
            GROUP BY b.DiaryGClinicID
        ) a2
    ) a
    CROSS JOIN (
        SELECT 1 AS IndicatorOrder, 'Total de Registros en Agenda' AS IndicatorTitle, 'total_registros' AS IndicatorKey
        UNION ALL SELECT 2, 'Movimientos de agenda (1+ dias)', 'mov_1plus'
        UNION ALL SELECT 3, 'Movimientos de agenda (mismo dia)', 'mov_same'
        UNION ALL SELECT 4, 'Indice de retrabajo', 'indice_retrabajo'
        UNION ALL SELECT 5, 'Citas Agendadas (Unicas)', 'citas_agendadas'
        UNION ALL SELECT 6, 'Citas canceladas con anticipacion (4+ dias)', 'cancel_4plus'
        UNION ALL SELECT 7, 'Citas canceladas con gestion (1 a 3 dias)', 'cancel_1_3'
        UNION ALL SELECT 8, 'Citas canceladas sin anticipacion (mismo dia)', 'cancel_same'
        UNION ALL SELECT 9, 'Citas no canceladas y no concluidas (mala gestion)', 'mala_gestion'
        UNION ALL SELECT 10, 'Indice de efectividad de gestion (1 a 3 dias)', 'indice_efectividad'
        UNION ALL SELECT 11, 'Indice de ausentismo', 'indice_ausentismo'
        UNION ALL SELECT 12, 'Citas Concluidas (Laser ID)', 'concluidas'
        UNION ALL SELECT 13, 'Canceladas con agenda futura (mismo dia)', 'rep_same'
        UNION ALL SELECT 14, 'Canceladas con agenda futura (1 a 3 dias)', 'rep_1_3'
        UNION ALL SELECT 15, 'Canceladas con agenda futura (4+ dias)', 'rep_4plus'
        UNION ALL SELECT 16, 'Indice de repechaje (1 a 3 dias)', 'indice_repechaje'
    ) ind
) m
GROUP BY
    m.IndicatorOrder,
    m.IndicatorTitle
ORDER BY
    m.IndicatorOrder;
