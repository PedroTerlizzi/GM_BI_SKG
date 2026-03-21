/* ============================================================================
    GROWMETRICA FLOWWW BI - SQL STANDARD TEMPLATE

    REPORT_ID: 18
    REPORT_TITLE: Gestion de Ausentismo - KPIs
    - Enfoque: Indicadores globales del periodo.
    - Base: diary_gen (Agenda)
============================================================================ */

SELECT
    k.KPIID AS `KPIID`,
    k.KPIName AS `KPIName`,
    k.KPIValue AS `KPIValue`,
    TRUE AS `KPIScheme`
FROM (
    SELECT
        1 AS KPIID,
        '% Retrabajo' AS KPIName,
        CASE
            WHEN TotalRegistros = 0 THEN '-'
            ELSE CONCAT(ROUND(100 * (Movimientos1Plus + MovimientosSameDay) / TotalRegistros, 1), '%')
        END AS KPIValue
    FROM (
        SELECT
            COUNT(*) AS TotalRegistros,
            SUM(CASE WHEN b.DiaryGDelayed = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays >= 1 THEN 1 ELSE 0 END) AS Movimientos1Plus,
            SUM(CASE WHEN b.DiaryGDelayed = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays <= 0 THEN 1 ELSE 0 END) AS MovimientosSameDay,
            SUM(CASE WHEN b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 THEN 1 ELSE 0 END) AS CitasAgendadas,
            SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Canceladas1a3,
            SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 THEN 1 ELSE 0 END) AS CanceladasTotal,
            SUM(CASE WHEN b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.DiaryGNotAttended = 0 AND (b.DiaryGLaserGID IS NULL OR b.DiaryGLaserGID = 0) AND b.DiaryGDate < CURDATE() THEN 1 ELSE 0 END) AS MalaGestion,
            SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays <= 0 THEN 1 ELSE 0 END) AS CanceladasSameDay,
            SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays BETWEEN 1 AND 3 AND b.HasFuture = 1 THEN 1 ELSE 0 END) AS Repechaje1a3
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
    ) totals

    UNION ALL

    SELECT
        2 AS KPIID,
        '% Efectividad' AS KPIName,
        CASE
            WHEN CanceladasTotal = 0 THEN '-'
            ELSE CONCAT(ROUND(100 * Canceladas1a3 / CanceladasTotal, 1), '%')
        END AS KPIValue
    FROM (
        SELECT
            SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Canceladas1a3,
            SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 THEN 1 ELSE 0 END) AS CanceladasTotal
        FROM (
            SELECT
                d.*,
                DATEDIFF(d.DiaryGDate, COALESCE(DATE(d.DiaryGTrackingStamp), DATE(d.DiaryGCreated), d.DiaryGDate)) AS TrackDays
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
    ) totals

    UNION ALL

    SELECT
        3 AS KPIID,
        '% Ausentismo' AS KPIName,
        CASE
            WHEN CitasAgendadas = 0 THEN '-'
            ELSE CONCAT(ROUND(100 * (CanceladasSameDay + MalaGestion) / CitasAgendadas, 1), '%')
        END AS KPIValue
    FROM (
        SELECT
            SUM(CASE WHEN b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 THEN 1 ELSE 0 END) AS CitasAgendadas,
            SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays <= 0 THEN 1 ELSE 0 END) AS CanceladasSameDay,
            SUM(CASE WHEN b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.DiaryGNotAttended = 0 AND (b.DiaryGLaserGID IS NULL OR b.DiaryGLaserGID = 0) AND b.DiaryGDate < CURDATE() THEN 1 ELSE 0 END) AS MalaGestion
        FROM (
            SELECT
                d.*,
                DATEDIFF(d.DiaryGDate, COALESCE(DATE(d.DiaryGTrackingStamp), DATE(d.DiaryGCreated), d.DiaryGDate)) AS TrackDays
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
    ) totals

    UNION ALL

    SELECT
        4 AS KPIID,
        '% Repechaje' AS KPIName,
        CASE
            WHEN Canceladas1a3 = 0 THEN '-'
            ELSE CONCAT(ROUND(100 * Repechaje1a3 / Canceladas1a3, 1), '%')
        END AS KPIValue
    FROM (
        SELECT
            SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Canceladas1a3,
            SUM(CASE WHEN b.DiaryGNotAttended = -1 AND b.DiaryGLocked = 0 AND b.DiaryGInvisible = 0 AND b.TrackDays BETWEEN 1 AND 3 AND b.HasFuture = 1 THEN 1 ELSE 0 END) AS Repechaje1a3
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
    ) totals
) k
ORDER BY
    k.KPIID;

