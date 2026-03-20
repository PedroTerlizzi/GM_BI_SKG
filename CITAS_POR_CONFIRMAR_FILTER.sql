SELECT
    FilterLevel,
    FilterLevelTitle,
    FilterIDs,
    FilterName
FROM (
    -- ==========================================================
    -- NIVEL 2: DOCTORES
    -- ==========================================================
    SELECT
        2000 AS SortOrder,
        2 AS FilterLevel,
        'Doctor' AS FilterLevelTitle,
        '0' AS FilterIDs,
        'Todos los doctores' AS FilterName

    UNION ALL

    SELECT DISTINCT
        2000 + u.UserID AS SortOrder,
        2 AS FilterLevel,
        'Doctor' AS FilterLevelTitle,
        CAST(u.UserID AS CHAR) AS FilterIDs,
        u.UserName AS FilterName
    FROM __x_config_users_view u
    INNER JOIN x_config_users_products up
        ON up.UserProductUserID = u.UserID
    WHERE u.UserDisabled = 0
      AND u.UserID NOT IN (273, 299, 294)
      AND up.UserProductProductID <> 33968
) t
ORDER BY
    SortOrder ASC,
    FilterName ASC;