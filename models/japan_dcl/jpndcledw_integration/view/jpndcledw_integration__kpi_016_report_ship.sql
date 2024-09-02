WITH dm_kesai_mart_dly_bkp_20221021_deployment
AS (
    SELECT *
    FROM {{ source('jpdcledw_integration', 'dm_kesai_mart_dly_bkp_20221021_deployment') }}
    ),
dm_user_status
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__dm_user_status') }} 
    ),
cld_m
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cld_m') }}
    ),
STATUS
AS (
    SELECT 'New_F1'::CHARACTER VARYING AS stat
    
    UNION
    
    SELECT 'New_F2'::CHARACTER VARYING
    
    UNION
    
    SELECT 'Lapsed_F1'::CHARACTER VARYING
    
    UNION
    
    SELECT 'Lapsed_F2'::CHARACTER VARYING
    
    UNION
    
    SELECT 'Existing'::CHARACTER VARYING
    ),
derived_table1
AS (
    SELECT "k".ship_dt,
        "k".channel,
        CASE 
            WHEN ((u.STATUS)::TEXT = ('Existing'::CHARACTER VARYING)::TEXT)
                THEN (u.STATUS)::TEXT
            ELSE (
                    ((u.STATUS)::TEXT || ('_F'::CHARACTER VARYING)::TEXT) || (
                        (
                            CASE 
                                WHEN ("k".f_ship445 >= 2)
                                    THEN (2)::BIGINT
                                ELSE "k".f_ship445
                                END
                            )::CHARACTER VARYING
                        )::TEXT
                    )
            END AS user_status,
        "k".kokyano,
        "k".saleno,
        round(sum("k".total_price)) AS sales
    FROM (
        dm_kesai_mart_dly_bkp_20221021_deployment "k" JOIN dm_user_status u ON (
                (
                    (("k".kokyano)::TEXT = (u.kokyano)::TEXT)
                    AND ("k".ship_dt = (u.dt)::TIMESTAMP without TIME zone)
                    )
                )
        )
    WHERE 
            ((u.base)::TEXT = ('ship445'::CHARACTER VARYING)::TEXT)
            AND ("k".ship_dt >= (dateadd('day',-1095,current_timestamp()))
            )
    GROUP BY "k".ship_dt,
        "k".channel,
        CASE 
            WHEN ((u.STATUS)::TEXT = ('Existing'::CHARACTER VARYING)::TEXT)
                THEN (u.STATUS)::TEXT
            ELSE (
                    ((u.STATUS)::TEXT || ('_F'::CHARACTER VARYING)::TEXT) || (
                        (
                            CASE 
                                WHEN ("k".f_ship445 >= 2)
                                    THEN (2)::BIGINT
                                ELSE "k".f_ship445
                                END
                            )::CHARACTER VARYING
                        )::TEXT
                    )
            END,
        "k".kokyano,
        "k".saleno
    ),
x
AS (
    (
        SELECT derived_table1.ship_dt,
            derived_table1.channel,
            derived_table1.user_status,
            count(DISTINCT derived_table1.kokyano) AS uu,
            count(DISTINCT derived_table1.saleno) AS "# of purchase",
            sum(derived_table1.sales) AS sales
        FROM derived_table1
        WHERE (derived_table1.sales <> ((0)::NUMERIC)::NUMERIC(18, 0))
        GROUP BY derived_table1.ship_dt,
            derived_table1.channel,
            derived_table1.user_status
        )
    ),
    final AS (
    SELECT x.ship_dt,
        x.channel,
        STATUS.stat AS user_status,
        COALESCE(x.uu, (0)::BIGINT) AS uu,
        COALESCE(x."# of purchase", (0)::BIGINT) AS "# of purchase",
        COALESCE(x.sales, ((0)::NUMERIC)::NUMERIC(18, 0)) AS sales,
        (x.ship_dt + 2) AS delivery_dt,
        cal.year_445,
        cal.month_445
    FROM x
    RIGHT JOIN STATUS ON ((x.user_status = (STATUS.stat)::TEXT))
    JOIN cld_m cal ON ((((x.ship_dt + 2))::TIMESTAMP without TIME zone = cal.ymd_dt))
    )
SELECT *
FROM final
