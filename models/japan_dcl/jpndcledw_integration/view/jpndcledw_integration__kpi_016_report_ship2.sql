with dm_user_status as (
select * from {{ ref('jpndcledw_integration__dm_user_status') }}
),
dm_kesai_mart_dly_bkp_20221021_deployment as (
select * from {{ source('jpdcledw_integration', 'dm_kesai_mart_dly_bkp_20221021_deployment') }}
),
cld_m as (
select * from {{ ref('jpndcledw_integration__cld_m') }}
),
final as (
SELECT 
  x.ship_dt, 
  x.channel, 
  status.stat AS user_status, 
  COALESCE(
    x.uu, 
    (0):: bigint
  ) AS uu, 
  COALESCE(
    x."# of purchase", 
    (0):: bigint
  ) AS "# of purchase", 
  COALESCE(
    x.sales, 
    (
      (0):: numeric
    ):: numeric(18, 0)
  ) AS sales, 
  (x.ship_dt + 2) AS delivery_dt, 
  cal.year_445, 
  cal.month_445 
FROM 
  (
    (
      (
        SELECT 
          derived_table1.ship_dt, 
          derived_table1.channel, 
          derived_table1.user_status, 
          count(DISTINCT derived_table1.kokyano) AS uu, 
          count(DISTINCT derived_table1.saleno) AS "# of purchase", 
          sum(derived_table1.sales) AS sales 
        FROM 
          (
            SELECT 
              "k".ship_dt, 
              "k".channel, 
              CASE WHEN (
                (u.status):: text = ('Existing' :: character varying):: text
              ) THEN (u.status):: text ELSE (
                (
                  (u.status):: text || ('_F' :: character varying):: text
                ) || (
                  (
                    CASE WHEN ("k".f_ship445 >= 2) THEN (2):: bigint ELSE "k".f_ship445 END
                  ):: character varying
                ):: text
              ) END AS user_status, 
              "k".kokyano, 
              "k".saleno, 
              round(
                sum("k".total_price)
              ) AS sales 
            FROM 
              (
                dm_kesai_mart_dly_bkp_20221021_deployment "k" 
                JOIN dm_user_status u ON (
                  (
                    (
                      ("k".kokyano):: text = (u.kokyano):: text
                    ) 
                    AND (
                      "k".ship_dt = to_date(
                        (u.dt):: timestamp without time zone
                      )
                    )
                  )
                )
              ) 
            WHERE 
              (
                (
                  (u.base):: text = ('ship445' :: character varying):: text
                ) 
                AND (
                  "k".ship_dt >= (
                    DATEADD(day, -1095, sysdate())
                  )
                )
              ) 
            GROUP BY 
              "k".ship_dt, 
              "k".channel, 
              CASE WHEN (
                (u.status):: text = ('Existing' :: character varying):: text
              ) THEN (u.status):: text ELSE (
                (
                  (u.status):: text || ('_F' :: character varying):: text
                ) || (
                  (
                    CASE WHEN ("k".f_ship445 >= 2) THEN (2):: bigint ELSE "k".f_ship445 END
                  ):: character varying
                ):: text
              ) END, 
              "k".kokyano, 
              "k".saleno
          ) derived_table1 
        WHERE 
          (
            derived_table1.sales <> (
              (0):: numeric
            ):: numeric(18, 0)
          ) 
        GROUP BY 
          derived_table1.ship_dt, 
          derived_table1.channel, 
          derived_table1.user_status
      ) x 
      RIGHT JOIN (
        (
          (
            (
              SELECT 
                'New_F1' :: character varying AS stat 
              UNION 
              SELECT 
                'New_F2' :: character varying
            ) 
            UNION 
            SELECT 
              'Lapsed_F1' :: character varying
          ) 
          UNION 
          SELECT 
            'Lapsed_F2' :: character varying
        ) 
        UNION 
        SELECT 
          'Existing' :: character varying
      ) status ON (
        (
          x.user_status = (status.stat):: text
        )
      )
    ) 
    JOIN cld_m cal ON (
      (
        (
          (x.ship_dt + 2)
        ):: timestamp without time zone = cal.ymd_dt
      )
    )
  )
)
select * from final