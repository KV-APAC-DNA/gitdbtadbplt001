{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "DELETE FROM {{this}}
                    WHERE YYYYMM =(SELECT TARGET_MONTH FROM {{ ref('jpndcledw_integration__dcl_calendar_sysdate') }} CAL WHERE  CAL.IS_ACTIVE = TRUE AND TO_CHAR(CURR_DATE,'DD')= '20');"
        )
}}

with KR_THIS_STAGE_POINT_DAILY as
(
    select * from {{ ref('jpndcledw_integration__kr_this_stage_point_daily') }}
),
DCL_CALENDAR_SYSDATE as
(
    select * from {{ ref('jpndcledw_integration__dcl_calendar_sysdate') }}
),
final as
(
    SELECT DAILY.YYYYMM::VARCHAR(9)             AS YYYYMM                  -- 作業日-1か月（最新）
        ,LPAD(DAILY.USRID,10,'0')::VARCHAR(30)  AS KOKYANO
        ,DAILY.USRID::NUMBER(38,0)              AS USRID
        ,DAILY.THISTOTALPRC::NUMBER(38,18)      AS THISTOTALPRC            -- 当月累計購入金額
        ,DAILY.STAGE::VARCHAR(18)               AS STAGE                   -- 確定ステージ
        ,DAILY.GOALP ::NUMBER(38,18)            AS THISPOINT               -- 当月時点の当年合（=GOALP）
        ,DAILY.POINT_GRANTED_SUM::NUMBER(38,18) AS PREVPOINT               -- 前月までの当年合計P
        ,DAILY.POINT_CONFIRMED ::NUMBER(38,18)  AS POINT                   -- 付与予定ポイント
        ,DAILY.UPDDATE::VARCHAR(25)             AS INSERTDATE
    FROM KR_THIS_STAGE_POINT_DAILY DAILY,
       DCL_CALENDAR_SYSDATE CAL
    WHERE CAL.IS_ACTIVE = TRUE AND DAILY.YYYYMM = CAL.TARGET_MONTH
        AND TO_CHAR(CAL.CURR_DATE,'DD') = '20'
)
select * from final