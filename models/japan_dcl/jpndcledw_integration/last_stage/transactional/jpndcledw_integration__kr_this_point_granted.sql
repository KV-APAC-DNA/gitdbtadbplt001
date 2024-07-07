with KR_THIS_STAGE_POINT_MONTHLY as(
    select * from {{ source('jpndcledw_integration', 'kr_this_stage_point_monthly') }}
),
dcl_calendar_sysdate as(
    select * from {{ source('jpndcledw_integration', 'dcl_calendar_sysdate') }}
),
C_TBMEMBUNITREL as(
    select * from {{ ref('jpndclitg_integration__c_tbmembunitrel') }}
),
transformed as(
    SELECT    TN.YYYYMM::varchar(9) as yyyymm
            ,TN.USRID::number(38,0) as usrid
            ,SUM(TN.POINT)::number(38,18) as point  -- 付与済みポイント合計
            ,TN.INSERTDATE::varchar(25) as insertdate                                 -- データ作成日
    FROM
        (
            SELECT  SP.YYYYMM
                    ,NVL2(TBMEMBUNITREL.C_DIPARENTUSRID, TBMEMBUNITREL.C_DIPARENTUSRID, SP.USRID) USRID
                    ,NVL(SP.POINT,0)          POINT
                    ,SP.INSERTDATE
            FROM KR_THIS_STAGE_POINT_MONTHLY SP           -- Monthly確定テーブル
            JOIN dcl_calendar_sysdate cal on cal.is_active = true and 1=1
            LEFT JOIN C_TBMEMBUNITREL TBMEMBUNITREL      -- 名寄せテーブル
            ON SP.USRID = TBMEMBUNITREL.C_DICHILDUSRID
            AND TBMEMBUNITREL.DIELIMFLG = '0'
            WHERE SP.YYYYMM BETWEEN 
            CASE WHEN TO_CHAR(cal.curr_date,'DD') <= 20 THEN TO_CHAR(ADD_MONTHS(cal.curr_date,-1),'YYYY')||'01'
                WHEN TO_CHAR(cal.curr_date,'DD') >  20 THEN TO_CHAR(cal.curr_date,'YYYY')||'01'
            END
            AND
            CASE WHEN TO_CHAR(cal.curr_date,'DD') <= 20 THEN TO_CHAR(ADD_MONTHS(cal.curr_date,-2),'YYYYMM')
                WHEN TO_CHAR(cal.curr_date,'DD') >  20 THEN TO_CHAR(ADD_MONTHS(cal.curr_date,-1),'YYYYMM')
            END
        
        ) TN
    GROUP BY
            TN.YYYYMM
            ,TN.USRID
            ,TN.INSERTDATE    
)
select * from transformed
