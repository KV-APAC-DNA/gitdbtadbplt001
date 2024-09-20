with KR_LAST_STAGE_WK_RAW as(
    select * from {{ ref('jpndcledw_integration__kr_last_stage_wk_raw') }}
),
C_TBMEMBUNITREL as(
    select * from {{ ref('jpndclitg_integration__c_tbmembunitrel') }}
),
transformed as(
        SELECT 
        YYYYMM::varchar(9) as yyyymm                                          -- 202112のみ（KR_LAST_STAGE_CPより取得のため）
        ,USRID::number(38,18) as USRID
        ,NVL(THISTOTALPRC, 0)::number(38,18) AS THISTOTALPRC                -- 累計額
        ,CASE WHEN THISTOTALPRC >= 80000 THEN 'ダイヤモンド'
                WHEN THISTOTALPRC >= 50000 THEN 'プラチナ'
                WHEN THISTOTALPRC >= 15000 THEN 'ゴールド'
                ELSE 'レギュラー'
        END::varchar(18) AS STAGE                                        -- ランク再計算
        ,CASE WHEN THISTOTALPRC >= 80000 THEN '04'
                WHEN THISTOTALPRC >= 50000 THEN '03'
                WHEN THISTOTALPRC >= 15000 THEN '02'
                ELSE '01'
        END::varchar(2) AS STAGE_CD                                     -- ステージコード
        ,CASE WHEN THISTOTALPRC >= 80000 THEN 8500
                WHEN THISTOTALPRC >= 50000 THEN 3500
                WHEN THISTOTALPRC >= 15000 THEN 500
                ELSE 0
        END::number(18,0) AS GOALPOINT                                    -- 名寄せ後本来もらえてる到達ポイント
        ,INSERTDATE::varchar(25) as insertdate                                          -- データ作成日
    FROM (
        SELECT YYYYMM
                ,USRID
                ,SUM(THISTOTALPRC) AS THISTOTALPRC             -- 名寄せによる累計額合算
                ,INSERTDATE
        FROM (
        SELECT WK_RAW.YYYYMM
                ,NVL2(TBMEMBUNITREL.C_DIPARENTUSRID, TBMEMBUNITREL.C_DIPARENTUSRID, WK_RAW.USRID) USRID
                ,NVL(WK_RAW.THISTOTALPRC, 0)  THISTOTALPRC
                ,WK_RAW.INSERTDATE
        FROM KR_LAST_STAGE_WK_RAW WK_RAW
        LEFT JOIN C_TBMEMBUNITREL TBMEMBUNITREL      -- ■名寄せテーブル
                ON WK_RAW.USRID = TBMEMBUNITREL.C_DICHILDUSRID
                AND TBMEMBUNITREL.DIELIMFLG = '0'
        )
    GROUP BY
            YYYYMM
            ,USRID
            ,INSERTDATE
)
)
select * from transformed
