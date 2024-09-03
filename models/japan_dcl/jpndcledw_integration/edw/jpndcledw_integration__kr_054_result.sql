{% if build_month_end_job_models()  %}
with KR_054_POINT_HIST as (
select * from {{ ref('jpndcledw_integration__kr_054_point_hist') }}
),
KR_COMM_POINT_PARA as (
select * from {{ source('jpdcledw_integration', 'kr_comm_point_para') }}
),
KR_054_SM_ITEM_MST as (
select * from {{ ref('jpndcledw_integration__kr_054_sm_item_mst') }}
),
KR_054_CEWQ008MNTH_P_MST as (
select * from {{ ref('jpndcledw_integration__kr_054_cewq008mnth_p_mst') }}
),
transformed as (
 SELECT
       No,
       LG_ITEM,
       MD_ITEM,
       SM_KB,
       SM_NM,
       SUM(PT_VAL) as PT_SUM,
       PT_PDT,
       PT_RSN
 FROM(
 SELECT
       '11' as No,
       '期間前ポイント残高' as LG_ITEM,
       '-' as MD_ITEM,
       999 as SM_KB,
       '全社' as SM_NM,
       999 as DISP_SEQ,
       SUM(DAT.DIPOINT) as PT_VAL,
       NULL as PT_PDT,
       DAT.DIVNAME_MAE as PT_RSN
 FROM
       KR_054_POINT_HIST DAT
 WHERE 
       DAT.P_DATE< (select Term_Start from KR_COMM_POINT_PARA)
 GROUP BY
       No,--'11',
       LG_ITEM,--'期間前ポイント残高',
       MD_ITEM,--'-',
       999::INT,
       SM_NM,--'全社',
       999::INT,
       PT_PDT,--NULL,
       DAT.DIVNAME_MAE
 UNION ALL
 SELECT
       '91' as No,
       '期間前ポイント残高' as LG_ITEM,
       '-' as MD_ITEM,
       MST.SM_KB as SM_KB,
       MST.SM_NM as SM_NM,
       MST.SM_KB as DISP_SEQ,
       SUM(DAT.PT_VAL) as PT_VAL,
       NULL as PT_PDT,
       DAT.PT_RSN as PT_RSN
 FROM
       KR_054_SM_ITEM_MST MST
 LEFT JOIN 
 (SELECT
       PHM.DIREGISTDIVCODE as TRK_KB,
       PHM.DIROUTEID as SM_KB,
       PHM.DIPOINT as PT_VAL,
       PHM.DSPREP as PT_PDT,
       PHM.DIVNAME_MAE as PT_RSN
 FROM
       KR_054_POINT_HIST PHM
 WHERE 
       PHM.P_DATE<(select Term_Start from KR_COMM_POINT_PARA) )DAT
  ON
       MST.SM_KB=DAT.SM_KB
 GROUP BY
       No,--'91',
       LG_ITEM,--'期間前ポイント残高',
       MD_ITEM,--'-',
       MST.SM_KB,
       MST.SM_NM,
       MST.SM_KB,
       PT_PDT,--NULL,
       DAT.PT_RSN
 UNION ALL
 SELECT
       '13' as No,
       '期間内増加ポイント' as LG_ITEM,
       CQ8.TRK_KB|| ':'||CQ8.TRK_KBNM as MD_ITEM,
       999 as SM_KB,
       '全社' as SM_NM,
       999 as DISP_SEQ,
       SUM(DAT.PT_VAL) as PT_VAL,
       DAT.PT_PDT as PT_PDT,
       DAT.PT_RSN as PT_RSN
 FROM
       KR_054_CEWQ008MNTH_P_MST CQ8
 LEFT JOIN 
 (SELECT
       PHM.DIREGISTDIVCODE as TRK_KB,
       PHM.DIROUTEID as SM_KB,
       PHM.DIPOINT as PT_VAL,
       PHM.DSPREP as PT_PDT,
       PHM.DIVNAME_MAE as PT_RSN
 FROM
       KR_054_POINT_HIST PHM
 WHERE   
       PHM.P_DATE between (select Term_Start from KR_COMM_POINT_PARA) and (select Term_End from KR_COMM_POINT_PARA)) DAT
  ON
       DAT.TRK_KB=CQ8.TRK_KB AND 
       DAT.SM_KB=CQ8.SM_KB AND 
       DAT.PT_VAL>0
 GROUP BY
       No,--'13',
       LG_ITEM,--'期間内増加ポイント',
       CQ8.TRK_KB|| ':'||CQ8.TRK_KBNM,
       999::INT,
       SM_NM,--'全社',
       999::INT,
       DAT.PT_PDT,
       DAT.PT_RSN
 UNION ALL
 SELECT
       '93' as No,
       '期間内増加ポイント' as LG_ITEM,
       CQ8.TRK_KB|| ':'||CQ8.TRK_KBNM as MD_ITEM,
       CQ8.SM_KB as SM_KB,
       CQ8.SM_NM as SM_NM,
       CQ8.DISP_SEQ as DISP_SEQ,
       SUM(DAT.PT_VAL) as PT_VAL,
       DAT.PT_PDT as PT_PDT,
       DAT.PT_RSN as PT_RSN
 FROM
       KR_054_CEWQ008MNTH_P_MST CQ8
 LEFT JOIN 
 (SELECT
       PHM.DIREGISTDIVCODE as TRK_KB,
       PHM.DIROUTEID as SM_KB,
       PHM.DIPOINT as PT_VAL,
       PHM.DSPREP as PT_PDT,
       PHM.DIVNAME_MAE as PT_RSN
 FROM
       KR_054_POINT_HIST PHM
 WHERE   
       PHM.P_DATE BETWEEN (select Term_Start from KR_COMM_POINT_PARA) and (select Term_End from KR_COMM_POINT_PARA)) DAT
  ON
       DAT.TRK_KB=CQ8.TRK_KB AND 
       DAT.SM_KB=CQ8.SM_KB AND 
       DAT.PT_VAL>0
 GROUP BY
       No,--'93',
       LG_ITEM,--'期間内増加ポイント',
       CQ8.TRK_KB|| ':'||CQ8.TRK_KBNM,
       CQ8.SM_KB,
       CQ8.SM_NM,
       CQ8.DISP_SEQ,
       DAT.PT_PDT,
       DAT.PT_RSN
 UNION ALL
 SELECT
       '14' as NO,
       '期間内減少ポイント' as LG_ITEM,
       CQ8.TRK_KB|| ':'||CQ8.TRK_KBNM as MD_ITEM,
       999 as SM_KB,
       '全社' as SM_NM,
       999 as DISP_SEQ,
       SUM(DAT.PT_VAL) as PT_VAL,
       DAT.PT_PDT as PT_PDT,
       DAT.PT_RSN as PT_RSN
 FROM
       KR_054_CEWQ008MNTH_P_MST CQ8
 LEFT JOIN 
 (SELECT
       PHM.DIREGISTDIVCODE as TRK_KB,
       PHM.DIROUTEID as SM_KB,
       PHM.DIPOINT as PT_VAL,
       PHM.DSPREP as PT_PDT,
       PHM.DIVNAME_MAE as PT_RSN
 FROM
       KR_054_POINT_HIST PHM
 WHERE   
       PHM.P_DATE BETWEEN (select Term_Start from KR_COMM_POINT_PARA) and (select Term_End from KR_COMM_POINT_PARA)) DAT
  ON
       DAT.TRK_KB=CQ8.TRK_KB AND 
       DAT.SM_KB=CQ8.SM_KB AND 
       DAT.PT_VAL<0
 GROUP BY
       No,--'14',
       LG_ITEM,--'期間内減少ポイント',
       CQ8.TRK_KB|| ':'||CQ8.TRK_KBNM,
       999::INT,
       SM_NM,--'全社',
       999::INT,
       DAT.PT_PDT,
       DAT.PT_RSN
 UNION ALL
 SELECT
       '94' as No,
       '期間内減少ポイント' as LG_ITEM,
       CQ8.TRK_KB|| ':'||CQ8.TRK_KBNM as MD_ITEM,
       CQ8.SM_KB as SM_KB,
       CQ8.SM_NM as SM_NM,
       CQ8.DISP_SEQ as DISP_SEQ,
       SUM(DAT.PT_VAL) as PT_VAL,
       DAT.PT_PDT as PT_PDT,
       DAT.PT_RSN as PT_RSN
 FROM
       KR_054_CEWQ008MNTH_P_MST CQ8
 LEFT JOIN 
 (SELECT
       PHM.DIREGISTDIVCODE as TRK_KB,
       PHM.DIROUTEID as SM_KB,
       PHM.DIPOINT as PT_VAL,
       PHM.DSPREP as PT_PDT,
       PHM.DIVNAME_MAE as PT_RSN
 FROM
       KR_054_POINT_HIST PHM
 WHERE   
       PHM.P_DATE BETWEEN (select Term_Start from KR_COMM_POINT_PARA) and (select Term_End from KR_COMM_POINT_PARA)) DAT
  ON
       DAT.TRK_KB=CQ8.TRK_KB AND 
       DAT.SM_KB=CQ8.SM_KB AND 
       DAT.PT_VAL<0
 GROUP BY
       No,--'94',
       LG_ITEM,--'期間内減少ポイント',
       CQ8.TRK_KB|| ':'||CQ8.TRK_KBNM,
       CQ8.SM_KB,
       CQ8.SM_NM,
       CQ8.DISP_SEQ,
       DAT.PT_PDT,
       DAT.PT_RSN
 UNION ALL
 SELECT
       '12' as No,
       '期間末ポイント残高' as LG_ITEM,
       '-' as MD_ITEM,
       999 as SM_KB,
       '全社' as SM_NM,
       999 as DISP_SEQ,
       SUM(DAT.DIPOINT) as PT_VAL,
       NULL as PT_PDT,
       DAT.DIVNAME_MAE as PT_RSN
 FROM
       KR_054_POINT_HIST DAT
 WHERE 
       DAT.P_DATE<= (select Term_End from KR_COMM_POINT_PARA)
 GROUP BY
      No,-- '12',
       LG_ITEM,--'期間末ポイント残高',
       MD_ITEM,--'-',
       999::INT,
       SM_NM,--'全社',
       999::INT,
       PT_PDT,--NULL,
       DAT.DIVNAME_MAE
 UNION ALL
 SELECT
       '92' as No,
       '期間末ポイント残高' as LG_ITEM,
       '-' as MD_ITEM,
       MST.SM_KB as SM_KB,
       MST.SM_NM as SM_NM,
       MST.SM_KB as DISP_SEQ,
       SUM(DAT.PT_VAL) as PT_VAL,
       NULL as PT_PDT,
       DAT.PT_RSN as PT_RSN
 FROM
       KR_054_SM_ITEM_MST MST
 LEFT JOIN 
 (SELECT
       PHM.DIREGISTDIVCODE as TRK_KB,
       PHM.DIROUTEID as SM_KB,
       PHM.DIPOINT as PT_VAL,
       PHM.DSPREP as PT_PDT,
       PHM.DIVNAME_MAE as PT_RSN
 FROM
       KR_054_POINT_HIST PHM
 WHERE 
       PHM.P_DATE<=(select Term_End from KR_COMM_POINT_PARA)) DAT
  ON
       MST.SM_KB=DAT.SM_KB
 GROUP BY
       No,--'92',
       LG_ITEM,--'期間末ポイント残高',
       MD_ITEM,--'-',
       MST.SM_KB,
       MST.SM_NM,
       MST.SM_KB,
       PT_PDT,--NULL,
       DAT.PT_RSN)
 GROUP BY
       No,
       LG_ITEM,
       MD_ITEM,
       SM_KB,
       SM_NM,
       DISP_SEQ,
       PT_PDT,
       PT_RSN
),
final as (
select
no::varchar(15) as no,
lg_item::varchar(60) as lg_item,
md_item::varchar(60) as md_item,
sm_kb::number(38,18) as sm_kb,
sm_nm::varchar(45) as sm_nm,
pt_sum::number(38,18) as pt_sum,
pt_pdt::varchar(15) as pt_pdt,
pt_rsn::varchar(300) as pt_rsn
from transformed
)
select * from final
{% else %}
    select * from {{this}}
{% endif %}