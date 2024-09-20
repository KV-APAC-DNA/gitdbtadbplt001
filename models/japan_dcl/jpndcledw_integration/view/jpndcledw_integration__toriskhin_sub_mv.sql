with TORISKHIN_SUB_MV_TBL as
(
    select * from {{ ref('jpndcledw_integration__toriskhin_sub_mv_tbl') }}
),final as 
(
SELECT toriskhin_sub_mv_tbl.torisk_cd,
    toriskhin_sub_mv_tbl.hin_cd,
    toriskhin_sub_mv_tbl.torisk_hin_cd
FROM TORISKHIN_SUB_MV_TBL
)
select * from final 
