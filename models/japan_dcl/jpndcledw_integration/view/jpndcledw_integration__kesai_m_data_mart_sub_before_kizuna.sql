with kesai_m_data_mart_sub_before_tbl as (
select * from {{ ref('jpndcledw_integration__kesai_m_data_mart_sub_before_tbl_kizuna') }}
),

final as (
    SELECT 
        kesai_m_data_mart_sub_before_tbl.saleno, 
        kesai_m_data_mart_sub_before_tbl.gyono, 
        kesai_m_data_mart_sub_before_tbl.meisaikbn, 
        kesai_m_data_mart_sub_before_tbl.itemcode, 
        kesai_m_data_mart_sub_before_tbl.itemname, 
        kesai_m_data_mart_sub_before_tbl.diid, 
        kesai_m_data_mart_sub_before_tbl.disetid, 
        kesai_m_data_mart_sub_before_tbl.setitemcd, 
        kesai_m_data_mart_sub_before_tbl.setitemnm, 
        kesai_m_data_mart_sub_before_tbl.suryo, 
        kesai_m_data_mart_sub_before_tbl.tanka, 
        kesai_m_data_mart_sub_before_tbl.kingaku, 
        kesai_m_data_mart_sub_before_tbl.meisainukikingaku, 
        kesai_m_data_mart_sub_before_tbl.wariritu, 
        kesai_m_data_mart_sub_before_tbl.warimaekomitanka, 
        kesai_m_data_mart_sub_before_tbl.warimaenukikingaku, 
        kesai_m_data_mart_sub_before_tbl.warimaekomikingaku, 
        kesai_m_data_mart_sub_before_tbl.bun_tanka, 
        kesai_m_data_mart_sub_before_tbl.bun_kingaku, 
        kesai_m_data_mart_sub_before_tbl.bun_meisainukikingaku, 
        kesai_m_data_mart_sub_before_tbl.bun_wariritu, 
        kesai_m_data_mart_sub_before_tbl.bun_warimaekomitanka, 
        kesai_m_data_mart_sub_before_tbl.bun_warimaenukikingaku, 
        kesai_m_data_mart_sub_before_tbl.bun_warimaekomikingaku, 
        kesai_m_data_mart_sub_before_tbl.dispsaleno, 
        kesai_m_data_mart_sub_before_tbl.kesaiid, 
        kesai_m_data_mart_sub_before_tbl.diorderid, 
        kesai_m_data_mart_sub_before_tbl.henpinsts, 
        kesai_m_data_mart_sub_before_tbl.c_dspointitemflg, 
        kesai_m_data_mart_sub_before_tbl.c_diitemtype, 
        kesai_m_data_mart_sub_before_tbl.c_diadjustprc, 
        kesai_m_data_mart_sub_before_tbl.ditotalprc, 
        kesai_m_data_mart_sub_before_tbl.diitemtax, 
        kesai_m_data_mart_sub_before_tbl.c_diitemtotalprc, 
        kesai_m_data_mart_sub_before_tbl.c_didiscountmeisai, 
        kesai_m_data_mart_sub_before_tbl.disetmeisaiid, 
        kesai_m_data_mart_sub_before_tbl.c_dssetitemkbn, 
        kesai_m_data_mart_sub_before_tbl.maker 
    FROM kesai_m_data_mart_sub_before_tbl
)
select * from final 