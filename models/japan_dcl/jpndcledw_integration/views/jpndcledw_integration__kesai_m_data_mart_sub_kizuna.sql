with kesai_m_data_mart_sub_tbl_kizuna as(
    select * from {{ ref('jpndcledw_integration__kesai_m_data_mart_sub_tbl_kizuna') }}
),
final as(
    SELECT kesai_m_data_mart_sub_tbl.saleno
        ,kesai_m_data_mart_sub_tbl.gyono
        ,kesai_m_data_mart_sub_tbl.meisaikbn
        ,kesai_m_data_mart_sub_tbl.itemcode
        ,kesai_m_data_mart_sub_tbl.itemname
        ,kesai_m_data_mart_sub_tbl.diid
        ,kesai_m_data_mart_sub_tbl.disetid
        ,kesai_m_data_mart_sub_tbl.setitemcd
        ,kesai_m_data_mart_sub_tbl.setitemnm
        ,kesai_m_data_mart_sub_tbl.suryo
        ,kesai_m_data_mart_sub_tbl.tanka
        ,kesai_m_data_mart_sub_tbl.kingaku
        ,kesai_m_data_mart_sub_tbl.meisainukikingaku
        ,kesai_m_data_mart_sub_tbl.wariritu
        ,kesai_m_data_mart_sub_tbl.warimaekomitanka
        ,kesai_m_data_mart_sub_tbl.warimaenukikingaku
        ,kesai_m_data_mart_sub_tbl.warimaekomikingaku
        ,kesai_m_data_mart_sub_tbl.bun_tanka
        ,kesai_m_data_mart_sub_tbl.bun_kingaku
        ,kesai_m_data_mart_sub_tbl.bun_meisainukikingaku
        ,kesai_m_data_mart_sub_tbl.bun_wariritu
        ,kesai_m_data_mart_sub_tbl.bun_warimaekomitanka
        ,kesai_m_data_mart_sub_tbl.bun_warimaenukikingaku
        ,kesai_m_data_mart_sub_tbl.bun_warimaekomikingaku
        ,kesai_m_data_mart_sub_tbl.dispsaleno
        ,kesai_m_data_mart_sub_tbl.kesaiid
        ,kesai_m_data_mart_sub_tbl.diorderid
        ,kesai_m_data_mart_sub_tbl.henpinsts
        ,kesai_m_data_mart_sub_tbl.c_dspointitemflg
        ,kesai_m_data_mart_sub_tbl.c_diitemtype
        ,kesai_m_data_mart_sub_tbl.c_diadjustprc
        ,kesai_m_data_mart_sub_tbl.ditotalprc
        ,kesai_m_data_mart_sub_tbl.diitemtax
        ,kesai_m_data_mart_sub_tbl.c_diitemtotalprc
        ,kesai_m_data_mart_sub_tbl.c_didiscountmeisai
        ,kesai_m_data_mart_sub_tbl.disetmeisaiid
        ,kesai_m_data_mart_sub_tbl.c_dssetitemkbn
        ,kesai_m_data_mart_sub_tbl.maker
    FROM kesai_m_data_mart_sub_tbl_kizuna kesai_m_data_mart_sub_tbl
)
select * from final