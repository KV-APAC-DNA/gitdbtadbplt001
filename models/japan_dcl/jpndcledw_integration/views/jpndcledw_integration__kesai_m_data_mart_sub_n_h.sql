with kesai_m_data_mart_sub_n_h_tbl as(
    select * from {{ ref('jpndcledw_integration__kesai_m_data_mart_sub_n_h_tbl') }}
),
final as(
    SELECT kesai_m_data_mart_sub_n_h_tbl.saleno
        ,kesai_m_data_mart_sub_n_h_tbl.gyono
        ,kesai_m_data_mart_sub_n_h_tbl.meisaikbn
        ,kesai_m_data_mart_sub_n_h_tbl.itemcode
        ,kesai_m_data_mart_sub_n_h_tbl.itemname
        ,kesai_m_data_mart_sub_n_h_tbl.diid
        ,kesai_m_data_mart_sub_n_h_tbl.disetid
        ,kesai_m_data_mart_sub_n_h_tbl.suryo
        ,kesai_m_data_mart_sub_n_h_tbl.tanka
        ,kesai_m_data_mart_sub_n_h_tbl.kingaku
        ,kesai_m_data_mart_sub_n_h_tbl.meisainukikingaku
        ,kesai_m_data_mart_sub_n_h_tbl.wariritu
        ,kesai_m_data_mart_sub_n_h_tbl.warimaekomitanka
        ,kesai_m_data_mart_sub_n_h_tbl.warimaenukikingaku
        ,kesai_m_data_mart_sub_n_h_tbl.warimaekomikingaku
        ,kesai_m_data_mart_sub_n_h_tbl.bun_tanka
        ,kesai_m_data_mart_sub_n_h_tbl.bun_kingaku
        ,kesai_m_data_mart_sub_n_h_tbl.bun_meisainukikingaku
        ,kesai_m_data_mart_sub_n_h_tbl.bun_wariritu
        ,kesai_m_data_mart_sub_n_h_tbl.bun_warimaekomitanka
        ,kesai_m_data_mart_sub_n_h_tbl.bun_warimaenukikingaku
        ,kesai_m_data_mart_sub_n_h_tbl.bun_warimaekomikingaku
        ,kesai_m_data_mart_sub_n_h_tbl.dispsaleno
        ,kesai_m_data_mart_sub_n_h_tbl.kesaiid
        ,kesai_m_data_mart_sub_n_h_tbl.diorderid
        ,kesai_m_data_mart_sub_n_h_tbl.henpinsts
        ,kesai_m_data_mart_sub_n_h_tbl.c_dspointitemflg
        ,kesai_m_data_mart_sub_n_h_tbl.c_diitemtype
        ,kesai_m_data_mart_sub_n_h_tbl.c_diadjustprc
        ,kesai_m_data_mart_sub_n_h_tbl.ditotalprc
        ,kesai_m_data_mart_sub_n_h_tbl.diitemtax
        ,kesai_m_data_mart_sub_n_h_tbl.c_diitemtotalprc
        ,kesai_m_data_mart_sub_n_h_tbl.c_didiscountmeisai
        ,kesai_m_data_mart_sub_n_h_tbl.disetmeisaiid
        ,kesai_m_data_mart_sub_n_h_tbl.c_dssetitemkbn
        ,kesai_m_data_mart_sub_n_h_tbl.maker
    FROM kesai_m_data_mart_sub_n_h_tbl
)
select * from final