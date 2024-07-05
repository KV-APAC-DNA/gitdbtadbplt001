with kesai_m_data_mart_mv_tbl_kizuna as(
    select * from {{ ref('jpndcledw_integration__kesai_m_data_mart_mv_tbl_kizuna') }}
),
final as(
    SELECT kesai_m_data_mart_mv_tbl.saleno_key
        ,kesai_m_data_mart_mv_tbl.saleno
        ,kesai_m_data_mart_mv_tbl.gyono
        ,kesai_m_data_mart_mv_tbl.bun_gyono
        ,kesai_m_data_mart_mv_tbl.meisaikbn
        ,kesai_m_data_mart_mv_tbl.itemcode
        ,kesai_m_data_mart_mv_tbl.setitemnm
        ,kesai_m_data_mart_mv_tbl.bun_itemcode
        ,kesai_m_data_mart_mv_tbl.diid
        ,kesai_m_data_mart_mv_tbl.disetid
        ,kesai_m_data_mart_mv_tbl.suryo
        ,kesai_m_data_mart_mv_tbl.tanka
        ,kesai_m_data_mart_mv_tbl.kingaku
        ,kesai_m_data_mart_mv_tbl.meisainukikingaku
        ,kesai_m_data_mart_mv_tbl.wariritu
        ,kesai_m_data_mart_mv_tbl.hensu
        ,kesai_m_data_mart_mv_tbl.warimaekomitanka
        ,kesai_m_data_mart_mv_tbl.warimaekomikingaku
        ,kesai_m_data_mart_mv_tbl.warimaenukikingaku
        ,kesai_m_data_mart_mv_tbl.meisaitax
        ,kesai_m_data_mart_mv_tbl.dispsaleno
        ,kesai_m_data_mart_mv_tbl.kesaiid
        ,kesai_m_data_mart_mv_tbl.saleno_trim
        ,kesai_m_data_mart_mv_tbl.diorderid
        ,kesai_m_data_mart_mv_tbl.henpinsts
        ,kesai_m_data_mart_mv_tbl.c_dspointitemflg
        ,kesai_m_data_mart_mv_tbl.c_diitemtype
        ,kesai_m_data_mart_mv_tbl.c_diadjustprc
        ,kesai_m_data_mart_mv_tbl.ditotalprc
        ,kesai_m_data_mart_mv_tbl.c_diitemtotalprc
        ,kesai_m_data_mart_mv_tbl.c_didiscountmeisai
        ,kesai_m_data_mart_mv_tbl.bun_suryo
        ,kesai_m_data_mart_mv_tbl.bun_tanka
        ,kesai_m_data_mart_mv_tbl.bun_kingaku
        ,kesai_m_data_mart_mv_tbl.bun_meisainukikingaku
        ,kesai_m_data_mart_mv_tbl.bun_wariritu
        ,kesai_m_data_mart_mv_tbl.bun_hensu
        ,kesai_m_data_mart_mv_tbl.bun_warimaekomitanka
        ,kesai_m_data_mart_mv_tbl.bun_warimaekomikingaku
        ,kesai_m_data_mart_mv_tbl.bun_warimaenukikingaku
        ,kesai_m_data_mart_mv_tbl.bun_meisaitax
        ,kesai_m_data_mart_mv_tbl.maker
        ,kesai_m_data_mart_mv_tbl.kakokbn
        ,NULL AS saleno_p
        ,NULL AS gyono_p
        ,NULL AS bun_gyono_p
    FROM kesai_m_data_mart_mv_tbl_kizuna kesai_m_data_mart_mv_tbl
    WHERE (
            "substring" (
                (kesai_m_data_mart_mv_tbl.saleno)::TEXT
                ,1
                ,1
                ) <> ('O'::CHARACTER VARYING)::TEXT
            )
)
select * from final