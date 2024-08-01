with edi_item_m as(
    select * from {{ ref('jpnedw_integration__edi_item_m') }}
),
transformed as(
    SELECT edi_item_m.create_dt
        ,edi_item_m.create_user
        ,edi_item_m.update_dt
        ,edi_item_m.update_user
        ,edi_item_m.reg_dt
        ,edi_item_m.item_cd
        ,edi_item_m.item_nm
        ,edi_item_m.iten_nm_kn
        ,edi_item_m.iten_nm_knj
        ,edi_item_m.jan_cd
        ,edi_item_m.itf_cd
        ,edi_item_m.pc
        ,edi_item_m.unt_prc
        ,edi_item_m.sub_frnch
        ,edi_item_m.jan_cd_so
        ,edi_item_m.itf_cd_so
        ,edi_item_m.updateflg
        ,edi_item_m.base_prod
        ,edi_item_m.variant
        ,edi_item_m.put_up
        ,edi_item_m.mega_brnd
        ,edi_item_m.brnd
        ,edi_item_m.dlt_flg
        ,edi_item_m.base_uom
        ,edi_item_m.item_cd_jd
        ,edi_item_m.sap_cstm_type
        ,edi_item_m.mega_brnd_chkflg
        ,edi_item_m.planet_l3_flg
        ,edi_item_m.rel_dt
        ,edi_item_m.discon_dt
        ,edi_item_m.new_prod_type
        ,edi_item_m.prom_goods_flg
        ,edi_item_m.parent_item_cd
        ,edi_item_m.imp_item_flg
        ,edi_item_m.succeeding_item_cd
        ,edi_item_m.ldw_flg01
        ,edi_item_m.ldw_flg02
        ,edi_item_m.ldw_flg03
    FROM edi_item_m
)
select * from transformed