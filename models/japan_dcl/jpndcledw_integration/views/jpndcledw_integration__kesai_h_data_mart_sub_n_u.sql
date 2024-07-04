with kesai_h_data_mart_sub_n_u_tbl as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_sub_n_u_tbl') }}
),
final as(
    SELECT kesai_h_data_mart_sub_n_u_tbl.saleno
        ,kesai_h_data_mart_sub_n_u_tbl.juchkbn
        ,kesai_h_data_mart_sub_n_u_tbl.juchym
        ,kesai_h_data_mart_sub_n_u_tbl.juchdate
        ,kesai_h_data_mart_sub_n_u_tbl.kokyano
        ,kesai_h_data_mart_sub_n_u_tbl.hanrocode
        ,kesai_h_data_mart_sub_n_u_tbl.syohanrobunname
        ,kesai_h_data_mart_sub_n_u_tbl.chuhanrobunname
        ,kesai_h_data_mart_sub_n_u_tbl.daihanrobunname
        ,kesai_h_data_mart_sub_n_u_tbl.mediacode
        ,kesai_h_data_mart_sub_n_u_tbl.kessaikbn
        ,kesai_h_data_mart_sub_n_u_tbl.soryo
        ,kesai_h_data_mart_sub_n_u_tbl.tax
        ,kesai_h_data_mart_sub_n_u_tbl.sogokei
        ,kesai_h_data_mart_sub_n_u_tbl.cardcorpcode
        ,kesai_h_data_mart_sub_n_u_tbl.henreasoncode
        ,kesai_h_data_mart_sub_n_u_tbl.cancelflg
        ,kesai_h_data_mart_sub_n_u_tbl.insertdate
        ,kesai_h_data_mart_sub_n_u_tbl.inserttime
        ,kesai_h_data_mart_sub_n_u_tbl.insertid
        ,kesai_h_data_mart_sub_n_u_tbl.updatedate
        ,kesai_h_data_mart_sub_n_u_tbl.updatetime
        ,kesai_h_data_mart_sub_n_u_tbl.zipcode
        ,kesai_h_data_mart_sub_n_u_tbl.todofukencode
        ,kesai_h_data_mart_sub_n_u_tbl.happenpoint
        ,kesai_h_data_mart_sub_n_u_tbl.riyopoint
        ,kesai_h_data_mart_sub_n_u_tbl.shukkasts
        ,kesai_h_data_mart_sub_n_u_tbl.torikeikbn
        ,kesai_h_data_mart_sub_n_u_tbl.tenpocode
        ,kesai_h_data_mart_sub_n_u_tbl.shukaym
        ,kesai_h_data_mart_sub_n_u_tbl.shukadate
        ,kesai_h_data_mart_sub_n_u_tbl.rank
        ,kesai_h_data_mart_sub_n_u_tbl.dispsaleno
        ,kesai_h_data_mart_sub_n_u_tbl.kesaiid
        ,kesai_h_data_mart_sub_n_u_tbl.ordercode
        ,kesai_h_data_mart_sub_n_u_tbl.henreasonname
        ,kesai_h_data_mart_sub_n_u_tbl.uketsukeusrid
        ,kesai_h_data_mart_sub_n_u_tbl.uketsuketelcompanycd
        ,kesai_h_data_mart_sub_n_u_tbl.smkeiroid
        ,kesai_h_data_mart_sub_n_u_tbl.dipromid
        ,kesai_h_data_mart_sub_n_u_tbl.dicollectprc
        ,kesai_h_data_mart_sub_n_u_tbl.ditoujitsuhaisoprc
        ,kesai_h_data_mart_sub_n_u_tbl.didiscountall
        ,kesai_h_data_mart_sub_n_u_tbl.c_didiscountprc
        ,kesai_h_data_mart_sub_n_u_tbl.point_exchange
        ,kesai_h_data_mart_sub_n_u_tbl.lastupdusrid
        ,kesai_h_data_mart_sub_n_u_tbl.divouchercode
        ,kesai_h_data_mart_sub_n_u_tbl.ditaxrate
        ,kesai_h_data_mart_sub_n_u_tbl.diseikyuremain
        ,kesai_h_data_mart_sub_n_u_tbl.dinyukinsts
        ,kesai_h_data_mart_sub_n_u_tbl.dicardnyukinsts
        ,kesai_h_data_mart_sub_n_u_tbl.disokoid
        ,kesai_h_data_mart_sub_n_u_tbl.dihaisokeitai
    FROM kesai_h_data_mart_sub_n_u_tbl
)
select * from final