with kesai_h_data_mart_sub_n_h_tbl as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_sub_n_h_tbl') }}
),
final as(
    SELECT kesai_h_data_mart_sub_n_h_tbl.saleno
        ,kesai_h_data_mart_sub_n_h_tbl.juchkbn
        ,kesai_h_data_mart_sub_n_h_tbl.juchym
        ,kesai_h_data_mart_sub_n_h_tbl.juchdate
        ,kesai_h_data_mart_sub_n_h_tbl.kokyano
        ,kesai_h_data_mart_sub_n_h_tbl.hanrocode
        ,kesai_h_data_mart_sub_n_h_tbl.syohanrobunname
        ,kesai_h_data_mart_sub_n_h_tbl.chuhanrobunname
        ,kesai_h_data_mart_sub_n_h_tbl.daihanrobunname
        ,kesai_h_data_mart_sub_n_h_tbl.mediacode
        ,kesai_h_data_mart_sub_n_h_tbl.kessaikbn
        ,kesai_h_data_mart_sub_n_h_tbl.soryo
        ,kesai_h_data_mart_sub_n_h_tbl.tax
        ,kesai_h_data_mart_sub_n_h_tbl.sogokei
        ,kesai_h_data_mart_sub_n_h_tbl.cardcorpcode
        ,kesai_h_data_mart_sub_n_h_tbl.henreasoncode
        ,kesai_h_data_mart_sub_n_h_tbl.cancelflg
        ,kesai_h_data_mart_sub_n_h_tbl.insertdate
        ,kesai_h_data_mart_sub_n_h_tbl.inserttime
        ,kesai_h_data_mart_sub_n_h_tbl.insertid
        ,kesai_h_data_mart_sub_n_h_tbl.updatedate
        ,kesai_h_data_mart_sub_n_h_tbl.updatetime
        ,kesai_h_data_mart_sub_n_h_tbl.zipcode
        ,kesai_h_data_mart_sub_n_h_tbl.todofukencode
        ,kesai_h_data_mart_sub_n_h_tbl.happenpoint
        ,kesai_h_data_mart_sub_n_h_tbl.riyopoint
        ,kesai_h_data_mart_sub_n_h_tbl.shukkasts
        ,kesai_h_data_mart_sub_n_h_tbl.torikeikbn
        ,kesai_h_data_mart_sub_n_h_tbl.tenpocode
        ,kesai_h_data_mart_sub_n_h_tbl.shukaym
        ,kesai_h_data_mart_sub_n_h_tbl.shukadate
        ,kesai_h_data_mart_sub_n_h_tbl.rank
        ,kesai_h_data_mart_sub_n_h_tbl.dispsaleno
        ,kesai_h_data_mart_sub_n_h_tbl.kesaiid
        ,kesai_h_data_mart_sub_n_h_tbl.ordercode
        ,kesai_h_data_mart_sub_n_h_tbl.diorderid
        ,kesai_h_data_mart_sub_n_h_tbl.henreasonname
        ,kesai_h_data_mart_sub_n_h_tbl.uketsukeusrid
        ,kesai_h_data_mart_sub_n_h_tbl.uketsuketelcompanycd
        ,kesai_h_data_mart_sub_n_h_tbl.smkeiroid
        ,kesai_h_data_mart_sub_n_h_tbl.dipromid
        ,kesai_h_data_mart_sub_n_h_tbl.dicollectprc
        ,kesai_h_data_mart_sub_n_h_tbl.ditoujitsuhaisoprc
        ,kesai_h_data_mart_sub_n_h_tbl.didiscountall
        ,kesai_h_data_mart_sub_n_h_tbl.c_didiscountprc
        ,kesai_h_data_mart_sub_n_h_tbl.point_exchange
        ,kesai_h_data_mart_sub_n_h_tbl.henpinsts
        ,kesai_h_data_mart_sub_n_h_tbl.lastupdusrid
        ,kesai_h_data_mart_sub_n_h_tbl.divouchercode
        ,kesai_h_data_mart_sub_n_h_tbl.ditaxrate
        ,kesai_h_data_mart_sub_n_h_tbl.diseikyuremain
        ,kesai_h_data_mart_sub_n_h_tbl.dinyukinsts
        ,kesai_h_data_mart_sub_n_h_tbl.dicardnyukinsts
        ,kesai_h_data_mart_sub_n_h_tbl.disokoid
        ,kesai_h_data_mart_sub_n_h_tbl.dihaisokeitai
    FROM kesai_h_data_mart_sub_n_h_tbl
)
select * from final