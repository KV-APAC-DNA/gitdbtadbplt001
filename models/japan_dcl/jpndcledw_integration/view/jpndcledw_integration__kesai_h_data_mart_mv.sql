with kesai_h_data_mart_mv_tbl as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_mv_tbl') }}
),
final as(
    SELECT kesai_h_data_mart_mv_tbl.saleno_key
        ,kesai_h_data_mart_mv_tbl.saleno
        ,kesai_h_data_mart_mv_tbl.juchkbn
        ,kesai_h_data_mart_mv_tbl.juchym
        ,kesai_h_data_mart_mv_tbl.juchdate
        ,kesai_h_data_mart_mv_tbl.juchquarter
        ,kesai_h_data_mart_mv_tbl.juchjigyoki
        ,kesai_h_data_mart_mv_tbl.kokyano
        ,kesai_h_data_mart_mv_tbl.torikeikbn
        ,kesai_h_data_mart_mv_tbl.cancelflg
        ,kesai_h_data_mart_mv_tbl.hanrocode
        ,kesai_h_data_mart_mv_tbl.syohanrobunname
        ,kesai_h_data_mart_mv_tbl.chuhanrobunname
        ,kesai_h_data_mart_mv_tbl.daihanrobunname
        ,kesai_h_data_mart_mv_tbl.mediacode
        ,kesai_h_data_mart_mv_tbl.soryo
        ,kesai_h_data_mart_mv_tbl.tax
        ,kesai_h_data_mart_mv_tbl.sogokei
        ,kesai_h_data_mart_mv_tbl.tenpocode
        ,kesai_h_data_mart_mv_tbl.shukaym
        ,kesai_h_data_mart_mv_tbl.shukadate
        ,kesai_h_data_mart_mv_tbl.shukaquarter
        ,kesai_h_data_mart_mv_tbl.shukajigyoki
        ,kesai_h_data_mart_mv_tbl.zipcode
        ,kesai_h_data_mart_mv_tbl.todofukencode
        ,kesai_h_data_mart_mv_tbl.riyopoint
        ,kesai_h_data_mart_mv_tbl.happenpoint
        ,kesai_h_data_mart_mv_tbl.kessaikbn
        ,kesai_h_data_mart_mv_tbl.cardcorpcode
        ,kesai_h_data_mart_mv_tbl.henreasoncode
        ,kesai_h_data_mart_mv_tbl.motoinsertid
        ,kesai_h_data_mart_mv_tbl.motoinsertdate
        ,kesai_h_data_mart_mv_tbl.motoupdatedate
        ,kesai_h_data_mart_mv_tbl.insertdate
        ,kesai_h_data_mart_mv_tbl.inserttime
        ,kesai_h_data_mart_mv_tbl.insertid
        ,kesai_h_data_mart_mv_tbl.updatedate
        ,kesai_h_data_mart_mv_tbl.updatetime
        ,kesai_h_data_mart_mv_tbl.updateid
        ,kesai_h_data_mart_mv_tbl.rank
        ,kesai_h_data_mart_mv_tbl.dispsaleno
        ,kesai_h_data_mart_mv_tbl.kesaiid
        ,kesai_h_data_mart_mv_tbl.ordercode
        ,kesai_h_data_mart_mv_tbl.maker
        ,kesai_h_data_mart_mv_tbl.todofuken_code
        ,kesai_h_data_mart_mv_tbl.henreasonname
        ,kesai_h_data_mart_mv_tbl.uketsukeusrid
        ,kesai_h_data_mart_mv_tbl.uketsuketelcompanycd
        ,kesai_h_data_mart_mv_tbl.smkeiroid
        ,kesai_h_data_mart_mv_tbl.dipromid
        ,kesai_h_data_mart_mv_tbl.saleno_trm
        ,kesai_h_data_mart_mv_tbl.dicollectprc
        ,kesai_h_data_mart_mv_tbl.ditoujitsuhaisoprc
        ,kesai_h_data_mart_mv_tbl.didiscountall
        ,kesai_h_data_mart_mv_tbl.c_didiscountprc
        ,kesai_h_data_mart_mv_tbl.point_exchange
        ,kesai_h_data_mart_mv_tbl.logincode
        ,kesai_h_data_mart_mv_tbl.shukkasts
        ,kesai_h_data_mart_mv_tbl.divouchercode
        ,kesai_h_data_mart_mv_tbl.ditaxrate
        ,kesai_h_data_mart_mv_tbl.diseikyuremain
        ,kesai_h_data_mart_mv_tbl.dinyukinsts
        ,kesai_h_data_mart_mv_tbl.dicardnyukinsts
        ,kesai_h_data_mart_mv_tbl.disokoid
        ,kesai_h_data_mart_mv_tbl.dihaisokeitai
        ,kesai_h_data_mart_mv_tbl.shukadate_e AS shukadate_p
        ,kesai_h_data_mart_mv_tbl.kakokbn
        ,(
            CASE 
                WHEN (
                        ((kesai_h_data_mart_mv_tbl.kakokbn)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                        AND ((kesai_h_data_mart_mv_tbl.saleno_key)::TEXT != ('%調整行DUMMY'::CHARACTER VARYING)::TEXT)
                        )
                    THEN '0'::CHARACTER VARYING
                ELSE '1'::CHARACTER VARYING
                END
            )::CHARACTER VARYING(1) AS port_uniq_flg
    FROM kesai_h_data_mart_mv_tbl kesai_h_data_mart_mv_tbl
)
select * from final