with kesai_h_data_mart_sub_old_tbl
as 
(
    select * from {{ source('jpdcledw_integration', 'kesai_h_data_mart_sub_old_tbl') }}
),
final as
(
    SELECT saleno::VARCHAR(20) AS saleno,
        juchkbn::VARCHAR(3) AS juchkbn,
        juchym::NUMBER(18,0) AS juchym,
        juchdate::NUMBER(18,0) AS juchdate,
        juchquarter::VARCHAR(3) AS juchquarter,
        juchjigyoki::VARCHAR(60) AS juchjigyoki,
        kokyano::VARCHAR(15) AS kokyano,
        torikeikbn::VARCHAR(3) AS torikeikbn,
        cancelflg::VARCHAR(60) AS cancelflg,
        hanrocode::VARCHAR(60) AS hanrocode,
        syohanrobunname::VARCHAR(60) AS syohanrobunname,
        chuhanrobunname::VARCHAR(60) AS chuhanrobunname,
        daihanrobunname::VARCHAR(60) AS daihanrobunname,
        mediacode::VARCHAR(8) AS mediacode,
        soryo::NUMBER(18,0) AS soryo,
        tax::NUMBER(18,0) AS tax,
        sogokei::NUMBER(18,0) AS sogokei,
        tenpocode::VARCHAR(8) AS tenpocode,
        shukaym::NUMBER(18,0) AS shukaym,
        shukadate::NUMBER(18,0) AS shukadate,
        shukaquarter::VARCHAR(3) AS shukaquarter,
        shukajigyoki::VARCHAR(60) AS shukajigyoki,
        zipcode::VARCHAR(15) AS zipcode,
        todofukencode::VARCHAR(15) AS todofukencode,
        riyopoint::NUMBER(18,0) AS riyopoint,
        happenpoint::NUMBER(18,0) AS happenpoint,
        kessaikbn::VARCHAR(11) AS kessaikbn,
        cardcorpcode::VARCHAR(11) AS cardcorpcode,
        henreasoncode::VARCHAR(5) AS henreasoncode,
        motoinsertid::VARCHAR(15) AS motoinsertid,
        motoinsertdate::NUMBER(18,0) AS motoinsertdate,
        motoupdatedate::NUMBER(18,0) AS motoupdatedate,
        insertdate::NUMBER(18,0) AS insertdate,
        inserttime::NUMBER(18,0) AS inserttime,
        insertid::VARCHAR(15) AS insertid,
        updatedate::NUMBER(18,0) AS updatedate,
        updatetime::NUMBER(18,0) AS updatetime,
        updateid::VARCHAR(9) AS updateid,
        rank::VARCHAR(3) AS rank,
        dispsaleno::VARCHAR(18) AS dispsaleno,
        kesaiid::VARCHAR(20) AS kesaiid,
        ordercode::VARCHAR(20) AS ordercode,
        maker::VARCHAR(2) AS maker,
        todofuken_code::VARCHAR(60) AS todofuken_code,
        okurino::VARCHAR(18) AS okurino,
        shukadate_p::NUMBER(8,0) AS shukadate_p,
        shohingokei::NUMBER(18,0) AS shohingokei,
        komiwarikingaku::NUMBER(18,0) AS komiwarikingaku,
        warimaenukigokei::NUMBER(18,0) AS warimaenukigokei,
        warimaetax::NUMBER(18,0) AS warimaetax,
        transbincode::VARCHAR(3) AS transbincode,
        nyuhenkin::NUMBER(18,0) AS nyuhenkin,
        nyuhenkanflg::NUMBER(18,0) AS nyuhenkanflg,
        kaisha::VARCHAR(15) AS kaisha,
        nukikingaku::NUMBER(18,0) AS nukikingaku,
        kiboudate::NUMBER(18,0) AS kiboudate,
        tokuicode::VARCHAR(15) AS tokuicode,
        shokei::NUMBER(18,0) AS shokei,
        tax_p::NUMBER(12,0) AS tax_p,
        sogokei_p::NUMBER(12,0) AS sogokei_p,
        sendenno::VARCHAR(9) AS sendenno,
        updatedate_p::NUMBER(8,0) AS updatedate_p,
        kkng_kbn::VARCHAR(2) AS kkng_kbn,
        tuka_cd::VARCHAR(15) AS tuka_cd,
        shokei_tuka_tuka::NUMBER(18,0) AS shokei_tuka_tuka,
        tax_tuka::NUMBER(18,0) AS tax_tuka,
        sogokei_tuka::NUMBER(18,0) AS sogokei_tuka,
        transactiontypekbn::VARCHAR(3) AS transactiontypekbn,
        kingaku::NUMBER(18,0) AS kingaku,
        warimaekomikingaku::NUMBER(18,0) AS warimaekomikingaku,
        meisainukikingaku::NUMBER(18,0) AS meisainukikingaku,
        warimaenukikingaku::NUMBER(18,0) AS warimaenukikingaku,
        meisaitax::NUMBER(18,0) AS meisaitax,
        has_bn_kingaku::NUMBER(18,0) AS has_bn_kingaku,
        has_bn_meisainukikingaku::NUMBER(18,0) AS has_bn_meisainukikingaku,
        has_bn_meisaitax::NUMBER(18,0) AS has_bn_meisaitax,
        has_bn_anbunmeisainukikingaku::NUMBER(18,0) AS has_bn_anbunmeisainukikingaku,
        has_bn_kingaku_tuka::NUMBER(18,0) AS has_bn_kingaku_tuka,
        has_bn_meisainukikingaku_tuka::NUMBER(18,0) AS has_bn_meisainukikingaku_tuka,
        has_bn_meisaitax_tuka::NUMBER(18,0) AS has_bn_meisaitax_tuka,
        inserted_date::TIMESTAMP_NTZ(9) AS inserted_date,
        inserted_by::VARCHAR(100) AS inserted_by,
        updated_date::TIMESTAMP_NTZ(9) AS updated_date,
        updated_by::VARCHAR(100) AS updated_by
    FROM kesai_h_data_mart_sub_old_tbl
)
select * from final