with teikikeiyaku_data_mart_cn
as
(
    select * from {{ ref('jpndcledw_integration__teikikeiyaku_data_mart_cn') }}
),
teikikeiyaku_data_mart_cl as
(
  select * from {{ source('jpdcledw_integration', 'teikikeiyaku_data_mart_cl') }}
    ),
t1 as
(
    SELECT teikikeiyaku_data_mart_cn.c_diregularcontractid,
        teikikeiyaku_data_mart_cn.c_diusrid,
        teikikeiyaku_data_mart_cn.dirouteid,
        teikikeiyaku_data_mart_cn.keiyakubi,
        teikikeiyaku_data_mart_cn.shokai_ym,
        teikikeiyaku_data_mart_cn.kaiyakubi,
        teikikeiyaku_data_mart_cn.c_dsregularmeisaiid,
        teikikeiyaku_data_mart_cn.header_flg,
        teikikeiyaku_data_mart_cn.c_dsdeleveryym,
        teikikeiyaku_data_mart_cn.dsitemid,
        teikikeiyaku_data_mart_cn.c_diregularcourseid,
        teikikeiyaku_data_mart_cn.diitemsalesprc,
        teikikeiyaku_data_mart_cn.c_dsordercreatekbn,
        teikikeiyaku_data_mart_cn.c_dscontractchangekbn,
        teikikeiyaku_data_mart_cn.c_dicancelflg,
        teikikeiyaku_data_mart_cn.kaiyaku_kbn,
        teikikeiyaku_data_mart_cn.contract_kbn,
        teikikeiyaku_data_mart_cn.diordercode,
        teikikeiyaku_data_mart_cn.c_dikesaiid,
        teikikeiyaku_data_mart_cn.dimeisaiid,
        '0' AS maker,
        teikikeiyaku_data_mart_cn.kaiyakumoushidebi
    FROM teikikeiyaku_data_mart_cn
),
t2 as
(
    SELECT teikikeiyaku_data_mart_cl.c_diregularcontractid,
        teikikeiyaku_data_mart_cl.c_diusrid,
        0 AS dirouteid,
        teikikeiyaku_data_mart_cl.keiyakubi,
        teikikeiyaku_data_mart_cl.shokai_ym,
        teikikeiyaku_data_mart_cl.kaiyakubi,
        teikikeiyaku_data_mart_cl.c_dsregularmeisaiid,
        teikikeiyaku_data_mart_cl.header_flg,
        teikikeiyaku_data_mart_cl.c_dsdeleveryym,
        teikikeiyaku_data_mart_cl.dsitemid,
        teikikeiyaku_data_mart_cl.c_diregularcourseid,
        teikikeiyaku_data_mart_cl.diitemsalesprc,
        teikikeiyaku_data_mart_cl.c_dsordercreatekbn,
        teikikeiyaku_data_mart_cl.c_dscontractchangekbn,
        teikikeiyaku_data_mart_cl.c_dicancelflg,
        teikikeiyaku_data_mart_cl.kaiyaku_kbn,
        teikikeiyaku_data_mart_cl.contract_kbn,
        teikikeiyaku_data_mart_cl.diordercode,
        teikikeiyaku_data_mart_cl.c_dikesaiid,
        teikikeiyaku_data_mart_cl.dimeisaiid,
        teikikeiyaku_data_mart_cl.maker,
        teikikeiyaku_data_mart_cl.kaiyakumoushidebi
    FROM teikikeiyaku_data_mart_cl
),
union_of as
(
    select * from t1
    union all
    select * from t2
),
final as
(
    select c_diregularcontractid::NUMBER(38,0) AS C_DIREGULARCONTRACTID,
        c_diusrid::VARCHAR(20) AS C_DIUSRID,
        dirouteid::NUMBER(38,0) AS DIROUTEID,
        keiyakubi::VARCHAR(150) AS KEIYAKUBI,
        shokai_ym::VARCHAR(10) AS SHOKAI_YM,
        kaiyakubi::VARCHAR(150) AS KAIYAKUBI,
        c_dsregularmeisaiid::NUMBER(38,0) AS C_DSREGULARMEISAIID,
        header_flg::VARCHAR(1) AS HEADER_FLG,
        c_dsdeleveryym::VARCHAR(10) AS C_DSDELEVERYYM,
        dsitemid::VARCHAR(45) AS DSITEMID,
        c_diregularcourseid::NUMBER(38,0) AS C_DIREGULARCOURSEID,
        diitemsalesprc::NUMBER(38,18) AS DIITEMSALESPRC,
        c_dsordercreatekbn::VARCHAR(96) AS C_DSORDERCREATEKBN,
        c_dscontractchangekbn::VARCHAR(96) AS C_DSCONTRACTCHANGEKBN,
        c_dicancelflg::VARCHAR(1) AS C_DICANCELFLG,
        kaiyaku_kbn::VARCHAR(96) AS KAIYAKU_KBN,
        contract_kbn::VARCHAR(150) AS CONTRACT_KBN,
        diordercode::VARCHAR(19) AS DIORDERCODE,
        c_dikesaiid::NUMBER(38,0) AS C_DIKESAIID,
        dimeisaiid::NUMBER(38,0) AS DIMEISAIID,
        maker::VARCHAR(1) AS MAKER,
        kaiyakumoushidebi::VARCHAR(10) AS KAIYAKUMOUSHIDEBI,
    FROM union_of
)
select * from final