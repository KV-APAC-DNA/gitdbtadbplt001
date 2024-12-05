{{ config(materialized='table') }}
with teikikeiyaku_data_mart_cn as (
    select * from {{ ref('jpndcledw_integration__teikikeiyaku_data_mart_cn') }}
),

teikikeiyaku_data_mart_cl as (
    select *
    from {{ source('jpdcledw_integration', 'teikikeiyaku_data_mart_cl') }}
),

t1 as (
    select
        teikikeiyaku_data_mart_cn.c_diregularcontractid,
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
        '0' as maker,
        teikikeiyaku_data_mart_cn.kaiyakumoushidebi
    from teikikeiyaku_data_mart_cn
),

t2 as (
    select
        teikikeiyaku_data_mart_cl.c_diregularcontractid,
        teikikeiyaku_data_mart_cl.c_diusrid,
        0 as dirouteid,
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
    from teikikeiyaku_data_mart_cl
),

union_of as (
    select * from t1
    union all
    select * from t2
),

final as (
    select
        c_diregularcontractid::NUMBER(38, 0) as c_diregularcontractid,
        c_diusrid::VARCHAR(20) as c_diusrid,
        dirouteid::NUMBER(38, 0) as dirouteid,
        keiyakubi::VARCHAR(150) as keiyakubi,
        shokai_ym::VARCHAR(10) as shokai_ym,
        kaiyakubi::VARCHAR(150) as kaiyakubi,
        c_dsregularmeisaiid::NUMBER(38, 0) as c_dsregularmeisaiid,
        header_flg::VARCHAR(1) as header_flg,
        c_dsdeleveryym::VARCHAR(10) as c_dsdeleveryym,
        dsitemid::VARCHAR(45) as dsitemid,
        c_diregularcourseid::NUMBER(38, 0) as c_diregularcourseid,
        diitemsalesprc::NUMBER(38, 18) as diitemsalesprc,
        c_dsordercreatekbn::VARCHAR(96) as c_dsordercreatekbn,
        c_dscontractchangekbn::VARCHAR(96) as c_dscontractchangekbn,
        c_dicancelflg::VARCHAR(1) as c_dicancelflg,
        kaiyaku_kbn::VARCHAR(96) as kaiyaku_kbn,
        contract_kbn::VARCHAR(150) as contract_kbn,
        diordercode::VARCHAR(19) as diordercode,
        c_dikesaiid::NUMBER(38, 0) as c_dikesaiid,
        dimeisaiid::NUMBER(38, 0) as dimeisaiid,
        maker::VARCHAR(1) as maker,
        kaiyakumoushidebi::VARCHAR(10) as kaiyakumoushidebi
    from union_of
)

select * from final
