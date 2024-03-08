with itg_th_dtssaletrans as
(
    select * from {{ source('thaitg_integration', 'itg_th_dtssaletrans') }}
),
itg_distributor_control as
(
    select * from {{ source('thaitg_integration', 'itg_distributor_control') }}
),
itg_th_htc_sellout as
(
    select * from {{ ref('thaitg_integration__itg_th_htc_sellout') }}
),
itg_th_dtscnreason as
(
    select * from {{ ref('thaitg_integration__itg_th_dtscnreason') }}
),
itg_th_dtsitemmaster as
(
    select * from {{ ref('thaitg_integration__itg_th_dtsitemmaster') }}
),
itg_lookup_retention_period as
(
    select * from {{ source('thaitg_integration', 'itg_lookup_retention_period') }}
),
itg_th_dms_sellout_fact as
(
    select * from {{ ref('thaitg_integration__sdl_raw_th_dms_sellout_fact') }}
),
union_1 as 
(
    SELECT 
        A.dstrbtr_id,
        order_no,
        order_dt,
        ar_cd,
        ar_nm,
        city,
        region,
        ar_typ_cd,
        sls_dist,
        sls_office,
        sls_grp,
        sls_emp,
        sls_nm,
        prod_cd,
        prod_desc,
        mega_brnd,
        brnd,
        base_prod_cd,
        vrnt_cd,
        put_up,
        grs_prc,
        qty,
        subamt1,
        discnt,
        subamt2,
        discnt_bt_ln,
        total_bfr_vat,
        total,
        line_num,
        iscalcredeem,
        redeem_amt,
        iscancel,
        src_file,
        cn_doc_no,
        cn_reason_cd,
        prom_header1,
        prom_header2,
        prom_header3,
        prom_desc1,
        prom_desc2,
        prom_desc3,
        prom_cd1,
        prom_cd2,
        prom_cd3,
        avg_discnt,
        cdl_dttm,
        updt_dt
    from itg_th_dtssaletrans a,
        itg_distributor_control b
    where a.dstrbtr_id = b.dstrbtr_id(+)
        and a.order_dt <= nvl(b.ord_dt, '9999-12-31 00:00:00')
),
union_2 as
(
    SELECT 
        distributorid,
        orderno,
        orderdate,
        arcode,
        arname,
        city,
        region,
        artypecode,
        saledistrict,
        saleoffice,
        salegroup,
        saleemployee,
        salename,
        productcode,
        productdesc,
        megabrand,
        brand,
        baseproduct,
        variant,
        putup,
        grossprice,
        qty,
        subamt1,
        discount,
        subamt2,
        discountbtline,
        totalbeforevat,
        total,
        linenumber,
        null as iscalcredeem,
        null as redeem_amt,
        iscancel,
        null as src_file,
        cndocno,
        cnreasoncode,
        promotionheader1,
        promotionheader2,
        promotionheader3,
        promodesc1,
        promodesc2,
        promodesc3,
        promocode1,
        promocode2,
        promocode3,
        avgdiscount,
        null as cdl_dttm,
        null as updt_dt
    FROM 
    (
        select 
            st.*,
            row_number() over (partition by distributorid,orderno,productcode,arcode,linenumber order by orderdate desc) rn
        from itg_th_dms_sellout_fact st
    )
    where rn = '1'
),
union_3 as
(
    SELECT 
        distributorid,
        orderno,
        orderdate,
        arcode,
        arname,
        city,
        region,
        artypecode,
        saledistrict,
        saleoffice,
        salegroup,
        saleemployee,
        salename,
        productcode,
        productdesc,
        megabrand,
        brand,
        baseproduct,
        variant,
        putup,
        grossprice,
        qty / 12 as qty,
        subamt1,
        discount,
        subamt2,
        discountbtline,
        totalbeforevat,
        total,
        linenumber,
        null as iscalcredeem,
        null as redeem_amt,
        iscancel,
        null as src_file,
        cndocno,
        cnreasoncode,
        promotionheader1,
        promotionheader2,
        promotionheader3,
        promodesc1,
        promodesc2,
        promodesc3,
        promocode1,
        promocode2,
        promocode3,
        avgdiscount,
        null as cdl_dttm,
        null as updt_dt
    from 
    (
        select st.*,
            row_number() over (partition by distributorid,orderno,productcode,arcode,linenumber order by orderdate desc
            ) rn
        from itg_th_htc_sellout st
    )
    where rn = '1'
),
final as
(
    select 
        trim(st.dstrbtr_id)::varchar(10) as dstrbtr_id,
        trim(st.order_no)::varchar(255) as order_no,
        st.order_dt::timestamp_ntz(9) as order_dt,
        st.line_num::number(18,0) as line_num,
        trim(st.ar_cd)::varchar(20) as ar_cd,
        trim(st.ar_nm)::varchar(500) as ar_nm,
        trim(st.city)::varchar(255) as city,
        trim(st.region)::varchar(20) as region,
        trim(st.ar_typ_cd)::varchar(20) as ar_typ_cd,
        trim(st.sls_dist)::varchar(200) as sls_dist,
        trim(st.sls_office)::varchar(255) as sls_office,
        trim(st.sls_grp)::varchar(255) as sls_grp,
        trim(st.sls_emp)::varchar(255) as sls_emp,
        trim(st.sls_nm)::varchar(350) as sls_nm,
        trim(st.prod_cd)::varchar(25) as prod_cd,
        trim(st.prod_desc)::varchar(300) as prod_desc,
        trim(st.mega_brnd)::varchar(10) as mega_brnd,
        trim(st.brnd)::varchar(10) as brnd,
        trim(st.base_prod_cd)::varchar(20) as base_prod_cd,
        trim(st.vrnt_cd)::varchar(10) as vrnt_cd,
        trim(st.put_up)::varchar(10) as put_up,
        st.grs_prc::number(19,6) as grs_prc,
        st.qty::number(19,6) as qty,
        st.subamt1::number(19,6) as subamt1,
        st.discnt::number(19,6) as discnt,
        st.subamt2::number(19,6) as subamt2,
        st.discnt_bt_ln::number(19,6) as discnt_bt_ln,
        st.total_bfr_vat::number(19,6) as total_bfr_vat,
        st.total::number(19,6) as total,
        st.iscalcredeem::number(18,0) as iscalcredeem,
        st.redeem_amt::number(24,6) as redeem_amt,
        st.iscancel::number(18,0) as iscancel,
        trim(st.cn_doc_no)::varchar(255) as cn_doc_no,
        trim(st.cn_reason_cd)::varchar(255) as cn_reason_cd,
        trim(cn.cn_th_desc)::varchar(250) as cn_reason_th_desc,
        trim(cn.cn_en_desc)::varchar(250) as cn_reason_en_desc,
        st.updt_dt::timestamp_ntz(9) as updt_dt,
        trim(st.prom_header1)::varchar(255) as prom_header1,
        trim(st.prom_header2)::varchar(255) as prom_header2,
        trim(st.prom_header3)::varchar(255) as prom_header3,
        trim(st.prom_desc1)::varchar(255) as prom_desc1,
        trim(st.prom_desc2)::varchar(255) as prom_desc2,
        trim(st.prom_desc3)::varchar(255) as prom_desc3,
        trim(st.prom_cd1)::varchar(255) as prom_cd1,
        trim(st.prom_cd2)::varchar(255) as prom_cd2,
        trim(st.prom_cd3)::varchar(255) as prom_cd3,
        st.avg_discnt::number(18,4) as avg_discnt,
        st.cdl_dttm::varchar(255) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from 
    (
        select * from union_1
        UNION ALL
        select * from union_2
        UNION ALL
        select * from union_3
    ) st
    left outer join itg_th_dtscnreason cn on trim (st.cn_reason_cd) = trim (cn.cn_reason)
    where st.iscancel = 0
    and trim(st.prod_cd) not in 
    (
        select trim(item_cd)
        from itg_th_dtsitemmaster
        where grp_cd in ('PM', 'FOC')
    )
    and nvl(order_dt, (current_timestamp()::date)) >
    (
        select date_trunc(year, current_timestamp::date - retention_years * 365)
        from itg_lookup_retention_period
        where upper(table_name) = 'ITG_TH_SELLOUT_SALES_FACT'
    )
)
select * from final