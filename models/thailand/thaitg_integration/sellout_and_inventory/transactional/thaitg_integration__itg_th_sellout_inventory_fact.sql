{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['dstrbtr_id', 'rec_dt', 'wh_cd', 'prod_cd']
    )
}}
with itg_th_dtsinventorybal as
(
    select * from {{ source('thaitg_integration', 'itg_th_dtsinventorybal') }}
),
itg_distributor_control as
(
    select * from {{ source('thaitg_integration', 'itg_distributor_control') }}
),
itg_th_dms_inventory_fact as
(
    select * from {{ ref('thaitg_integration__itg_th_dms_inventory_fact') }}
),
itg_th_htc_inventory as
(
    select * from {{ ref('thaitg_integration__itg_th_htc_inventory') }}
),
itg_th_dtsitemmaster as
(
    select * from {{ ref('thaitg_integration__itg_th_dtsitemmaster') }}
),
itg_lookup_retention_period as
(
   select * from {{ source('thaitg_integration', 'itg_lookup_retention_period') }} 
),
union_1 as 
(
    select 
        a.dstrbtr_id,
        a.rec_dt,
        wh_cd,
        prod_cd,
        qty,
        amt,
        mega_brnd,
        brnd,
        base_prod,
        vrnt,
        put_up,
        isupdt_amt,
        src_file,
        crt_date,
        updt_date,
        cdl_dttm,
        null as batchno,
        null as expirydate
    from itg_th_dtsinventorybal a,
        itg_distributor_control b
    where a.dstrbtr_id = b.dstrbtr_id (+)
        and a.rec_dt <= nvl(b.rec_dt, '9999-12-31 00:00:00')
),
transformed as
(
    select 
        dstrbtr_id,
        rec_dt,
        wh_cd,
        prod_cd,
        qty,
        amt,
        mega_brnd,
        brnd,
        base_prod,
        vrnt,
        put_up,
        isupdt_amt,
        src_file,
        crt_date,
        updt_date,
        cdl_dttm,
        null as batchno,
        null as expirydate
    from union_1
    UNION ALL
    select 
        distributorid,
        recdate,
        whcode,
        productcode,
        qty,
        amount,
        null as mega_brnd,
        null as brnd,
        null as base_prod,
        null as vrnt,
        null as put_up,
        null as isupdt_amt,
        null as src_file,
        null as crt_date,
        null as updt_date,
        null as cdl_dttm,
        batchno,
        expirydate
    from itg_th_dms_inventory_fact
    union all
    select 
        distributorid,
        recdate,
        whcode,
        productcode,
        qty,
        amount,
        null AS mega_brnd,
        null AS brnd,
        null AS base_prod,
        null AS vrnt,
        null AS put_up,
        null AS isupdt_amt,
        null AS src_file,
        null AS crt_date,
        null AS updt_date,
        null AS cdl_dttm,
        batchno,
        expirydate
    from itg_th_htc_inventory
),   
final as
(
    select 
        trim(dstrbtr_id)::varchar(10) as dstrbtr_id,
        rec_dt::timestamp_ntz(9) as rec_dt,
        trim(wh_cd)::varchar(20) as wh_cd,
        trim(prod_cd)::varchar(25) as prod_cd,
        qty::number(19,6) as qty,
        amt::number(19,6) as amt,
        trim(mega_brnd)::varchar(10) as mega_brnd,
        trim(brnd)::varchar(10) as brnd,
        trim(base_prod)::varchar(20) as base_prod,
        trim(vrnt)::varchar(10) as vrnt,
        trim(put_up)::varchar(10) as put_up,
        isupdt_amt::number(18,0) as isupdt_amt,
        crt_date::timestamp_ntz(9) as crt_date,
        updt_date::timestamp_ntz(9) as updt_date,
        cdl_dttm::varchar(255) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from transformed
    where trim (prod_cd) not in 
    (
        select trim(item_cd)
        from itg_th_dtsitemmaster
        where grp_cd in ('PM', 'FOC')
    )
    and nvl(rec_dt, (current_timestamp()::date)) >
    (
        select date_trunc(year, current_timestamp::date - retention_years * 365)
        from itg_lookup_retention_period
        where upper(table_name) = 'ITG_TH_SELLOUT_INVENTORY_FACT'
    )
)
select * from final