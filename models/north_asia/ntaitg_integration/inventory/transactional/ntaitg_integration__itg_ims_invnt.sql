{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
        unique_key=["invnt_dt", "prod_cd", "ean_num", "ctry_cd", "chng_flg", "dstr_cd"],
        pre_hook="{% if is_incremental() %}
        delete from {{this}} itg_ims_invnt using {{ ref('ntawks_integration__wks_itg_ims_invnt_std') }} wks_itg_ims_invnt_std where coalesce(itg_ims_invnt.invnt_dt,'1/1/1900') = coalesce(wks_itg_ims_invnt_std.invnt_dt,'1/1/1900') and   coalesce(itg_ims_invnt.prod_cd,'#') = coalesce(wks_itg_ims_invnt_std.prod_cd,'#') and   coalesce(itg_ims_invnt.ean_num,'#') = coalesce(wks_itg_ims_invnt_std.ean_num,'#') and   coalesce(itg_ims_invnt.ctry_cd,'#') = coalesce(wks_itg_ims_invnt_std.ctry_cd,'#') and   wks_itg_ims_invnt_std.chng_flg = 'U' and coalesce(itg_ims_invnt.dstr_nm,'#')=coalesce(wks_itg_ims_invnt_std.dstr_nm,'#') and  itg_ims_invnt.dstr_cd in (select distinct distributor_code from {{ source('ntawks_integration', 'tw_ims_distributor_ingestion_metadata') }} where subject_area = 'Stock-Data' and   ctry_cd = 'TW');
        delete from {{this}} as itg_ims_invnt using {{ ref('ntawks_integration__wks_itg_ims_invnt') }} as wks_itg_ims_invnt where coalesce (itg_ims_invnt.invnt_dt, '1/1/1900') = coalesce (wks_itg_ims_invnt.invnt_dt, '1/1/1900') and coalesce (itg_ims_invnt.prod_cd, '#') = coalesce (wks_itg_ims_invnt.prod_cd, '#') and coalesce (itg_ims_invnt.ean_num, '#') = coalesce (wks_itg_ims_invnt.ean_num, '#') and coalesce (itg_ims_invnt.ctry_cd, '#') = coalesce (wks_itg_ims_invnt.ctry_cd, '#') and wks_itg_ims_invnt.chng_flg = 'U' and itg_ims_invnt.dstr_cd = '110256';
        {% endif %}"
    )
}}

with source as(
    select * from {{ ref('ntawks_integration__wks_itg_ims_invnt_std') }}
),
wks_itg_ims_invnt as (
    select * from {{ ref('ntawks_integration__wks_itg_ims_invnt') }}
),
tw_final as(
    select 
       to_date(invnt_dt) as invnt_dt,
       dstr_cd::varchar(30) as dstr_cd,
       dstr_nm::varchar(100) as dstr_nm,
       prod_cd::varchar(20) as prod_cd,
       prod_nm::varchar(200) as prod_nm,
       ean_num::varchar(20) as ean_num,
       cust_nm::varchar(100) as cust_nm,
       invnt_qty::number(21,5) as invnt_qty,
       invnt_amt::number(21,5) as invnt_amt,
       avg_prc_amt::number(21,5) as avg_prc_amt,
       safety_stock::number(21,5) as safety_stock,
       bad_invnt_qty::number(21,5) as bad_invnt_qty,
       book_invnt_qty::number(21,5) as book_invnt_qty,
       convs_amt::number(21,5) as convs_amt,
       prch_disc_amt::number(21,5) as prch_disc_amt,
       end_invnt_qty::number(21,5) as end_invnt_qty,
       batch_no::varchar(20) as batch_no,
       uom::varchar(20) as uom,
       sls_rep_cd::varchar(20) as sls_rep_cd,
       sls_rep_nm::varchar(50) as sls_rep_nm,
       ctry_cd::varchar(2) as ctry_cd,
       crncy_cd::varchar(3) as crncy_cd,
       case when chng_flg = 'I' then current_timestamp()::timestamp_ntz(9) else tgt_crt_dttm end as crt_dttm,
       current_timestamp()::timestamp_ntz(9) as updt_dttm,
       chn_uom::varchar(100) as chn_uom,
       storage_name::varchar(200) as storage_name
    from source
),
hk_final as (
    SELECT invnt_dt::date as invnt_dt,
        dstr_cd::varchar(30) as dstr_cd,
        dstr_nm::varchar(100) as dstr_nm,
        prod_cd::varchar(20) as prod_cd,
        prod_nm::varchar(200) as prod_nm,
        ean_num::varchar(20) as ean_num,
        cust_nm::varchar(100) as cust_nm,
        invnt_qty::number(21,5) as invnt_qty,
        invnt_amt::number(21,5) as invnt_amt,
        avg_prc_amt::number(21,5) as avg_prc_amt,
        safety_stock::number(21,5) as safety_stock,
        bad_invnt_qty::number(21,5) as bad_invnt_qty,
        book_invnt_qty::number(21,5) as book_invnt_qty,
        convs_amt::number(21,5) as convs_amt,
        prch_disc_amt::number(21,5) as prch_disc_amt,
        end_invnt_qty::number(21,5) as end_invnt_qty,
        batch_no::varchar(20) as batch_no,
        uom::varchar(20) as uom,
        sls_rep_cd::varchar(20) as sls_rep_cd,
        sls_rep_nm::varchar(50) as sls_rep_nm,
        ctry_cd::varchar(2) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        case when chng_flg = 'I' then convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) else tgt_crt_dttm end as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
        chn_uom::varchar(100) as chn_uom,
        null::varchar(200) as storage_name
    FROM wks_itg_ims_invnt
),
final as (
    select * from tw_final
    union all
    select * from hk_final
)
select * from final