{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        delete from {{this}} ITG_SALESMANMASTER USING {{ ref('indwks_integration__wks_csl_salesmanmaster') }} WKS_CSL_SALESMANMASTER
        WHERE ITG_SALESMANMASTER.distcode = WKS_CSL_SALESMANMASTER.distcode
        AND WKS_CSL_SALESMANMASTER.smcode = ITG_SALESMANMASTER.smcode
        AND WKS_CSL_SALESMANMASTER.smname = ITG_SALESMANMASTER.smname
        AND COALESCE(WKS_CSL_SALESMANMASTER.rmname, 'NA') = COALESCE(ITG_SALESMANMASTER.rmname, 'NA')
        AND COALESCE(WKS_CSL_SALESMANMASTER.rmcode, 'NA') = COALESCE(ITG_SALESMANMASTER.rmcode, 'NA')
        AND WKS_CSL_SALESMANMASTER.CHNG_FLG = 'U';
        {% endif %}"
    )
}}

with source as 
(
    select * from {{ ref('indwks_integration__wks_csl_salesmanmaster') }}
),
final as
(
    select
        distcode::varchar(100) as distcode,
        smid::number(18,0) as smid,
        smcode::varchar(100) as smcode,
        smname::varchar(100) as smname,
        smphoneno::varchar(100) as smphoneno,
        smemail::varchar(100) as smemail,
        smotherdetails::varchar(500) as smotherdetails,
        smdailyallowance::number(38,6) as smdailyallowance,
        smmonthlysalary::number(38,6) as smmonthlysalary,
        smmktcredit::number(38,6) as smmktcredit,
        smcreditdays::number(18,0) as smcreditdays,
        status::varchar(20) as status,
        rmid::number(18,0) as rmid,
        rmcode::varchar(100) as rmcode,
        rmname::varchar(100) as rmname,
        uploadflag::varchar(1) as uploadflag,
        createddate::timestamp_ntz(9) as createddate,
        syncid::number(38,0) as syncid,
        rdssmtype::varchar(100) as rdssmtype,
        uniquesalescode::varchar(15) as uniquesalescode,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        to_date(convert_timezone('UTC', current_timestamp())) as file_rec_dt
    from source
)
select * from final
