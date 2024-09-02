{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} ITG_RETAILERROUTE USING {{ ref('indwks_integration__wks_csl_retailerroute') }} WKS_CSL_RETAILERROUTE
        WHERE ITG_RETAILERROUTE.distcode = WKS_CSL_RETAILERROUTE.distcode
        AND WKS_CSL_RETAILERROUTE.rtrcode = ITG_RETAILERROUTE.rtrcode
        AND COALESCE(WKS_CSL_RETAILERROUTE.rtrname, 'NA') = COALESCE(ITG_RETAILERROUTE.rtrname, 'NA')
        AND WKS_CSL_RETAILERROUTE.rmcode = ITG_RETAILERROUTE.rmcode
        AND COALESCE(WKS_CSL_RETAILERROUTE.rmname, 'NA') = COALESCE(ITG_RETAILERROUTE.rmname, 'NA')
        AND WKS_CSL_RETAILERROUTE.CHNG_FLG = 'U';
        {% endif %}"
    )
}}

with wks_csl_retailerroute as 
(
    select * from {{ ref('indwks_integration__wks_csl_retailerroute') }}
),
final as 
(
    select
        distcode::varchar(100) as distcode,
        rtrcode::varchar(100) as rtrcode,
        rtrname::varchar(100) as rtrname,
        rmcode::varchar(100) as rmcode,
        rmname::varchar(100) as rmname,
        routetype::varchar(100) as routetype,
        uploadflag::varchar(10) as uploadflag,
        createddate::timestamp_ntz(9) as createddate,
        syncid::number(38,0) as syncid,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        to_date(convert_timezone('UTC', current_timestamp())) as file_rec_dt
    from wks_csl_retailerroute
)
select * from final
