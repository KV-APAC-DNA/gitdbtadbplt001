{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        delete from {{this}} ITG_DISTRIBUTORACTIVATION USING {{ ref('indwks_integration__wks_csl_distributoractivation') }} WKS_CSL_DISTRIBUTORACTIVATION
        WHERE ITG_DISTRIBUTORACTIVATION.distcode = WKS_CSL_DISTRIBUTORACTIVATION.distcode
        AND WKS_CSL_DISTRIBUTORACTIVATION.CHNG_FLG = 'U';
        {% endif %}"
    )
}}

with wks_csl_distributoractivation as 
(
    select * from {{ ref('indwks_integration__wks_csl_distributoractivation') }}
),
final as 
(
    select
        distcode::varchar(400) as distcode,
        activefromdate::timestamp_ntz(9) as activefromdate,
        activatedby::number(18,0) as activatedby,
        activatedon::timestamp_ntz(9) as activatedon,
        inactivefromdate::timestamp_ntz(9) as inactivefromdate,
        inactivatedby::number(18,0) as inactivatedby,
        inactivatedon::timestamp_ntz(9) as inactivatedon,
        activestatus::number(18,0) as activestatus,
        createddate::timestamp_ntz(9) as createddate,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        file_name:: varchar(255) as file_name
    from wks_csl_distributoractivation
)
select * from final
