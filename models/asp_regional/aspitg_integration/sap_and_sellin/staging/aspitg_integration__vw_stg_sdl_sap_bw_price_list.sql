
with source as(
    select * from {{ source('bwa_access', 'bwa_list_price') }}
),
final as(
    select
        iff(replace(salesorg,'\"','')='',null,replace(salesorg,'\"','')) as sls_org,
        iff(replace(material,'\"','')='',null,replace(material,'\"','')) as material,
        iff(replace(condrecno,'\"','')='',null,replace(condrecno,'\"','')) as cond_rec_no,
        iff(replace(matl_group,'\"','')='',null,replace(matl_group,'\"','')) as matl_grp,
        iff(replace(validto,'\"','')='',null,replace(validto,'\"','')) as valid_to,
        iff(replace(knart,'\"','')='',null,replace(knart,'\"','')) as knart,
        iff(replace(datefrom,'\"','')='',null,replace(datefrom,'\"','')) as dt_from,
        iff(replace(amount,'\"','')='',null,replace(amount,'\"',''))::decimal(18,4) as amount,
        iff(replace(currency,'\"','')='',null,replace(currency,'\"','')) as currency,
        iff(replace(unit,'\"','')='',null,replace(unit,'\"','')) as unit,
        iff(replace(recordmode,'\"','')='',null,replace(recordmode,'\"','')) as record_mode,
        iff(replace(comp_code,'\"','')='',null,replace(comp_code,'\"','')) as comp_cd,
        iff(replace(price_unit,'\"','')='',null,replace(price_unit,'\"','')) as price_unit,
        iff(replace(bic_zcurrfpa,'\"','')='',null,replace(bic_zcurrfpa,'\"','')) as zcurrfpa,
        _ingestiontimestamp_ as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as curr_dt,
        null as file_name
    from source

)
select * from final