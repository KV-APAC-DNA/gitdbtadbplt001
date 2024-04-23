{{
    config
    (
        pre_hook="{{ build_edw_ps_msl_items()}}"
    )
}}
with sdl_mds_pacific_ps_msl as
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_ps_msl') }}
),
wks as
(
    select distinct  -- records in sdl and edw are same where latest record = y
       edw_ps.ean,
       edw_ps.retail_environment,
       sdl_mds.msl_flag,
       edw_ps.latest_record,
       edw_ps.valid_from,
       edw_ps.valid_to,
       edw_ps.crt_dttm,
       current_timestamp as updt_dttm,
       sdl_mds.msl_rank,
       sdl_mds.store_grade_a,       
       sdl_mds.store_grade_b,
       sdl_mds.store_grade_c,
       sdl_mds.store_grade_d
    from {{this}} edw_ps, 
    sdl_mds_pacific_ps_msl sdl_mds
    where edw_ps.latest_record = 'Y'
    and ltrim(edw_ps.ean,0) || upper(edw_ps.retail_environment) ||  edw_ps.msl_flag ||  edw_ps.msl_rank || 
    (case when edw_ps.store_grade_a is null or edw_ps.store_grade_a = '' then 'NA' else edw_ps.store_grade_a end) || 
    (case when edw_ps.store_grade_b is null or edw_ps.store_grade_b = '' then 'NA' else edw_ps.store_grade_b end) || 
    (case when edw_ps.store_grade_c is null or edw_ps.store_grade_c = '' then 'NA' else edw_ps.store_grade_c end) || 
    (case when edw_ps.store_grade_d is null or edw_ps.store_grade_d = '' then 'NA' else edw_ps.store_grade_d end)
    = ltrim(sdl_mds.ean,0) ||
    upper(sdl_mds.retail_environment) || sdl_mds.msl_flag || sdl_mds.msl_rank || 
    (case when sdl_mds.store_grade_a is null or sdl_mds.store_grade_a = '' then 'NA' else sdl_mds.store_grade_a end) || 
    (case when sdl_mds.store_grade_b is null or sdl_mds.store_grade_b = '' then 'NA' else sdl_mds.store_grade_b end) || 
    (case when sdl_mds.store_grade_c is null or sdl_mds.store_grade_c = '' then 'NA' else sdl_mds.store_grade_c end) || 
    (case when sdl_mds.store_grade_d is null or sdl_mds.store_grade_d = '' then 'NA' else sdl_mds.store_grade_d end)

    union all

    select distinct -- records present in edw (latest record = y ) but not in sdl
        edw_ps.ean,
        edw_ps.retail_environment,
        edw_ps.msl_flag,
        'N' as latest_record,
        edw_ps.valid_from,
        current_timestamp as valid_to,
        edw_ps.crt_dttm,
        current_timestamp as updt_dttm,
        edw_ps.msl_rank,
        edw_ps.store_grade_a,
        edw_ps.store_grade_b,
        edw_ps.store_grade_c,
        edw_ps.store_grade_d
    from {{this}} edw_ps
    where edw_ps.latest_record = 'Y'
    and ltrim(edw_ps.ean,0) ||upper(edw_ps.retail_environment) || edw_ps.msl_flag || edw_ps.msl_rank || 
    (case when edw_ps.store_grade_a is null or edw_ps.store_grade_a = '' then 'NA' else edw_ps.store_grade_a end) || 
    (case when edw_ps.store_grade_b is null or edw_ps.store_grade_b = '' then 'NA' else edw_ps.store_grade_b end) || 
    (case when edw_ps.store_grade_c is null or edw_ps.store_grade_c = '' then 'NA' else edw_ps.store_grade_c end) || 
    (case when edw_ps.store_grade_d is null or edw_ps.store_grade_d = '' then 'NA' else edw_ps.store_grade_d end)
    not in (select * from (select distinct ltrim(sdl_mds.ean,0) ||upper(sdl_mds.retail_environment) || sdl_mds.msl_flag ||  sdl_mds.msl_rank  || 
    (case when sdl_mds.store_grade_a is null or sdl_mds.store_grade_a = '' then 'NA' else sdl_mds.store_grade_a end) || 
    (case when sdl_mds.store_grade_b is null or sdl_mds.store_grade_b = '' then 'NA' else sdl_mds.store_grade_b end) || 
    (case when sdl_mds.store_grade_c is null or sdl_mds.store_grade_c = '' then 'NA' else sdl_mds.store_grade_c end) || 
    (case when sdl_mds.store_grade_d is null or sdl_mds.store_grade_d = '' then 'NA' else sdl_mds.store_grade_d end) as pri_key
                from sdl_mds_pacific_ps_msl sdl_mds) where pri_key is not null)

    
    union all		   

    select distinct --- record present in sdl but not in edw
        cast(sdl_mds.ean as varchar) as ean,
        sdl_mds.retail_environment,
        sdl_mds.msl_flag,
        'Y' as latest_record,
        current_timestamp as valid_from,
        to_date('2099-12-31','yyyy-mm-dd') as valid_to,
        current_timestamp as crt_dttm,
        current_timestamp as updt_dttm,
        sdl_mds.msl_rank,
        sdl_mds.store_grade_a ,
        sdl_mds.store_grade_b,
        sdl_mds.store_grade_c,
        sdl_mds.store_grade_d
    from sdl_mds_pacific_ps_msl sdl_mds
    where ltrim(sdl_mds.ean,0) ||upper(sdl_mds.retail_environment) || sdl_mds.msl_flag || sdl_mds.msl_rank || 
    (case when sdl_mds.store_grade_a is null or sdl_mds.store_grade_a = '' then 'NA' else sdl_mds.store_grade_a end) || 
    (case when sdl_mds.store_grade_b is null or sdl_mds.store_grade_b = '' then 'NA' else sdl_mds.store_grade_b end) || 
    (case when sdl_mds.store_grade_c is null or sdl_mds.store_grade_c = '' then 'NA' else sdl_mds.store_grade_c end) || 
    (case when sdl_mds.store_grade_d is null or sdl_mds.store_grade_d = '' then 'NA' else sdl_mds.store_grade_d end)
    not in (select distinct ltrim(edw_ps.ean,0) ||upper(edw_ps.retail_environment) || edw_ps.msl_flag || edw_ps.msl_rank || 
    (case when edw_ps.store_grade_a is null or edw_ps.store_grade_a = '' then 'NA' else edw_ps.store_grade_a end) || 
    (case when edw_ps.store_grade_b is null or edw_ps.store_grade_b = '' then 'NA' else edw_ps.store_grade_b end) || 
    (case when edw_ps.store_grade_c is null or edw_ps.store_grade_c = '' then 'NA' else edw_ps.store_grade_c end) || 
    (case when edw_ps.store_grade_d is null or edw_ps.store_grade_d = '' then 'NA' else edw_ps.store_grade_d end)
                from {{this}} edw_ps
                )

    
    union all

    select distinct -- record present in sdl and edw both but in edw latest_record is n
        cast(sdl_mds.ean as varchar) as ean,
        sdl_mds.retail_environment,
        sdl_mds.msl_flag,
        'Y' as latest_record,
        current_timestamp as valid_from,
        to_date('2099-12-31','yyyy-mm-dd') as valid_to,
        current_timestamp as crt_dttm,
        current_timestamp as updt_dttm,
        sdl_mds.msl_rank,
        sdl_mds.store_grade_a,
        sdl_mds.store_grade_b,
        sdl_mds.store_grade_c,
        sdl_mds.store_grade_d
    from sdl_mds_pacific_ps_msl sdl_mds
    where ltrim(sdl_mds.ean,0) ||upper(sdl_mds.retail_environment) || sdl_mds.msl_flag || sdl_mds.msl_rank || 
    (case when sdl_mds.store_grade_a is null or sdl_mds.store_grade_a = '' then 'NA' else sdl_mds.store_grade_a end) || 
    (case when sdl_mds.store_grade_b is null or sdl_mds.store_grade_b = '' then 'NA' else sdl_mds.store_grade_b end) || 
    (case when sdl_mds.store_grade_c is null or sdl_mds.store_grade_c = '' then 'NA' else sdl_mds.store_grade_c end) || 
    (case when sdl_mds.store_grade_d is null or sdl_mds.store_grade_d = '' then 'NA' else sdl_mds.store_grade_d end)
    in (select distinct ltrim(edw_ps.ean,0) ||upper(edw_ps.retail_environment) || edw_ps.msl_flag || edw_ps.msl_rank ||
    (case when edw_ps.store_grade_a is null or edw_ps.store_grade_a = '' then 'NA' else edw_ps.store_grade_a end) || 
    (case when edw_ps.store_grade_b is null or edw_ps.store_grade_b = '' then 'NA' else edw_ps.store_grade_b end) || 
    (case when edw_ps.store_grade_c is null or edw_ps.store_grade_c = '' then 'NA' else edw_ps.store_grade_c end) || 
    (case when edw_ps.store_grade_d is null or edw_ps.store_grade_d = '' then 'NA' else edw_ps.store_grade_d end)
                from {{this}} edw_ps
                where latest_record = 'N' )

    union all			

    select distinct -- records from edw with latest_record = n
        edw_ps.ean,
        edw_ps.retail_environment,
        edw_ps.msl_flag,
        'N' as latest_record,
        edw_ps.valid_from,
        edw_ps.valid_to,
        edw_ps.crt_dttm,
        edw_ps.updt_dttm,---ch
        edw_ps.msl_rank,
        edw_ps.store_grade_a,
        edw_ps.store_grade_b,
        edw_ps.store_grade_c,
        edw_ps.store_grade_d
    from {{this}} edw_ps
    where edw_ps.latest_record = 'N'
),
final as
(
    select 
        ean::varchar(40) as ean,
        retail_environment::varchar(100) as retail_environment,
        msl_flag::varchar(5) as msl_flag,
        latest_record::varchar(5) as latest_record,
        valid_from::date as valid_from,
        valid_to::date as valid_to,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        msl_rank::number(15,0) as msl_rank,
        store_grade_a::varchar(5) as store_grade_a,
        store_grade_b::varchar(5) as store_grade_b,
        store_grade_c::varchar(5) as store_grade_c,
        store_grade_d::varchar(5) as store_grade_d,
     from wks
)
select * from final