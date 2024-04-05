{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="delete from {{this}} where (id_sale, id_customer, date_plan, coalesce (date_visi, '9999-12-31'), coalesce (time_visi, 'NA'), coalesce (visit_end, '9999-12-31'), saleunit, coalesce(trim(time_plan), 'NA')) in (select distinct id_sale, id_customer, try_to_date(date_plan,'yyyymmdd'), cast(coalesce (try_to_date(date_visi,'yyyymmdd'), '9999-12-31') as date) as date_visi, coalesce (time_visi, 'NA') as time_visi, cast(coalesce (try_to_date(visit_end,'yyyymmdd'), '9999-12-31') as date) as visit_end, saleunit, coalesce(trim(time_plan), 'NA') from {{source('thasdl_raw','sdl_la_gt_visit')}})"
    )
}}
with source as (
    select *,
    dense_rank() over(partition by id_sale,id_customer,try_to_date(date_plan,'yyyymmdd'),try_to_date(date_visi,'yyyymmdd'),time_visi,try_to_date(visit_end,'yyyymmdd'),saleunit,time_plan order by filename desc) as rnk
    from {{ source('thasdl_raw', 'sdl_la_gt_visit') }}
),
final as (
    select
        id_sale::varchar(50) as id_sale,
        sale_name::varchar(50) as sale_name,
        id_customer::varchar(50) as id_customer,
        customer_name::varchar(200) as customer_name,
        try_to_date(date_plan,'yyyymmdd') as date_plan,
        trim(time_plan)::varchar(20) as time_plan,
        try_to_date(date_visi,'yyyymmdd') as date_visi,
        time_visi::varchar(20) as time_visi,
        object::varchar(200) as object,
        try_to_date(visit_end,'yyyymmdd') as visit_end,
        visit_time::varchar(20) as visit_time,
        regioncode::varchar(50) as regioncode,
        areacode::varchar(50) as areacode,
        branchcode::varchar(50) as branchcode,
        saleunit::varchar(50) as saleunit,
        time_survey_in::varchar(50) as time_survey_in,
        time_survey_out::varchar(50) as time_survey_out,
        count_survey::varchar(50) as count_survey,
        filename::varchar(50) as filename,
        run_id::varchar(14) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where rnk=1
)
select * from final