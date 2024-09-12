{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['dstrbtr_grp_cd', 'dstrbtr_cust_id', 'planned_visit', 'actual_visit', 'order_no'],
        pre_hook= [
            "{% if is_incremental() %}
                        delete from {{this}} where nvl(dstrbtr_grp_cd,'#') || nvl(dstrbtr_cust_id,'#') || nvl(planned_visit,'9999-12-31') || nvl(actual_visit,'9999-12-31') || nvl(order_no,'#') in (select distinct nvl(dstrbtr_grp_cd,'#') || nvl(cust_id,'#') || nvl(to_date(planned_visit,'YYYYMMDD'),'9999-12-31') || nvl(to_date(actual_visit,'YYYYMMDD'),'9999-12-31') || nvl(order_no,'#') from {{ source('phlsdl_raw', 'sdl_ph_cpg_calls') }}
                        where filename not in (
                        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_cpg_calls__null_test')}})
                        );
            {%endif%}"]
    )
}}

with source as(
    select *, dense_rank() over (partition by nvl(dstrbtr_grp_cd,'#') || nvl(cust_id,'#') || nvl(to_date(planned_visit,'YYYYMMDD'),'9999-12-31') || nvl(to_date(actual_visit,'YYYYMMDD'),'9999-12-31') || nvl(order_no,'#') order by filename desc) rn from {{ source('phlsdl_raw', 'sdl_ph_cpg_calls') }}
    where filename not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_cpg_calls__null_test')}}
    )
    qualify rn=1
),
final as(
    select 
        dstrbtr_grp_cd::varchar(100) as dstrbtr_grp_cd,
        cntry_cd::varchar(100) as cntry_cd,
        to_date(planned_visit,'YYYYMMDD') as planned_visit,
        to_date(actual_visit,'YYYYMMDD') as actual_visit,
        sls_rep_id::varchar(100) as sls_rep_id,
        cust_id::varchar(100) as dstrbtr_cust_id,
        case when len(dstrbtr_cust_id) < 12 then case
                when substring(right ('000000000000' || dstrbtr_cust_id,12),1,3) = '000' then dstrbtr_grp_cd ||substring(right ('000000000000' || dstrbtr_cust_id,12),4,12)
                when substring(right ('000000000000' || dstrbtr_cust_id,12),1,3) = dstrbtr_grp_cd then right ('000000000000' || dstrbtr_cust_id,12)
                else dstrbtr_grp_cd || right ('000000000000' || dstrbtr_cust_id,12)
            end 
            else case
                    when substring(dstrbtr_cust_id,1,3) = '000' then dstrbtr_grp_cd ||substring(dstrbtr_cust_id,4,12)
                    when substring(dstrbtr_cust_id,1,3) = dstrbtr_grp_cd then dstrbtr_cust_id
                    else dstrbtr_grp_cd || dstrbtr_cust_id
            end 
        end::varchar(100) as trnsfrm_cust_id,
        order_no::varchar(100) as order_no,
        reason_no_order::varchar(255) as reason_no_order,
        in_cpg_flag::varchar(10) as in_cpg_flag,
        approved_flag::varchar(10) as approved_flag,
        filename::varchar(255) as filename,
        cdl_dttm::varchar(50) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) crtd_dttm
    from source
)

select * from final