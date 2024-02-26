{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["matl_no"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_matl_text') }}
),

--Logical CTE

--Final CTE
trans as (
    select
        matnr as matl_no,
        spras as lang_key,
        txtmd as matl_desc,
        current_timestamp() as crt_dttm,
        current_timestamp() as updt_dttm
  from source
),
final as(
    select 
        matl_no::varchar(18) as matl_no,
        lang_key::varchar(1) as lang_key,
        matl_desc::varchar(40) as matl_desc,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)

--Final select
select * from final