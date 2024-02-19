{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["need_states"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_needstates_text') }}
),

--Logical CTE

--Final CTE
trans as (
    SELECT
        zneed as need_states,
        langu as language_key,
        txtsh as short_desc,
        txtmd as medium_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
),
final as
(
    select
        need_states::number(18,0) as need_states,
        language_key::varchar(1) as language_key,
        short_desc::varchar(40) as short_desc,
        medium_desc::varchar(40) as medium_desc,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)

--Final select
select * from final
