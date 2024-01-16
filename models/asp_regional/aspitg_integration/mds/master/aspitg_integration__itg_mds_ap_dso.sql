--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_ap_dso') }}
),
--Logical CTE

--Final CTE
final as (
    select
        id::varchar(10) as id,
        code::varchar(10) as code,
        mrc_code::varchar(200) as mrc_code,
        mrc_desc::varchar(200) as mrc_desc,
        year::varchar(10) as year,
        month::varchar(10) as month,
        gts::number(31,2) as gts,
        gross_account_receivable::number(31,2) as gross_account_receivable,
        jnj_days::number(31,0) as jnj_days,
        market::varchar(40) as market,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)

--Final select
select * from final
