{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}

--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_rg_ecom_plan') }}
),

--Logical CTE

--Final CTE
final as (
    select
        current_timestamp::timestamp_ntz(9) as load_date,
        name::varchar(500) as name,
        code::varchar(500) as code,
        changetrackingmask::number(18,0) as changetrackingmask,
        fiscal_quarter::varchar(4) as fiscal_quarter,
        brand::varchar(40) as brand,
        cluster::varchar(20) as cluster,
        country::varchar(40) as country,
        fisc_month::number(31,0) as fisc_month,
        sub_country::varchar(40) as sub_country,
        fisc_year::number(31,0) as fisc_year,
        franchise::varchar(200) as franchise,
        need_state::varchar(40) as need_state,
        type::varchar(20) as type,
        target_ori::NUMBER(31,2) as target_ori 
    from source
)

--Final select
select * from final
