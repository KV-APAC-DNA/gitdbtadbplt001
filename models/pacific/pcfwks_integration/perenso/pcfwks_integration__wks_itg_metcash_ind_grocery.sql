with wks_metcash_grocery_date as (
    select *
    from { { ref('pcfwks_integration__wks_metcash_grocery_date') } }
),
sdl_metcash_ind_grocery as (
    select * from {{ source('pcfsdl_raw', 'sdl_metcash_ind_grocery') }}
),
transformed as (
    --week 1 data mapping & conversion
    (
        select to_date(week_end_dt, 'DD/MM/YYYY') week_end_dt,
            supp_id,
            supp_name,
            state,
            banner_id,
            banner,
            customer_id,
            customer,
            product_id,
            product,
            cast(gross_sales_wk1 as float4) as gross_sales,
            cast(gross_cases_wk1 as float4) as gross_cases,
            a.file_name,
            run_id,
            create_dt
        from (
                select *
                from sdl_metcash_ind_grocery
                where gross_sales_wk1 <> 0
                    and trim(gross_sales_wk1) is not null
            ) a
            join (
                select file_name,
                    gross_sales_wk1 as week_end_dt
                from wks_metcash_grocery_date
            ) b on a.file_name = b.file_name --week 2 data mapping & conversion
        union all
        select to_date(week_end_dt, 'DD/MM/YYYY') week_end_dt,
            supp_id,
            supp_name,
            state,
            banner_id,
            banner,
            customer_id,
            customer,
            product_id,
            product,
            cast(gross_sales_wk2 as float4) as gross_sales,
            cast(gross_cases_wk2 as float4) as gross_cases,
            a.file_name,
            run_id,
            create_dt
        from (
                select *
                from sdl_metcash_ind_grocery
                where gross_sales_wk2 <> 0
                    and trim(gross_sales_wk2) is not null
            ) a
            join (
                select file_name,
                    gross_sales_wk2 as week_end_dt
                from wks_metcash_grocery_date
            ) b on a.file_name = b.file_name --week 3 data mapping & conversion
        union all
        select to_date(week_end_dt, 'DD/MM/YYYY') week_end_dt,
            supp_id,
            supp_name,
            state,
            banner_id,
            banner,
            customer_id,
            customer,
            product_id,
            product,
            cast(gross_sales_wk3 as float4) as gross_sales,
            cast(gross_cases_wk3 as float4) as gross_cases,
            a.file_name,
            run_id,
            create_dt
        from (
                select *
                from sdl_metcash_ind_grocery
                where gross_sales_wk3 <> 0
                    and trim(gross_sales_wk3) is not null
            ) a
            join (
                select file_name,
                    gross_sales_wk3 as week_end_dt
                from wks_metcash_grocery_date
            ) b on a.file_name = b.file_name --week 4 data mapping & conversion
        union all
        select to_date(week_end_dt, 'DD/MM/YYYY') week_end_dt,
            supp_id,
            supp_name,
            state,
            banner_id,
            banner,
            customer_id,
            customer,
            product_id,
            product,
            cast(gross_sales_wk4 as float4) as gross_sales,
            cast(gross_cases_wk4 as float4) as gross_cases,
            a.file_name,
            run_id,
            create_dt
        from (
                select *
                from sdl_metcash_ind_grocery
                where gross_sales_wk4 <> 0
                    and trim(gross_sales_wk4) is not null
            ) a
            join (
                select file_name,
                    gross_sales_wk4 as week_end_dt
                from wks_metcash_grocery_date
            ) b on a.file_name = b.file_name --week 5 data mapping & conversion
        union all
        select to_date(week_end_dt, 'DD/MM/YYYY') week_end_dt,
            supp_id,
            supp_name,
            state,
            banner_id,
            banner,
            customer_id,
            customer,
            product_id,
            product,
            cast(gross_sales_wk5 as float4) as gross_sales,
            cast(gross_cases_wk5 as float4) as gross_cases,
            a.file_name,
            run_id,
            create_dt
        from (
                select *
                from sdl_metcash_ind_grocery
                where gross_sales_wk5 <> 0
                    and trim(gross_sales_wk5) is not null
            ) a
            join (
                select file_name,
                    gross_sales_wk5 as week_end_dt
                from wks_metcash_grocery_date
            ) b on a.file_name = b.file_name
    )
),
final as (
    select week_end_dt::date as week_end_dt,
        supp_id::varchar(20) as supp_id,
        supp_name::varchar(50) as supp_name,
        state::varchar(50) as state,
        banner_id::varchar(10) as banner_id,
        banner::varchar(50) as banner,
        customer_id::varchar(20) as customer_id,
        customer::varchar(50) as customer,
        product_id::varchar(20) as product_id,
        product::varchar(50) as product,
        gross_sales::float as gross_sales,
        gross_cases::float as gross_cases,
        file_name::varchar(100) as file_name,
        run_id::varchar(50) as run_id,
        create_dt::timestamp_ntz(9) as create_dt
    from transformed
)
select *
from final