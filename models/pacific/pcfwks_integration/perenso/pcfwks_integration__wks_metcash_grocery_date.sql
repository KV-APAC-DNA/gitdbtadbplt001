with sdl_metcash_ind_grocery as (
    select * from {{ source('pcfsdl_raw', 'sdl_metcash_ind_grocery') }}
),
final as (
    select file_name,
        gross_sales_wk1,
        gross_sales_wk2,
        gross_sales_wk3,
        gross_sales_wk4,
        gross_sales_wk5,
        gross_cases_wk1,
        gross_cases_wk2,
        gross_cases_wk3,
        gross_cases_wk4,
        gross_cases_wk5
    from sdl_metcash_ind_grocery
    where trim(
            supp_id || supp_name || state || banner_id || banner || customer_id || customer
        ) is null
        and (
            gross_sales_wk1 like '%/%'
            or gross_sales_wk2 like '%/%'
            or gross_sales_wk3 like '%/%'
            or gross_sales_wk4 like '%/%'
            or gross_sales_wk5 like '%/%'
        )
)
select * from final