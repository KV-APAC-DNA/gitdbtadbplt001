with wks_metcash_grocery_date as (
    select * from {{ ref('pcfwks_integration__wks_metcash_grocery_date') }}
),
sdl_metcash_ind_grocery as (
select * from DEV_DNA_LOAD.SNAPPCFSDL_RAW.SDL_RAW_METCASH_IND_GROCERY where file_name like '%2024%'
 and  TRIM(SUPP_ID||SUPP_NAME||STATE||BANNER_ID||BANNER||CUSTOMER_ID||CUSTOMER) IS not NULL),
final as (
--week 1 data mapping & conversion

(select to_date(week_end_dt,'DD/MM/YYYY') week_end_dt,

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

from (select *

      from sdl_metcash_ind_grocery

      where gross_sales_wk1 <> 0

      and   trim(gross_sales_wk1) is not null) a

  join (select file_name,

               gross_sales_wk1 as week_end_dt

        from wks_metcash_grocery_date) b on a.file_name = b.file_name



--week 2 data mapping & conversion        

union all

select to_date(week_end_dt,'DD/MM/YYYY') week_end_dt,

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

from (select *

      from sdl_metcash_ind_grocery

      where gross_sales_wk2 <> 0

      and   trim(gross_sales_wk2) is not null) a

  join (select file_name,

               gross_sales_wk2 as week_end_dt

        from wks_metcash_grocery_date) b on a.file_name = b.file_name

        

--week 3 data mapping & conversion

union all

select to_date(week_end_dt,'DD/MM/YYYY') week_end_dt,

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

from (select *

      from sdl_metcash_ind_grocery

      where gross_sales_wk3 <> 0

      and   trim(gross_sales_wk3) is not null) a

  join (select file_name,

               gross_sales_wk3 as week_end_dt

        from wks_metcash_grocery_date) b on a.file_name = b.file_name

        

--week 4 data mapping & conversion

union all

select to_date(week_end_dt,'DD/MM/YYYY') week_end_dt,

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

from (select *

      from sdl_metcash_ind_grocery

      where gross_sales_wk4 <> 0

      and   trim(gross_sales_wk4) is not null) a

  join (select file_name,

               gross_sales_wk4 as week_end_dt

        from wks_metcash_grocery_date) b on a.file_name = b.file_name



--week 5 data mapping & conversion        

union all

select to_date(week_end_dt,'DD/MM/YYYY') week_end_dt,

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

from (select *

      from sdl_metcash_ind_grocery

      where gross_sales_wk5 <> 0

      and   trim(gross_sales_wk5) is not null) a

  join (select file_name,

               gross_sales_wk5 as week_end_dt

        from wks_metcash_grocery_date) b on a.file_name = b.file_name)
)
select * from final



