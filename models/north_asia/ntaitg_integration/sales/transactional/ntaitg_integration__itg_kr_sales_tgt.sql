{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where (fisc_yr, target_type) in (select distinct cast(trim(year_code) as integer) fisc_yr, trim(target_type_code) target_type from {{ source('ntasdl_raw','sdl_mds_kr_target') }});
        {% endif %}"
)}}

with source as (
    select * from {{ source('ntasdl_raw','sdl_mds_kr_target') }}
),
cte1 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '001' as integer) as fisc_yr_per,
        nvl(cast(january as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
cte2 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '002' as integer) as fisc_yr_per,
        nvl(cast(february as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
cte3 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '003' as integer) as fisc_yr_per,
        nvl(cast(march as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
cte4 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '004' as integer) as fisc_yr_per,
        nvl(cast(april as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
cte5 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '005' as integer) as fisc_yr_per,
        nvl(cast(may as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
cte6 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '006' as integer) as fisc_yr_per,
        nvl(cast(june as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
cte7 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '007' as integer) as fisc_yr_per,
        nvl(cast(july as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
cte8 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '008' as integer) as fisc_yr_per,
        nvl(cast(august as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
cte9 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '009' as integer) as fisc_yr_per,
        nvl(cast(september as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
cte10 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '010' as integer) as fisc_yr_per,
        nvl(cast(october as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
cte11 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '011' as integer) as fisc_yr_per,
        nvl(cast(november as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
cte12 as (
    select 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        trim(sales_office_code_code) as sls_ofc_cd,
        trim(sales_office_code_name) as sls_ofc_desc,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code_code) as sls_grp_cd,
        trim(sales_group_code_name) as sls_grp,
        trim(target_type_code) as target_type,
        trim(product_h2_code) as prod_hier_l2,
        trim(product_h4_code) as prod_hier_l4,
        cast(trim(year_code) as integer) as fisc_yr,
        cast(trim(year_code) || '012' as integer) as fisc_yr_per,
        nvl(cast(december as numeric(20, 5)), 0) as target_amt,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
),
transformed as (
    select * from cte1
    union all
    select * from cte2
    union all
    select * from cte3
    union all
    select * from cte4
    union all
    select * from cte5
    union all
    select * from cte6
    union all
    select * from cte7
    union all
    select * from cte8
    union all
    select * from cte9
    union all
    select * from cte10
    union all
    select * from cte11
    union all
    select * from cte12

),
final as (
    select 
        ctry_cd::varchar(2) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        sls_ofc_cd::varchar(4) as sls_ofc_cd,
        sls_ofc_desc::varchar(50) as sls_ofc_desc,
        channel::varchar(50) as channel,
        store_type::varchar(50) as store_type,
        sls_grp_cd::varchar(3) as sls_grp_cd,
        sls_grp::varchar(100) as sls_grp,
        target_type::varchar(4) as target_type,
        prod_hier_l2::varchar(50) as prod_hier_l2,
        prod_hier_l4::varchar(50) as prod_hier_l4,
        fisc_yr::number(18,0) as fisc_yr,
        fisc_yr_per::number(18,0) as fisc_yr_per,
        target_amt::number(20,5) as target_amt,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final