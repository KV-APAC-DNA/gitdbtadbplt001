with source as(
    select * from {{ ref('pcfwks_integration__wks_au_pharm_ecomm_data') }}
),
final as(
    select 
        cust_group::varchar(10) as cust_group,
        product_probe_id::varchar(20) as product_probe_id,
        product_name::varchar(100) as product_name,
        nec1_desc::varchar(100) as nec1_desc,
        nec2_desc::varchar(100) as nec2_desc,
        nec3_desc::varchar(100) as nec3_desc,
        brand::varchar(50) as brand,
        category::varchar(50) as category,
        owner as owner,
        manufacturer::varchar(50) as manufacturer,
        mat_year::varchar(10) as mat_year,
        time_period::varchar(10) as time_period,
        week_end_dt::date as week_end_dt,
        sales_value::number(10,2) as sales_value,
        sales_qty::number(10,2) as sales_qty,
        crncy::varchar(3) as crncy,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final