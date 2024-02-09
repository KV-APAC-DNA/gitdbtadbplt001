{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["period"]
        )
}}
--Import CTE
with source as 
(
    select *
    from {{ source('sgpsdl_raw', 'sdl_sg_zuellig_sellout') }}
),
itg_sg_zuellig_product_mapping as
(
    select * from {{ ref('sgpitg_integration__itg_sg_zuellig_product_mapping') }}
),
itg_sg_zuellig_customer_mapping as
(
    select * from {{ ref('sgpitg_integration__itg_sg_zuellig_customer_mapping') }}
),
wks_sg_listprice as
(
    select * from {{ ref('sgpwks_integration__wks_sg_listprice') }}
),

--logical CTEs
ispm as 
(
    select distinct * from itg_sg_zuellig_product_mapping
),
iscm as 
(
    select distinct * from itg_sg_zuellig_customer_mapping    
),
a as 
(
    select cast(to_char(to_date(upper(wszs.month_no), 'MON-YY'), 'YYYYMM') as int) as period,
        to_date(to_char(to_date(wszs.sales_date, 'MM/DD/YYYY'),'YYYY-MM-DD'),'YYYY-MM-DD') as sales_date,
        wszs.warehouse_code,
        wszs.customer_code,
        wszs.customer_name,
        iscm.regional_banner as sg_banner,
        iscm.merchandizing,
        wszs.invoice,
        ltrim(wszs.item_code, '0') as item_code,
        wszs.item_name,
        ispm.jj_code as sap_item_code,
        ispm.brand as sg_brand,
        wszs.type,
        --nvl(evolp.rate,evolp2.min_period_rate) as matl_lp,
        wszs.sales_value,
        wszs.sales_units,
        wszs.bonus_units,
        wszs.returns_units,
        wszs.returns_value,
        wszs.returns_bonus_units,
        wszs.cdl_dttm,
        wszs.curr_dt,
        current_timestamp()::timestamp_ntz as updt_dttm,
        wszs.file_name,
        wszs.run_id
    from source wszs,ispm,iscm
    where ltrim(wszs.item_code, '0') = ispm.zp_item_code(+)
    and upper(iscm.zuellig_customer(+)) = upper(wszs.customer_name)
),
t2 as
(
            select 
                distinct rate,
                replace(
                    substring(to_char(snapshot_dt, 'YYYY-MM-DD'), 0, 8),
                    '-',
                    ''
                ) as yearmo,
                to_date(valid_from, 'YYYY-MM-DD') as valid_from,
                to_date(valid_to, 'YYYY-MM-DD') AS valid_to,
                item_cd
            from wks_sg_listprice
),
combined_joins as
(
    SELECT 
        a.period,
        a.sales_date,
        a.warehouse_code,
        a.customer_code,
        a.customer_name,
        a.sg_banner,
        a.merchandizing,
        a.invoice,
        a.item_code,
        a.item_name,
        a.sap_item_code,
        a.sg_brand,
        a.type,
        --nvl(b.rate,c.min_period_rate) as matl_lp,
        t2.rate as matl_lp,
        a.sales_value,
        a.sales_units,
        a.bonus_units,
        a.returns_units,
        a.returns_value,
        a.returns_bonus_units,
        (t2.rate *(a.sales_units + a.bonus_units)) as gts,
        a.cdl_dttm,
        a.curr_dt,
        a.updt_dttm,
        a.file_name,
        a.run_id
    from a
    left join t2
    on a.period = t2.yearmo
    and a.sap_item_code = t2.item_cd
    and a.sales_date >= t2.valid_from
    and a.sales_date < t2.valid_to

),

--final CTE
final as
(
    select 
    	period::varchar(255) as period,
        sales_date::varchar(255) as sales_date,
        warehouse_code::varchar(255) as warehouse_code,
        customer_code::varchar(255) as customer_code,
        customer_name::varchar(255) as customer_name,
        sg_banner::varchar(255) as sg_banner,
        merchandizing::varchar(1) as merchandizing_flg,
        invoice::varchar(255) as invoice,
        item_code::varchar(255) as item_code,
        item_name::varchar(255) as item_name,
        sap_item_code::varchar(20) as sap_item_code,
        sg_brand::varchar(20) as sg_brand,
        type::varchar(255) as type,
        matl_lp::number(17,3) as matl_lp,
        sales_value::number(17,3) as sales_value,
        sales_units::number(17,3) as sales_units,
        bonus_units::number(17,3) as bonus_units,
        returns_units::number(17,3) as returns_units,
        returns_value::number(17,3) as returns_value,
        returns_bonus_units::number(17,3) as returns_bonus_units,
        gts::number(17,3) as gts,
        cdl_dttm::varchar(255) as cdl_dttm,
        curr_dt::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(255) as file_name,
        run_id::number(14,0) as run_id
        from combined_joins
)
select * from final