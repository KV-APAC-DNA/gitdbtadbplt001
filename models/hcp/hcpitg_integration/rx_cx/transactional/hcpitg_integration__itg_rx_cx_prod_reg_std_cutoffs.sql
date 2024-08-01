{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}}
        WHERE 'Y' = (SELECT parameter_value
             FROM {{ source('inditg_integration', 'itg_query_parameters') }}
             WHERE  UPPER(country_code) = 'IN'
             AND    UPPER(parameter_type) = 'ACTIVE_FLAG'
             AND    UPPER(parameter_name) = 'IN_RX_CX_CUTOFFS_LOAD_FLAG');
        {% endif %}"
    )
}}
with 
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
rx_cx_prod_reg_cutoffs as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_prod_reg_cutoffs') }}
),
final as 
(
    SELECT  
    rx_product::varchar(200) as rx_product,
	year::number(18,0) as year,
	quarter::number(18,0) as quarter,
	region_cohort::varchar(20) as region_cohort,
	lower_cut::number(38,2) as lower_cut,
	upper_cut::number(38,2) as upper_cut,
	sales_percentile_25::number(38,2) as sales_percentile_25,
	outlet_count::number(38,0) as outlet_count,
	to_date(current_timestamp()) as created_date
FROM  rx_cx_prod_reg_cutoffs
WHERE 'Y' = (SELECT parameter_value
             FROM   itg_query_parameters
             WHERE  UPPER(country_code) = 'IN'
             AND    UPPER(parameter_type) = 'ACTIVE_FLAG'
             AND    UPPER(parameter_name) = 'IN_RX_CX_CUTOFFS_LOAD_FLAG')
)
select * from final