{{
    config
    (
        materialized = "incremental",
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}}
        WHERE year =  (SELECT CAST(parameter_value AS INTEGER) AS parameter_value
                 FROM   {{ source('inditg_integration', 'itg_query_parameters') }}
                 WHERE  UPPER(country_code) = 'IN'
                 AND    UPPER(parameter_type) = 'YEAR'
                 AND    UPPER(parameter_name) = 'IN_RX_CX_TARGET_GEN_YEAR')
  AND quarter = 'Q'||(SELECT CAST(parameter_value AS INTEGER) AS parameter_value
                 FROM   {{ source('inditg_integration', 'itg_query_parameters') }}
                 WHERE  UPPER(country_code) = 'IN'
                 AND    UPPER(parameter_type) = 'QUARTER'
                 AND    UPPER(parameter_name) = 'IN_RX_CX_TARGET_GEN_QTR');
        {% endif %}"
    )
}}
with 
rx_cx_cy_target as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_cy_target') }}
),
final as 
(
    SELECT 
	urc::varchar(50) as urc,
	rx_product::varchar(25) as product_vent,
	trim(year + 1)::number(18,0) as year,
	'Q'|| quarter::varchar(2) as quarter,
	lysq_ach_nr::number(10,2) as lysq_ach_nr,
	lysq_qty::number(18,0) as lysq_qty,
	lysq_presc::number(10,2) as lysq_presc,
	target_qty::number(18,0) as target_qty,
	target_presc::number(10,2) as target_presc,
	"case"::number(18,0) as case,
	prescription_action::varchar(100) as prescription_action,
	sales_action::varchar(100) as sales_action,
	NULL::varchar(2000) as hcp,
	target_presc::number(10,2) as prescriptions_needed,
    current_timestamp() as crt_dttm
FROM rx_cx_cy_target
)
select * from final