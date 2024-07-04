{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["year"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where cast(year as integer) in (
        select cast(year as integer) from {{ source('idnsdl_raw', 'sdl_mds_id_lav_sales_target') }});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_lav_sales_target') }}
),
edw_time_dim as 
(
    select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),
trans as 
(
    SELECT  
        TRIM(t1.year) AS YEAR,
        TRIM(t1.sales_type) AS sales_type,
        TRIM(t1.customer_code) AS customer_code,
        TRIM(t1.customer) AS customer,
        TRIM(t2.jj_mnth_long) AS jj_mnth_long,
        TRIM(t1.franchise) AS franchise,
        TRIM(t1.brand) AS brand,
        TRIM(t1.channel) AS channel,
        DECODE(
            TRIM(jj_mnth_long),
            'January',
            january,
            'February',
            february,
            'March',
            march,
            'April',
            april,
            'May',
            may,
            'June',
            june,
            'July',
            july,
            'August',
            august,
            'September',
            september,
            'October',
            october,
            'November',
            november,
            'December',
            december
        ) AS val
    FROM source AS t1,
        (
            SELECT DISTINCT jj_mnth_long
            FROM edw_time_dim
        ) AS t2
),
transformed as 
(
    SELECT CAST(YEAR AS INTEGER) AS YEAR,
            customer_code,
            customer,
            jj_mnth_long,
            brand,
            franchise,
            channel,
            (
                CASE
                    WHEN UPPER(sales_type) = 'HNA' THEN CAST(REPLACE(val, ',', '.') AS NUMERIC(38, 4))
                    ELSE 0
                END
            ) AS trgt_hna,
            (
                CASE
                    WHEN UPPER(sales_type) = 'NIV' THEN CAST(REPLACE(val, ',', '.') AS NUMERIC(38, 4))
                    ELSE 0
                END
            ) AS trgt_niv
        FROM trans
),
final as 
(
    select
	year::varchar(4) as year,
	REPLACE(customer_code, '.0', '')::varchar(75) as jj_sap_dstrbtr_id,
	customer::varchar(75) as jj_sap_dstrbtr_nm,
	jj_mnth_long::varchar(10) as jj_mnth_long,
	franchise::varchar(75) as franchise,
	brand::varchar(75) as brand,
	channel::varchar(75) as channel,
	SUM(trgt_hna)::number(38,4) as trgt_hna,
	SUM(trgt_niv)::number(38,4) as trgt_niv
    from transformed
    group by 
    year,
    customer_code,
    customer,
    jj_mnth_long,
    franchise,
    brand,
    channel
)
select * from final
