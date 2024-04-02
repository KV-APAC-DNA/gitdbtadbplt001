with source as(
    select * from {{ ref('vnmitg_integration__itg_vn_dksh_daily_sales') }} 
),
itg_query_parameters as(
    select * from {{ source('aspitg_integration', 'itg_query_parameters') }}
),
final as(
    select
        'VN'::varchar(255) AS cntry_cd,
        'Vietnam'::varchar(255) AS cntry_nm,
        (
            SELECT
            parameter_value::varchar(30) as parameter_value
            FROM itg_query_parameters
            WHERE
            country_code = 'VN' AND parameter_name = 'vn_dksh_soldto_code'
        ) AS sold_to_code, 
        group_ds::varchar(255) as group_ds,
        category::varchar(255) as category,
        material::varchar(255) as material,
        materialdescription::varchar(255) as materialdescription,
        syslot::varchar(255) as syslot,
        batchno::varchar(255) as batchno,
        exp_date::varchar(255) as exp_date,
        total::number(18,0) as total,
        hcm::number(18,0) as hcm,
        vsip::number(18,0) as vsip,
        langha::number(18,0) as langha,
        thanhtri::number(18,0) as thanhtri,
        danang::number(18,0) as danang,
        values_lc::number(24,10) as values_lc,
        reason::varchar(255) as reason,
        TO_DATE(file_date, 'YYYY-MM-DD') AS transaction_date,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final