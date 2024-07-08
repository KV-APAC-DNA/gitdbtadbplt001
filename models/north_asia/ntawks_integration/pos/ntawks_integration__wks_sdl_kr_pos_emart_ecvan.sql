with source as (
    select * from {{ ref('ntawks_integration__wks_kr_pos_emart_evydy') }}
),
final as (
    SELECT to_date(SALE_DATE) as pos_dt,
        NULL as business_cd,
        NULL as comp_nm,
        'ECVAN' as vend_nm,
        sale_id + '_online' AS str_cd,
        --new
        sale_name + '_online' AS str_nm,
        --new
        prod_code as ean,
        (prod_name1 || '' || prod_name2) as prod_nm,
        cast (sale_qty as integer) AS sellout_qty,
        cast (sale_amnt as numeric(16, 5)) AS sellout_amt,
        NULL as mnth_tot_qty,
        NULL as mnth_tot_amt,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm
    FROM source
    where mesg_from = 'RSHIN19'
)
select * from final