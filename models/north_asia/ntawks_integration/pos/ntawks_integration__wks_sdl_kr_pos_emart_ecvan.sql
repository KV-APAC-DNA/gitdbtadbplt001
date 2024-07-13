with source as (
    select * from {{ ref('ntawks_integration__wks_kr_pos_emart_evydy') }}
),
final as (
    SELECT CAST(SALE_DATE AS DATE) as pos_dt,
        NULL::varchar(30) as business_cd,
        NULL::varchar(255) as comp_nm,
        'ECVAN'::varchar(30) as vend_nm,
        (sale_id || '_online')::varchar(30) AS str_cd,
        (sale_name || '_online')::varchar(255) AS str_nm,
        prod_code::varchar(50) as ean,
        (prod_name1 || '' || prod_name2)::varchar(255) as prod_nm,
        cast (sale_qty as integer) AS sellout_qty,
        cast (sale_amnt as numeric(16, 5)) AS sellout_amt,
        NULL::varchar(255) as mnth_tot_qty,
        NULL::varchar(255) as mnth_tot_amt,
        current_timestamp() as crtd_dttm,
        null::varchar(255) as filename
    FROM source
    where mesg_from = 'RSHIN19'
)
select * from final