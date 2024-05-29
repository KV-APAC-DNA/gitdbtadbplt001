with source as 
(
    select * from {{ source('bwa_access', 'bwa_kr_list_price') }}
),
final as 
(
    select 
        salesorg as sls_org,
        sales_grp as sales_grp_cd,
        knart as cnd_type,
        ltrim(material,0) as matl_num,
        customer as sold_to_cust_cd,
        replace(knval,',','') as price,
        to_date(datefrom,'yyyymmdd') as vld_frm,
        to_date(dateto,'yyyymmdd') as vld_to,
        cond_curr as cond_curr,
        doc_currcy as doc_cur,
        recordmode as recordmode,
        'KR' as ctry_cd,
        current_timestamp::timestamp_ntz(9) as crt_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final
