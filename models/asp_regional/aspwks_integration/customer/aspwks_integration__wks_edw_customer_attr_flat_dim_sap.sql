
with edw_customer_base_dim as 
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_customer_attr_flat_dim as 
(
    select * from {{ source('aspedw_integration', 'edw_customer_attr_flat_dim_temp') }}
),
v_edw_customer_sales_dim as 
(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
sap as 
(
    Select 
        distinct replace(
            ltrim(replace(custbase.cust_num, '0', ' ')),
            ' ',
            '0'
        ) as aw_remote_key,
        custbase.cust_nm as cust_nm,
        'NA' as street_num,
        'NA' as street_nm,
        custbase.city,
        custbase.pstl_cd as post_cd,
        'NA' as dist,
        custbase.ctry_key as county,
        (
            CASE
                WHEN upper(custbase.ctry_key) = 'KR' THEN 'Korea'
                WHEN upper(custbase.ctry_key) = 'TW' THEN 'Taiwan'
                WHEN upper(custbase.ctry_key) = 'HK' THEN 'HongKong'
                else custbase.ctry_key
            end
        ) as cntry,
        custbase.fst_phn_num as ph_num,
        'NA' as email_id,
        'NA' as website,
        '' as store_typ,
        'NA' as cust_store_ref,
        case
            when custbase.ctry_key = 'TW' then custsales.sls_ofc_desc
            else ''
        end as channel,
        custsales.sls_grp_desc as sls_grp,
        'NA' as secondary_trade_cd,
        'NA' as secondary_trade_nm,
        replace(
            ltrim(replace(custbase.cust_num, '0', ' ')),
            ' ',
            '0'
        ) as sold_to_party,
        'flat' AS trgt_type,
        custsales.sls_ofc,
        custsales.sls_ofc_desc,
        custsales.sls_grp as sls_grp_cd,
        --custbase.cust_nm as cust_name,-----need to remove
        'NA' as sfa_cust_code,
        convert_timezone('UTC', current_timestamp()) UPDT_DTTM
    from edw_customer_base_dim custbase
    inner join v_edw_customer_sales_dim custsales 
    ON custbase.cust_num = custsales.cust_num
    where ctry_key in ('KR', 'TW') and custsales.sls_org in ('320S', '1200')
    UNION ALL
    Select 
        distinct replace(
            ltrim(replace(custbase.cust_num, '0', ' ')),
            ' ',
            '0'
        ) as aw_remote_key,
        custbase.cust_nm as cust_nm,
        'NA' as street_num,
        'NA' as street_nm,
        custbase.city,
        custbase.pstl_cd as post_cd,
        'NA' as dist,
        custbase.ctry_key as county,
        (
            CASE
                WHEN upper(custbase.ctry_key) = 'KR' THEN 'Korea'
                WHEN upper(custbase.ctry_key) = 'TW' THEN 'Taiwan'
                WHEN upper(custbase.ctry_key) = 'HK' THEN 'HongKong'
                else custbase.ctry_key
            end
        ) as cntry,
        custbase.fst_phn_num as ph_num,
        'NA' as email_id,
        'NA' as website,
        '' as store_typ,
        'NA' as cust_store_ref,
        custsales.code_desc as channel,
        custsales.sls_grp_desc as sls_grp,
        'NA' as secondary_trade_cd,
        'NA' as secondary_trade_nm,
        replace(
            ltrim(replace(custbase.cust_num, '0', ' ')),
            ' ',
            '0'
        ) as sold_to_party,
        'flat' AS trgt_type,
        custsales.sls_ofc,
        custsales.sls_ofc_desc,
        custsales.sls_grp as sls_grp_cd,
        --custbase.cust_nm as cust_name,-----need to remove
        'NA' as sfa_cust_code,
        convert_timezone('UTC', current_timestamp()) UPDT_DTTM
    from edw_customer_base_dim custbase
    inner join v_edw_customer_sales_dim custsales 
    ON custbase.cust_num = custsales.cust_num
    where ctry_key in ('HK')
    and custsales.sls_org in ('1110')
    and custsales.cust_del_flag <> 'X'
),
final as 
(
    Select 
        distinct SRC.aw_remote_key,
        SRC.cust_nm,
        SRC.street_num,
        SRC.street_nm,
        SRC.city,
        SRC.post_cd,
        SRC.dist,
        SRC.county,
        SRC.cntry,
        SRC.ph_num,
        SRC.email_id,
        SRC.website,
        SRC.store_typ,
        SRC.cust_store_ref,
        SRC.channel,
        SRC.sls_grp,
        SRC.secondary_trade_cd,
        SRC.secondary_trade_nm,
        SRC.sold_to_party,
        SRC.trgt_type,
        SRC.sls_ofc,
        SRC.sls_ofc_desc,
        SRC.sls_grp_cd,
        --SRC.cust_name,
        src.sfa_cust_code,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        SRC.UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'SAP'
            ELSE TGT.CRT_DS
        END AS CRT_DS,
        'SAP' as UPD_DS,
        TGT.sls_grp as target_sls_grp,
        TGT.Priority_nm
    FROM sap SRC
    LEFT OUTER JOIN 
    (
        Select 
            distinct c.aw_remote_key,
            c.sold_to_party,
            c.CRT_DS,
            c.CRT_DTTM,
            c.cntry,
            c.sls_grp,
            'SAP' as Priority_nm
        from edw_customer_attr_flat_dim c -- inner join rg_itg.ITG_CUSTOMER_LOAD_PRIORITY p on c.cntry = p.country
    ) TGT on SRC.cntry = TGT.cntry
    and SRC.aw_remote_key = TGT.aw_remote_key
)
select * from final