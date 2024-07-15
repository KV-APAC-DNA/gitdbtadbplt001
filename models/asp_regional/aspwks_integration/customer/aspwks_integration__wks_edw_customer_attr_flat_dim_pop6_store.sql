with edw_vw_pop6_store as 
(
     select * from {{ ref('aspedw_integration__edw_vw_pop6_store') }}
),
edw_vw_store_master_rex_pop6 as 
(
    select * from {{ ref('aspedw_integration__edw_vw_store_master_rex_pop6') }}
),
edw_customer_base_dim as 
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }} 
),
edw_customer_attr_flat_dim as 
(
    select * from {{ source('aspedw_integration', 'edw_customer_attr_flat_dim_temp') }}
),
pop6_store as 
(
    SELECT 
        aw_remote_key,
        external_pop_code,
        cust_nm,
        street_num,
        street_nm,
        city,
        post_cd,
        dist,
        county,
        cntry,
        ph_num,
        email_id,
        website,
        store_typ,
        cust_store_ref,
        channel,
        sls_grp,
        secondary_trade_cd,
        secondary_trade_nm,
        sold_to_party,
        sfa_cust_code,
        trgt_type,
        updt_dttm
    FROM 
        (
            SELECT 
                pop_code AS aw_remote_key,
                external_pop_code,
                pop_name AS cust_nm,
                address AS street_num,
                NULL AS street_nm,
                NULL AS CITY,
                NULL AS post_cd,
                NULL AS dist,
                DECODE(
                    COUNTRY,
                    'Korea',
                    'KR',
                    'Taiwan',
                    'TW',
                    'Hong Kong',
                    'HK',
                    COUNTRY
                ) AS county,
                country AS cntry,
                NULL AS ph_num,
                NULL AS email_id,
                NULL AS website,
                retail_environment_ps AS store_typ,
                NULL AS cust_store_ref,
                channel,
                sales_group_name AS sls_grp,
                NULL AS secondary_trade_cd,
                NULL AS secondary_trade_nm,
                NULL AS sold_to_party,
                NULL AS sfa_cust_code,
                'flat' AS trgt_type,
                convert_timezone('UTC', current_timestamp()) as UPDT_DTTM
            FROM edw_vw_pop6_store
            WHERE POP_CODE || DECODE 
                (
                    COUNTRY,
                    'Korea',
                    'KR',
                    'Taiwan',
                    'TW',
                    'Hong Kong',
                    'HK',
                    COUNTRY
                ) NOT IN 
                (
                    SELECT DISTINCT POP_CODE || cntry_cd FROM EDW_VW_STORE_MASTER_REX_POP6
                )
                and nvl(cntry_cd, '#') not in ('JP', 'SG')
        )
    WHERE CUST_NM <> ' '
        AND CUST_NM IS NOT NULL
        and nvl(cntry, '#') not in ('JP', 'SG')
        AND (TRIM(aw_remote_key), UPPER(county)) NOT IN (
            SELECT DISTINCT REPLACE(LTRIM(REPLACE(cust_num, '0', ' ')), ' ', '0') AS aw_remote_key,
                ctry_key
            FROM edw_customer_base_dim
            WHERE ctry_key IN ('KR', 'TW', 'HK')
        )
),
final as 
(
    SELECT 
        SRC.aw_remote_key,
        SRC.external_pop_code,
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
        SRC.sfa_cust_code,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        SRC.UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'JSON'
            ELSE TGT.CRT_DS
        END AS CRT_DS,
        'JSON' AS UPD_DS,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM 
        pop6_store SRC
        LEFT OUTER JOIN (
            SELECT DISTINCT * FROM edw_customer_attr_flat_dim
        ) TGT ON SRC.county = DECODE(
            TGT.cntry,
            'Korea',
            'KR',
            'Taiwan',
            'TW',
            'Hong Kong',
            'HK',
            'HongKong',
            'HK',
            TGT.cntry
        )
        AND SRC.aw_remote_key = TGT.aw_remote_key
)
select * from final