with v_cust_customer as 
(
     select * from {{ ref('aspitg_integration__v_cust_customer') }}
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
v_cust as 
(
    Select 
        remotekey as aw_remote_key,
        customername as cust_nm,
        streetnumber as street_num,
        streetname as street_nm,
        city,
        postcode as post_cd,
        district as dist,
        county as county,
        (
            CASE
                WHEN upper(country) = 'KR' THEN 'Korea'
                WHEN upper(country) = 'TW' THEN 'Taiwan'
                WHEN upper(country) = 'HK' THEN 'HongKong'
                else country
            end
        ) as cntry,
        phonenumber as ph_num,
        email as email_id,
        website,
        storetype as store_typ,
        storereference as cust_store_ref,
        channel,
        salesgroup as sls_grp,
        secondarytradecode as secondary_trade_cd,
        secondarytradename as secondary_trade_nm,
        soldtoparty as sold_to_party,
        soldtoparty || storereference as sfa_cust_code,
        'flat' AS trgt_type,
        convert_timezone('UTC', current_timestamp()) as UPDT_DTTM
    from 
        (
            select * from v_cust_customer
            where rank = 1
                AND REMOTEKEY || UPPER
                (
                    DECODE(
                        country,
                        'Korea',
                        'KR',
                        'Taiwan',
                        'TW',
                        'Hong Kong',
                        'HK',
                        'HongKong',
                        'HK',
                        country
                    )
                ) not in 
                (
                    select distinct remotekey || cntry_cd from edw_vw_store_master_rex_pop6
                    where external_pop_code is not null
                )
        )
    where remotekey <> soldtoparty
    and customername <> ''
    and (remotekey, upper(country)) not in 
    (
        Select distinct replace(ltrim(replace(cust_num, '0', ' ')), ' ', '0') as aw_remote_key,
            ctry_key
        from edw_customer_base_dim
        where ctry_key in ('KR', 'TW', 'HK')
    )
),
final as 
(
    Select 
        distinct src.aw_remote_key,
        src.cust_nm,
        src.street_num,
        src.street_nm,
        src.city,
        src.post_cd,
        src.dist,
        src.county,
        src.cntry,
        src.ph_num,
        src.email_id,
        src.website,
        src.store_typ,
        src.cust_store_ref,
        src.channel,
        src.sls_grp,
        src.secondary_trade_cd,
        src.secondary_trade_nm,
        src.sold_to_party,
        src.trgt_type,
        src.sfa_cust_code,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        src.UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'JSON'
            ELSE TGT.CRT_DS
        END AS CRT_DS,
        'JSON' as UPD_DS,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM 
        v_cust src
        LEFT OUTER JOIN 
        (
            Select distinct * from edw_customer_attr_flat_dim
        ) TGT on src.cntry = TGT.cntry
        and src.aw_remote_key = TGT.aw_remote_key
        and src.sold_to_party <> TGT.aw_remote_key
)
select * from final