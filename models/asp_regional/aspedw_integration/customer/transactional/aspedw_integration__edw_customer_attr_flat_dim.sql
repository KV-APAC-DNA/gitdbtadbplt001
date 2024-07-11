{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} edw using {{ ref('aspwks_integration__wks_edw_customer_attr_flat_dim_rex_pop6') }} wks where wks.county = decode(edw.cntry,'Korea','KR','Taiwan','TW','Hong Kong','HK','HongKong','HK',edw.cntry ) AND wks.aw_remote_key = edw.aw_remote_key and wks.chng_flg = 'U';
        delete from {{this}} edw using {{ ref('aspwks_integration__wks_edw_customer_attr_flat_dim_pop6_store') }} wks where wks.county = decode(edw.cntry,'Korea','KR','Taiwan','TW','Hong Kong','HK','HongKong','HK',edw.cntry) and wks.aw_remote_key = edw.aw_remote_key;
        delete from {{this}} edw using {{ ref('aspwks_integration__wks_edw_customer_attr_flat_dim_v_cust') }} wks where wks.cntry = edw.cntry and wks.aw_remote_key = edw.aw_remote_key and wks.chng_flg = 'U';
        delete from {{this}} edw using {{ ref('aspwks_integration__wks_edw_customer_attr_flat_dim_sap') }} wks where wks.cntry = edw.cntry AND wks.aw_remote_key = edw.aw_remote_key and wks.Priority_nm = 'SAP' and wks.chng_flg = 'U';
        {% endif %}"
    )
}}
with wks_edw_customer_attr_flat_dim_rex_pop6 as 
(
    select * from {{ ref('aspwks_integration__wks_edw_customer_attr_flat_dim_rex_pop6') }}
                            
),
wks_edw_customer_attr_flat_dim_pop6_store as 
(
    select * from {{ ref('aspwks_integration__wks_edw_customer_attr_flat_dim_pop6_store') }}
),
wks_edw_customer_attr_flat_dim_v_cust as 
(
    select * from {{ ref('aspwks_integration__wks_edw_customer_attr_flat_dim_v_cust') }}
),
wks_edw_customer_attr_flat_dim_sap as 
(
    select * from {{ ref('aspwks_integration__wks_edw_customer_attr_flat_dim_sap') }}
),
itg_kr_sales_store_map as 
(
    select * from {{ ref('ntaitg_integration__itg_kr_sales_store_map') }}
    where sales_group_code not in ('K99') or (sales_group_code = 'K99' and sls_ofc_desc is not null)
),
rex_pop6 as
(
    select 
        src.pop_code as aw_remote_key,
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
        CASE
            WHEN chng_flg = 'I' THEN convert_timezone('UTC', current_timestamp())
            ELSE tgt_crt_dttm
        END AS crt_dttm,
        convert_timezone('UTC', current_timestamp()) AS updt_dttm,
        src.crt_ds,
        CASE
            WHEN src.chng_flg = 'U' THEN src.upd_ds
            ELSE null
        END AS upd_ds,
        '#' AS sls_ofc,
        '#' AS sls_ofc_desc,
        '#' AS sls_grp_cd,
        src.sfa_cust_code
    FROM 
        (
            select a.*,
                dense_rank() over (
                    PARTITION BY cntry,
                    aw_remote_key
                    order by sls_grp,
                        sold_to_party asc
                ) as d_rnk
            from wks_edw_customer_attr_flat_dim_rex_pop6 a
            where chng_flg = 'U'
        ) SRC
    where src.d_rnk = 1
),
pop6_store as
(
    Select src.aw_remote_key,
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
        CASE
            WHEN chng_flg = 'I' THEN convert_timezone('UTC', current_timestamp())
            ELSE tgt_crt_dttm
        END AS crt_dttm,
        convert_timezone('UTC', current_timestamp()) AS updt_dttm,
        src.crt_ds,
        CASE
            WHEN src.chng_flg = 'U' THEN src.upd_ds
            ELSE null
        END AS upd_ds,
        '#' AS sls_ofc,
        '#' AS sls_ofc_desc,
        '#' AS sls_grp_cd,
        src.sfa_cust_code
    FROM (
            select a.*,
                dense_rank() over (
                    PARTITION BY cntry,
                    aw_remote_key
                    order by sls_grp,
                        sold_to_party asc
                ) as d_rnk
            from wks_edw_customer_attr_flat_dim_pop6_store a
        ) SRC
    where src.d_rnk = 1
    and (src.aw_remote_key,decode(src.county,'Korea','KR','Taiwan','TW','Hong Kong','HK','HongKong','HK',src.cntry )) not in 
    (
        select distinct aw_remote_key,cntry from rex_pop6
    )
),
v_cust as
(
    Select 
        src.aw_remote_key,
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
        CASE
            WHEN chng_flg = 'I' THEN convert_timezone('UTC', current_timestamp())
            ELSE tgt_crt_dttm
        END AS crt_dttm,
        convert_timezone('UTC', current_timestamp()) AS updt_dttm,
        src.crt_ds,
        CASE
            WHEN src.chng_flg = 'U' THEN src.upd_ds
            ELSE null
        END AS upd_ds,
        '#' AS sls_ofc,
        '#' AS sls_ofc_desc,
        '#' AS sls_grp_cd,
        src.sfa_cust_code
    FROM 
        (
            select a.*,
                dense_rank() over (
                    PARTITION BY cntry,
                    aw_remote_key
                    order by sls_grp,
                        sold_to_party asc
                ) as d_rnk
            from wks_edw_customer_attr_flat_dim_v_cust a
        ) SRC
    where src.d_rnk = 1
),
sap_union1 as
(
    Select 
        src.aw_remote_key,
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
        CASE
            WHEN chng_flg = 'I' THEN convert_timezone('UTC', current_timestamp())
            ELSE tgt_crt_dttm
        END AS crt_dttm,
        convert_timezone('UTC', current_timestamp()) AS updt_dttm,
        src.crt_ds,
        CASE
            WHEN src.Priority_nm = 'SAP' THEN src.upd_ds
            ELSE null
        END AS upd_ds,
        src.sls_ofc,
        src.sls_ofc_desc,
        src.sls_grp_cd,
        src.sfa_cust_code
    FROM wks_edw_customer_attr_flat_dim_sap SRC
    where 
    src.chng_flg = 'U' and 
    src.Priority_nm = 'SAP'
),
sap_union2 as
(
    Select 
        src.aw_remote_key,
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
        CASE
            WHEN chng_flg = 'I' THEN convert_timezone('UTC', current_timestamp())
            ELSE src.tgt_crt_dttm
        END AS crt_dttm,
        convert_timezone('UTC', current_timestamp()) AS updt_dttm,
        CASE
            WHEN chng_flg = 'I' THEN src.crt_ds
            ELSE null
        END AS crt_ds,
        '' AS upd_ds,
        src.sls_ofc,
        src.sls_ofc_desc,
        src.sls_grp_cd,
        src.sfa_cust_code,
    FROM wks_edw_customer_attr_flat_dim_sap SRC
    where src.chng_flg = 'I'
),
sap as 
(
    select
        aw_remote_key,
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
        trgt_type,
        crt_dttm,
        updt_dttm,
        crt_ds,
        upd_ds,
        sls_ofc,
        sls_ofc_desc,
        sls_grp_cd,
        sfa_cust_code
    from 
        (
            select * from sap_union1
            union all
            select * from sap_union2
        )

),
all_combined as 
(
    select 
        a.aw_remote_key,
        a.cust_nm,
        a.street_num,
        a.street_nm,
        a.city,
        a.post_cd,
        a.dist,
        a.county,
        a.cntry,
        a.ph_num,
        a.email_id,
        a.website,
        case 
            when cntry in ('Korea','KR') and sales.sales_group_code is not null 
            then sales.store_type
            else a.store_typ
        end as store_typ,
        a.cust_store_ref,
        case 
            when cntry in ('Korea','KR') and sales.sales_group_code is not null 
            then sales.channel
            else a.channel 
        end as channel,
        a.sls_grp,
        a.secondary_trade_cd,
        a.secondary_trade_nm,
        a.sold_to_party,
        a.trgt_type,
        a.crt_dttm,
        case 
            when cntry in ('Korea','KR') and sales.sales_group_code is not null 
            then convert_timezone('UTC', current_timestamp())
            else a.updt_dttm
        end as updt_dttm,
        a.crt_ds,
        a.upd_ds,
        case 
            when cntry in ('Korea','KR') and sales.sales_group_code is not null 
            then sales.sls_ofc
            else a.sls_ofc 
        end as sls_ofc,
        case 
            when cntry in ('Korea','KR') and sales.sales_group_code is not null 
            then sales.sls_ofc_desc
            else a.sls_ofc_desc 
        end as sls_ofc_desc,
        a.sls_grp_cd,
        a.sfa_cust_code,
    from
    (
        select * from rex_pop6
        union all
        select * from pop6_store
        where (aw_remote_key,cntry) not in 
        (
            select distinct aw_remote_key,cntry from v_cust
        )
        union all
        select * from v_cust
        union all
        select * from sap
    )
    a left join itg_kr_sales_store_map sales
    on sls_grp_cd = sales.sales_group_code
),
final as 
(
    select 
        aw_remote_key::varchar(100) as aw_remote_key,
        cust_nm::varchar(100) as cust_nm,
        street_num::varchar(256) as street_num,
        street_nm::varchar(500) as street_nm,
        city::varchar(100) as city,
        post_cd::varchar(100) as post_cd,
        dist::varchar(100) as dist,
        county::varchar(100) as county,
        cntry::varchar(100) as cntry,
        ph_num::varchar(100) as ph_num,
        email_id::varchar(100) as email_id,
        website::varchar(100) as website,
        store_typ::varchar(100) as store_typ,
        cust_store_ref::varchar(100) as cust_store_ref,
        channel::varchar(100) as channel,
        sls_grp::varchar(100) as sls_grp,
        secondary_trade_cd::varchar(100) as secondary_trade_cd,
        secondary_trade_nm::varchar(100) as secondary_trade_nm,
        sold_to_party::varchar(100) as sold_to_party,
        trgt_type::varchar(50) as trgt_type,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        crt_ds::varchar(10) as crt_ds,
        upd_ds::varchar(10) as upd_ds,
        sls_ofc::varchar(4) as sls_ofc,
        sls_ofc_desc::varchar(40) as sls_ofc_desc,
        sls_grp_cd::varchar(3) as sls_grp_cd,
        sfa_cust_code::varchar(20) as sfa_cust_code
    from all_combined
)
select * from final