with itg_tw_strategic_cust_hier as(
    select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_TW_STRATEGIC_CUST_HIER
),
edw_customer_attr_flat_dim as(
    select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_CUSTOMER_ATTR_FLAT_DIM
),
transformed as(
    select 
        src.aw_remote_key::varchar(100) as aw_remote_key,
        src.cust_nm::varchar(100) as cust_nm,
        src.street_num::varchar(100) as street_num,
        src.street_nm::varchar(500) as street_nm,
        src.city::varchar(100) as city,
        src.post_cd::varchar(100) as post_cd,
        src.dist::varchar(100) as dist,
        src.county::varchar(100) as county,
        src.cntry::varchar(100) as cntry,
        src.ph_num::varchar(100) as ph_num,
        src.email_id::varchar(100) as email_id,
        src.website::varchar(100) as website,
        src.store_typ::varchar(100) as store_typ,
        src.cust_store_ref::varchar(100) as cust_store_ref,
        src.channel::varchar(100) as channel,
        src.sls_grp::varchar(100) as sls_grp,
        src.secondary_trade_cd::varchar(100) as secondary_trade_cd,
        src.secondary_trade_nm::varchar(100) as secondary_trade_nm,
        src.sold_to_party::varchar(100) as sold_to_party,
        src.sfa_cust_code::varchar(20) as sfa_cust_code,
        /*COALESCE(TGT.cust_hier_l1,'#') AS cust_hier_l1,
        COALESCE(TGT.cust_hier_l2,'#') AS cust_hier_l2,
        COALESCE(TGT.cust_hier_l3,'#') AS cust_hier_l3,
        COALESCE(TGT.cust_hier_l4,'#') AS cust_hier_l4,
        COALESCE(TGT.cust_hier_l5,'#') AS cust_hier_l5,
        */
        CASE 
            WHEN (SRC.sls_ofc || ' ' || SRC.sls_ofc_desc) IS NULL THEN '#'
            WHEN TRIM(SRC.sls_ofc || ' ' || SRC.sls_ofc_desc) = '' THEN '#'
            ELSE (SRC.sls_ofc || ' ' || SRC.sls_ofc_desc)
        END::varchar(500) AS cust_hier_l1,
        CASE 
            WHEN SRC.channel IS NULL THEN '#'
            WHEN TRIM(SRC.channel) = '' THEN '#'
            ELSE SRC.channel
        END::varchar(500) AS cust_hier_l2,
        CASE 
            WHEN SRC.store_typ IS NULL THEN '#'
            WHEN TRIM(SRC.store_typ) = '' THEN '#'
            ELSE SRC.store_typ
        END::varchar(500) AS cust_hier_l3,
        CASE 
            WHEN (SRC.sls_grp_cd || ' ' || SRC.sls_grp) IS NULL THEN '#'
            WHEN TRIM(SRC.sls_grp_cd || ' ' || SRC.sls_grp) = '' THEN '#'
            ELSE (SRC.sls_grp_cd || ' ' || SRC.sls_grp)
        END::varchar(500) AS cust_hier_l4,
        CASE 
            WHEN (SRC.aw_remote_key || ' ' || SRC.cust_nm) IS NULL THEN '#'
            WHEN TRIM(SRC.aw_remote_key || ' ' || SRC.cust_nm) = '' THEN '#'
            ELSE (SRC.aw_remote_key || ' ' || SRC.cust_nm)
        END::varchar(500) AS cust_hier_l5,
        'hierarchy'::varchar(50) AS trgt_type,
        src.crt_dttm::timestamp_ntz(9) as crt_dttm,
        src.updt_dttm::timestamp_ntz(9) as updt_dttm,
        src.crt_ds::varchar(10) as crt_ds,
        src.upd_ds::varchar(10) as upd_ds,
        sch.strategy_customer_hierachy_name::varchar(255) as strategy_customer_hierachy_name
    FROM edw_customer_attr_flat_dim src LEFT JOIN (
	SELECT cust_cd,
		max(strategy_customer_hierachy_name) AS strategy_customer_hierachy_name
	FROM itg_tw_strategic_cust_hier
	GROUP BY cust_cd
	) sch ON src.sold_to_party = sch.cust_cd
	
)
select * from transformed