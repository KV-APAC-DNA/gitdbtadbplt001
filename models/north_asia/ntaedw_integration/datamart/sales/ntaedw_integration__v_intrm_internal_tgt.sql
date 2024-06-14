with edw_internal_target_fact as (
    select * from snapntaedw_integration.edw_internal_target_fact
),
edw_customer_attr_flat_dim as (
    select * from snapaspedw_integration.edw_customer_attr_flat_dim
),
edw_customer_base_dim as (
    select * from snapaspedw_integration.edw_customer_base_dim
),
final as
(    
    SELECT 
        edw_internal_target_fact.fisc_yr_per,
        edw_internal_target_fact.co_cd,
        d.ctry_nm,
        d.ctry_key,
        edw_internal_target_fact.crncy,
        edw_internal_target_fact.cust_num,
        COALESCE(h.sls_grp, 'Not Available'::character varying) AS sls_grp_desc,
        COALESCE(h.channel, 'Others'::character varying) AS channel,
        (
            CASE
                WHEN ((f.rgn)::text = ('TPE'::character varying)::text) THEN 'TPE'::character varying
                WHEN ((f.rgn)::text = ('CNT'::character varying)::text) THEN 'Central'::character varying
                WHEN ((f.rgn)::text = ('STH'::character varying)::text) THEN 'South'::character varying
                WHEN ((f.rgn)::text = ('NTH'::character varying)::text) THEN 'North'::character varying
                ELSE 'Others'::character varying
            END
        )::character varying(150) AS rgn,
        f.cust_nm AS edw_cust_nm,
        sum(edw_internal_target_fact.sls_trgt) AS sls_trgt
    FROM edw_internal_target_fact
        LEFT JOIN 
        (
            SELECT DISTINCT edw_customer_attr_flat_dim.sold_to_party AS sold_to_prty,
                edw_customer_attr_flat_dim.channel,
                edw_customer_attr_flat_dim.sls_grp
            FROM edw_customer_attr_flat_dim
            WHERE (
                    (edw_customer_attr_flat_dim.trgt_type)::text = ('flat'::character varying)::text
                )
        ) h ON 
        (
            (
                ltrim(
                    (
                        (edw_internal_target_fact.cust_num)::character varying
                    )::text,
                    ((0)::character varying)::text
                ) = ltrim(
                    (h.sold_to_prty)::text,
                    ((0)::character varying)::text
                )
            )
        )
        LEFT JOIN edw_customer_base_dim f ON 
        (
            (
                ltrim(
                    (
                        (edw_internal_target_fact.cust_num)::character varying
                    )::text,
                    ((0)::character varying)::text
                ) = ltrim(
                    (f.cust_num)::text,
                    ((0)::character varying)::text
                )
            )
        )
        JOIN (
            SELECT edw_company_dim.co_cd,
                edw_company_dim.ctry_nm,
                edw_company_dim.ctry_key,
                edw_company_dim.company_nm
            FROM edw_company_dim
            WHERE (
                    (edw_company_dim.ctry_key)::text = ('TW'::character varying)::text
                )
        ) d ON (
            (
                (edw_internal_target_fact.co_cd)::text = (d.co_cd)::text
            )
        )
    GROUP BY 
        edw_internal_target_fact.fisc_yr_per,
        edw_internal_target_fact.co_cd,
        edw_internal_target_fact.crncy,
        edw_internal_target_fact.cust_num,
        h.sls_grp,
        h.channel,
        f.rgn,
        f.cust_nm,
        d.ctry_nm,
        d.ctry_key
)
select * from final