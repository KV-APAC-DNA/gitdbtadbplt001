with edw_sales_rep_route_plan as (
    select * from ntaedw_integration.edw_sales_rep_route_plan
),
edw_ims_fact as (
    select * from ntaedw_integration.edw_ims_fact
),
c as (
    SELECT rtrim(edw_ims_fact.sls_rep_cd) as sls_rep_cd,
        max(edw_ims_fact.ims_txn_dt) AS ims_txn_dt
    FROM edw_ims_fact
    WHERE (
            (
                NOT (
                    rtrim(edw_ims_fact.sls_rep_cd) IN (
                        SELECT DISTINCT rtrim(edw_sales_rep_route_plan.sls_rep_cd)
                        FROM edw_sales_rep_route_plan
                    )
                )
            )
            AND (
                (edw_ims_fact.sls_rep_nm)::text not like ('%?%'::character varying)::text
            )
        )
    GROUP BY rtrim(edw_ims_fact.sls_rep_cd)
),
derived_table1 as (
    SELECT DISTINCT a.ctry_cd,
        a.dstr_cd,
        b.dstr_nm,
        a.sls_rep_cd,
        a.sls_rep_nm,
        a.sls_rep_typ
    FROM edw_sales_rep_route_plan a,
        edw_ims_fact b
    WHERE (
            ((a.ctry_cd)::text = (b.ctry_cd)::text)
            AND (rtrim(a.dstr_cd)::text = rtrim(b.dstr_cd)::text)
        )
    UNION
    SELECT DISTINCT a.ctry_cd,
        a.dstr_cd,
        a.dstr_nm,
        a.sls_rep_cd,
        a.sls_rep_nm,
        a.acc_type AS sls_rep_typ
    FROM edw_ims_fact a
            JOIN c ON (
                (
                    (rtrim(a.sls_rep_cd)::text = rtrim(c.sls_rep_cd)::text)
                    AND (a.ims_txn_dt = c.ims_txn_dt)
                )
            )
),
final as (
    SELECT derived_table1.ctry_cd,
        derived_table1.dstr_cd,
        derived_table1.dstr_nm,
        derived_table1.sls_rep_cd,
        derived_table1.sls_rep_nm,
        CASE
            WHEN (
                (derived_table1.sls_rep_typ)::text <> (''::character varying)::text
            ) THEN derived_table1.sls_rep_typ
            WHEN (
                (
                    (derived_table1.sls_rep_typ)::text = (''::character varying)::text
                )
                AND (
                    (derived_table1.dstr_cd)::text = ('110256'::character varying)::text
                )
            ) THEN 'Corporate'::character varying
            ELSE 'Not Assigned'::character varying
        END AS sls_rep_typ
    from derived_table1
    WHERE (
        (derived_table1.sls_rep_cd)::text <> (''::character varying)::text
    )
)
select * from final