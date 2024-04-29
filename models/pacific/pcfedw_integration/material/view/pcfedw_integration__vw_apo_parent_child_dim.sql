with edw_material_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
dly_sls_cust_attrb_lkp as
(
    select * from {{ ref('pcfedw_integration__dly_sls_cust_attrb_lkp') }}
),
final as
(
    SELECT 
        t1.sales_org,
        t1.cmp_id,
        t1.matl_id,
        t1.matl_desc,
        t1.master_code,
        t1.launch_date,
        t1.predessor_id,
        t2.matl_id AS parent_id,
        t2.matl_desc AS parent_matl_desc
    FROM 
        (
            SELECT DISTINCT a.sls_org AS sales_org,
                c.cmp_id,
                a.matl_num AS matl_id,
                a.mstr_cd AS master_code,
                CASE
                    WHEN (
                        (a.launch_dt = '1111-01-01'::date)
                        OR (
                            (a.launch_dt IS NULL)
                            AND ('1111-01-01' IS NULL)
                        )
                    ) THEN '2011-01-01'::date
                    WHEN (
                        (a.launch_dt = '6009-10-10'::date)
                        OR (
                            (a.launch_dt IS NULL)
                            AND ('6009-10-10' IS NULL)
                        )
                    ) THEN '2011-01-01'::date
                    WHEN (
                        (a.launch_dt = '9999-01-01'::date)
                        OR (
                            (a.launch_dt IS NULL)
                            AND ('9999-01-01' IS NULL)
                        )
                    ) THEN '2011-01-01'::date
                    WHEN (
                        (a.launch_dt = '9999-09-09'::date)
                        OR (
                            (a.launch_dt IS NULL)
                            AND ('9999-09-09' IS NULL)
                        )
                    ) THEN '2011-01-01'::date
                    WHEN (
                        (a.launch_dt = '9999-10-01'::date)
                        OR (
                            (a.launch_dt IS NULL)
                            AND ('9999-10-01' IS NULL)
                        )
                    ) THEN '2011-01-01'::date
                    WHEN (
                        (a.launch_dt = '2201-07-01'::date)
                        OR (
                            (a.launch_dt IS NULL)
                            AND ('2201-07-01' IS NULL)
                        )
                    ) THEN '2011-01-01'::date
                    WHEN (
                        (a.launch_dt = NULL::date)
                        OR (
                            (a.launch_dt IS NULL)
                            AND (NULL IS NULL)
                        )
                    ) THEN '2011-01-01'::date
                    ELSE a.launch_dt
                END AS launch_date,
                a.predecessor AS predessor_id,
                b.matl_desc
            FROM edw_material_sales_dim a,
                (
                    SELECT DISTINCT edw_material_dim.matl_num,
                        edw_material_dim.matl_desc
                    FROM edw_material_dim
                ) b,
                (
                    SELECT DISTINCT dly_sls_cust_attrb_lkp.sls_org,
                        dly_sls_cust_attrb_lkp.cmp_id
                    FROM dly_sls_cust_attrb_lkp
                ) c
            WHERE (
                    (
                        (
                            (
                                ((a.matl_num)::text = (b.matl_num)::text)
                                AND (
                                    (a.dstr_chnl)::text = ('19'::character varying)::text
                                )
                            )
                            AND (
                                (a.mstr_cd)::text <> (''::character varying)::text
                            )
                        )
                        AND (a.mstr_cd IS NOT NULL)
                    )
                    AND ((a.sls_org)::text = (c.sls_org)::text)
                )
            ORDER BY a.sls_org,
                c.cmp_id,
                a.matl_num
        ) t1,
        (
            SELECT derived_table2.sales_org,
                derived_table2.matl_id,
                derived_table2.master_code,
                derived_table2.launch_date,
                derived_table2.matl_desc
            FROM 
                (
                    SELECT derived_table1.sales_org,
                        derived_table1.matl_id,
                        derived_table1.master_code,
                        derived_table1.launch_date,
                        derived_table1.matl_desc,
                        row_number() OVER(
                            PARTITION BY derived_table1.sales_org,
                            derived_table1.master_code
                            ORDER BY derived_table1.launch_date DESC, derived_table1.matl_id DESC
                        ) AS rowno
                    FROM (
                            SELECT DISTINCT a.sls_org AS sales_org,
                                a.matl_num AS matl_id,
                                a.mstr_cd AS master_code,
                                CASE
                                    WHEN (
                                        (a.launch_dt = '1111-01-01'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('1111-01-01' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = '6009-10-10'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('6009-10-10' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = '9999-01-01'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('9999-01-01' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = '9999-09-09'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('9999-09-09' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = '9999-10-01'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('9999-10-01' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = '2201-07-01'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('2201-07-01' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = NULL::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND (NULL IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    ELSE a.launch_dt
                                END AS launch_date,
                                b.matl_desc
                            FROM edw_material_sales_dim a,
                                (
                                    SELECT DISTINCT edw_material_dim.matl_num,
                                        edw_material_dim.matl_desc
                                    FROM edw_material_dim
                                ) b,
                                (
                                    SELECT DISTINCT dly_sls_cust_attrb_lkp.sls_org,
                                        dly_sls_cust_attrb_lkp.cmp_id
                                    FROM dly_sls_cust_attrb_lkp
                                ) c
                            WHERE (
                                    (
                                        (
                                            ((a.matl_num)::text = (b.matl_num)::text)
                                            AND ((a.sls_org)::text = (c.sls_org)::text)
                                        )
                                        AND (
                                            (a.mstr_cd)::text <> (''::character varying)::text
                                        )
                                    )
                                    AND (a.mstr_cd IS NOT NULL)
                                )
                            ORDER BY a.mstr_cd,
                                CASE
                                    WHEN (
                                        (a.launch_dt = '1111-01-01'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('1111-01-01' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = '6009-10-10'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('6009-10-10' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = '9999-01-01'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('9999-01-01' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = '9999-09-09'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('9999-09-09' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = '9999-10-01'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('9999-10-01' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = '2201-07-01'::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND ('2201-07-01' IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    WHEN (
                                        (a.launch_dt = NULL::date)
                                        OR (
                                            (a.launch_dt IS NULL)
                                            AND (NULL IS NULL)
                                        )
                                    ) THEN '2011-01-01'::date
                                    ELSE a.launch_dt
                                END DESC
                        ) derived_table1
                ) derived_table2
            WHERE (derived_table2.rowno = 1)
        ) t2
    WHERE 
        (
            ((t1.master_code)::text = (t2.master_code)::text)
            AND ((t1.sales_org)::text = (t2.sales_org)::text)
        )
)
select * from final