with itg_ims_dstr_cust_attr as (
select * from {{ ref('ntaitg_integration__itg_ims_dstr_cust_attr') }}
),
edw_sales_rep_route_plan as (
select * from ntaedw_integration.edw_sales_rep_route_plan
),
edw_ims_fact as (
select * from {{ ref('ntaedw_integration__edw_ims_fact') }}
),
final as (
SELECT 
  derived_table2.ctry_cd, 
  derived_table2.dstr_cd, 
  derived_table2.dstr_nm, 
  derived_table2.store_cd, 
  derived_table2.store_nm, 
  derived_table2.store_class, 
  derived_table2.dstr_cust_area, 
  derived_table2.dstr_cust_clsn1, 
  derived_table2.dstr_cust_clsn2, 
  derived_table2.dstr_cust_clsn3, 
  derived_table2.dstr_cust_clsn4, 
  derived_table2.dstr_cust_clsn5, 
  derived_table2.effctv_strt_dt, 
  derived_table2.effctv_end_dt, 
  derived_table2.distributor_address, 
  derived_table2.distributor_telephone, 
  derived_table2.distributor_contact, 
  derived_table2.store_type, 
  derived_table2.hq 
FROM 
  (
    SELECT 
      derived_table1.ctry_cd, 
      derived_table1.dstr_cd, 
      derived_table1.dstr_nm, 
      derived_table1.store_cd, 
      derived_table1.store_nm, 
      derived_table1.store_class, 
      derived_table1.dstr_cust_area, 
      derived_table1.dstr_cust_clsn1, 
      derived_table1.dstr_cust_clsn2, 
      derived_table1.dstr_cust_clsn3, 
      derived_table1.dstr_cust_clsn4, 
      derived_table1.dstr_cust_clsn5, 
      derived_table1.effctv_strt_dt, 
      derived_table1.effctv_end_dt, 
      derived_table1.distributor_address, 
      derived_table1.distributor_telephone, 
      derived_table1.distributor_contact, 
      derived_table1.store_type, 
      derived_table1.hq 
    FROM 
      (
        (
          SELECT 
            DISTINCT itg_ims_dstr_cust_attr.ctry_cd, 
            itg_ims_dstr_cust_attr.dstr_cd, 
            itg_ims_dstr_cust_attr.dstr_nm, 
            itg_ims_dstr_cust_attr.dstr_cust_cd AS store_cd, 
            itg_ims_dstr_cust_attr.dstr_cust_nm AS store_nm, 
            b.store_class, 
            itg_ims_dstr_cust_attr.dstr_cust_area, 
            itg_ims_dstr_cust_attr.dstr_cust_clsn1, 
            itg_ims_dstr_cust_attr.dstr_cust_clsn2, 
            itg_ims_dstr_cust_attr.dstr_cust_clsn3, 
            itg_ims_dstr_cust_attr.dstr_cust_clsn4, 
            itg_ims_dstr_cust_attr.dstr_cust_clsn5, 
            (b.effctv_strt_dt):: character varying AS effctv_strt_dt, 
            (b.effctv_end_dt):: character varying AS effctv_end_dt, 
            itg_ims_dstr_cust_attr.distributor_address, 
            itg_ims_dstr_cust_attr.distributor_telephone, 
            itg_ims_dstr_cust_attr.distributor_contact, 
            itg_ims_dstr_cust_attr.store_type, 
            itg_ims_dstr_cust_attr.hq 
          FROM 
            (
              itg_ims_dstr_cust_attr itg_ims_dstr_cust_attr 
              LEFT JOIN edw_sales_rep_route_plan b ON (
                (
                  (
                    (
                      (itg_ims_dstr_cust_attr.ctry_cd):: text = (b.ctry_cd):: text
                    ) 
                    AND (
                      (itg_ims_dstr_cust_attr.dstr_cd):: text = (b.dstr_cd):: text
                    )
                  ) 
                  AND (
                    (
                      itg_ims_dstr_cust_attr.dstr_cust_cd
                    ):: text = (b.store_cd):: text
                  )
                )
              )
            ) 
          UNION 
          SELECT 
            DISTINCT a.ctry_cd, 
            a.dstr_cd, 
            b.dstr_nm, 
            a.store_cd, 
            a.store_nm, 
            a.store_class, 
            NULL :: character varying AS dstr_cust_area, 
            NULL :: character varying AS dstr_cust_clsn1, 
            NULL :: character varying AS dstr_cust_clsn2, 
            NULL :: character varying AS dstr_cust_clsn3, 
            NULL :: character varying AS dstr_cust_clsn4, 
            NULL :: character varying AS dstr_cust_clsn5, 
            (a.effctv_strt_dt):: character varying AS effctv_strt_dt, 
            (a.effctv_end_dt):: character varying AS effctv_end_dt, 
            NULL :: character varying AS distributor_address, 
            NULL :: character varying AS distributor_telephone, 
            NULL :: character varying AS distributor_contact, 
            NULL :: character varying AS store_type, 
            NULL :: character varying AS hq 
          FROM 
            edw_sales_rep_route_plan a, 
            edw_ims_fact b 
          WHERE 
            (
              (
                (
                  (a.ctry_cd):: text = (b.ctry_cd):: text
                ) 
                AND (
                  (a.dstr_cd):: text = (b.dstr_cd):: text
                )
              ) 
              AND (
                NOT (
                  a.store_cd IN (
                    SELECT 
                      DISTINCT itg_ims_dstr_cust_attr.dstr_cust_cd 
                    FROM 
                      itg_ims_dstr_cust_attr
                  )
                )
              )
            )
        ) 
        UNION 
        SELECT 
          DISTINCT a.ctry_cd, 
          a.dstr_cd, 
          a.dstr_nm, 
          a.cust_cd AS store_cd, 
          a.cust_nm AS store_nm, 
          NULL :: character varying AS store_class, 
          NULL :: character varying AS dstr_cust_area, 
          NULL :: character varying AS dstr_cust_clsn1, 
          NULL :: character varying AS dstr_cust_clsn2, 
          NULL :: character varying AS dstr_cust_clsn3, 
          NULL :: character varying AS dstr_cust_clsn4, 
          NULL :: character varying AS dstr_cust_clsn5, 
          NULL :: character varying AS effctv_strt_dt, 
          NULL :: character varying AS effctv_end_dt, 
          NULL :: character varying AS distributor_address, 
          NULL :: character varying AS distributor_telephone, 
          NULL :: character varying AS distributor_contact, 
          NULL :: character varying AS store_type, 
          NULL :: character varying AS hq 
        FROM 
          (
            edw_ims_fact a 
            JOIN (
              SELECT 
                edw_ims_fact.cust_cd, 
                "max"(edw_ims_fact.ims_txn_dt) AS ims_txn_dt 
              FROM 
                edw_ims_fact 
              WHERE 
                (
                  (
                    (
                      (edw_ims_fact.cust_nm):: text ! like ('%?%' :: character varying):: text
                    ) 
                    AND (
                      NOT (
                        edw_ims_fact.cust_cd IN (
                          SELECT 
                            DISTINCT itg_ims_dstr_cust_attr.dstr_cust_cd 
                          FROM 
                            itg_ims_dstr_cust_attr
                        )
                      )
                    )
                  ) 
                  AND (
                    NOT (
                      edw_ims_fact.cust_cd IN (
                        SELECT 
                          DISTINCT edw_sales_rep_route_plan.store_cd 
                        FROM 
                          edw_sales_rep_route_plan
                      )
                    )
                  )
                ) 
              GROUP BY 
                edw_ims_fact.cust_cd
            ) b ON (
              (
                (
                  (a.cust_cd):: text = (b.cust_cd):: text
                ) 
                AND (a.ims_txn_dt = b.ims_txn_dt)
              )
            )
          )
      ) derived_table1
  ) derived_table2
  )
  select * from final