with itg_mds_pacific_terms_master as (
select * from {{ ref('pcfitg_integration__itg_mds_pacific_terms_master') }}
),
vw_customer_dim as (
select * from {{ ref('pcfedw_integration__vw_customer_dim') }}
),
final as (
SELECT 
  terms.country, 
  terms.sls_grp_cd, 
  terms.sls_grp_nm, 
  (terms.cust_id):: character varying AS cust_id, 
  (terms.cust_nm):: character varying AS cust_nm, 
  terms.terms_percentage 
FROM 
  (
    SELECT 
      imptm.country, 
      imptm.sls_grp_cd, 
      imptm.sls_grp_nm, 
      ltrim(
        (imptm.sls_grp_cd):: text, 
        ('0' :: character varying):: text
      ) AS cust_id, 
      vcd.cust_nm, 
      imptm.terms_percentage 
    FROM 
      itg_mds_pacific_terms_master imptm, 
      vw_customer_dim vcd 
    WHERE 
      (
        (imptm.sls_grp_cd):: text = ltrim(
          (vcd.cust_no):: text, 
          ('0' :: character varying):: text
        )
      )
  ) terms 
UNION ALL 
SELECT 
  imptm.country, 
  imptm.sls_grp_cd, 
  imptm.sls_grp_nm, 
  (
    ltrim(
      (vcd.cust_no):: text, 
      ('0' :: character varying):: text
    )
  ):: character varying AS cust_id, 
  (vcd.cust_nm):: character varying AS cust_nm, 
  imptm.terms_percentage 
FROM 
  (
    (
      SELECT 
        itg_mds_pacific_terms_master.country, 
        itg_mds_pacific_terms_master.sls_grp_cd, 
        itg_mds_pacific_terms_master.sls_grp_nm, 
        itg_mds_pacific_terms_master.terms_percentage, 
        itg_mds_pacific_terms_master.crtd_dttm 
      FROM 
        itg_mds_pacific_terms_master 
      WHERE 
        (
          length(
            (
              itg_mds_pacific_terms_master.sls_grp_cd
            ):: text
          ) <= 5
        )
    ) imptm 
    LEFT JOIN vw_customer_dim vcd ON (
      (
        (imptm.sls_grp_cd):: text = (vcd.sales_grp_cd):: text
      )
    )
  ) 
WHERE 
  (
    NOT (
      COALESCE(
        ltrim(
          (vcd.cust_no):: text, 
          ('0' :: character varying):: text
        ), 
        ('NA' :: character varying):: text
      ) IN (
        SELECT 
          DISTINCT COALESCE(
            terms.cust_id, 
            ('NA' :: character varying):: text
          ) AS "coalesce" 
        FROM 
          (
            SELECT 
              imptm.country, 
              imptm.sls_grp_cd, 
              imptm.sls_grp_nm, 
              ltrim(
                (imptm.sls_grp_cd):: text, 
                ('0' :: character varying):: text
              ) AS cust_id, 
              vcd.cust_nm, 
              imptm.terms_percentage 
            FROM 
              itg_mds_pacific_terms_master imptm, 
              vw_customer_dim vcd 
            WHERE 
              (
                (imptm.sls_grp_cd):: text = ltrim(
                  (vcd.cust_no):: text, 
                  ('0' :: character varying):: text
                )
              )
          ) terms
      )
    )
  )
)
select * from final