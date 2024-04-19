with customer_control_tp_accrual_reversal_ac as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.CUSTOMER_CONTROL_TP_ACCRUAL_REVERSAL_AC
),
edw_customer_base_dim as (
select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_CUSTOMER_BASE_DIM
),
edw_customer_sales_dim as (
select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_CUSTOMER_SALES_DIM
),
dly_sls_cust_attrb_lkp as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.DLY_SLS_CUST_ATTRB_LKP
),
final as (
SELECT 
  cust.cust_no, 
  pac_lkup.cmp_id, 
  pac_lkup.chnl_cd AS channel_cd, 
  pac_lkup.chnl_desc AS channel_desc, 
  cust.ctry_key, 
  pac_lkup.country, 
  cust.state_cd, 
  cust.post_cd, 
  cust.cust_suburb, 
  cust.cust_nm, 
  pac_lkup.sls_org, 
  cust.cust_del_flag, 
  pac_lkup.sls_ofc AS sales_office_cd, 
  pac_lkup.sls_ofc_desc AS sales_office_desc, 
  pac_lkup.sls_grp AS sales_grp_cd, 
  pac_lkup.sls_grp_desc AS sales_grp_desc, 
  cust.mercia_ref, 
  cust.curr_cd 
FROM 
  (
    (
      SELECT 
        customer.cust_no, 
        min(
          (customer.cmp_id):: text
        ) AS cmp_id, 
        min(
          (customer.channel_cd):: text
        ) AS channel_cd, 
        min(
          (customer.channel_desc):: text
        ) AS channel_desc, 
        min(
          (customer.ctry_key):: text
        ) AS ctry_key, 
        min(
          (customer.country):: text
        ) AS country, 
        min(
          (customer.state_cd):: text
        ) AS state_cd, 
        min(
          (customer.post_cd):: text
        ) AS post_cd, 
        min(
          (customer.cust_suburb):: text
        ) AS cust_suburb, 
        min(
          (customer.cust_nm):: text
        ) AS cust_nm, 
        min(
          (customer.sls_org):: text
        ) AS sls_org, 
        min(
          (customer.cust_del_flag):: text
        ) AS cust_del_flag, 
        min(
          (customer.sales_office_cd):: text
        ) AS sales_office_cd, 
        min(
          (customer.sales_office_desc):: text
        ) AS sales_office_desc, 
        min(
          (customer.sales_grp_cd):: text
        ) AS sales_grp_cd, 
        min(
          (customer.sales_grp_desc):: text
        ) AS sales_grp_desc, 
        min(
          (customer.mercia_ref):: text
        ) AS mercia_ref, 
        min(
          (customer.curr_cd):: text
        ) AS curr_cd 
      FROM 
        (
          SELECT 
            DISTINCT cust.cust_num AS cust_no, 
            pac_lkup.cmp_id, 
            pac_lkup.chnl_cd AS channel_cd, 
            pac_lkup.chnl_desc AS channel_desc, 
            cust.ctry_key, 
            pac_lkup.country, 
            cust.rgn AS state_cd, 
            cust.pstl_cd AS post_cd, 
            cust.city AS cust_suburb, 
            cust.cust_nm, 
            pac_lkup.sls_org, 
            cust_sales.cust_del_flag, 
            pac_lkup.sls_ofc AS sales_office_cd, 
            pac_lkup.sls_ofc_desc AS sales_office_desc, 
            CASE WHEN (
              ltrim(
                (cust.cust_num):: text, 
                ('0' :: character varying):: text
              ) = ltrim(
                (control.cust_no):: text, 
                ('0' :: character varying):: text
              )
            ) THEN control.sls_grp ELSE pac_lkup.sls_grp END AS sales_grp_cd, 
            pac_lkup.sls_grp_desc AS sales_grp_desc, 
            cust.fcst_chnl AS mercia_ref, 
            cust_sales.crncy_key AS curr_cd 
          FROM 
            customer_control_tp_accrual_reversal_ac control, 
            edw_customer_base_dim cust, 
            (
              SELECT 
                a.cust_num, 
                min(
                  (
                    CASE WHEN (
                      (
                        (a.cust_del_flag):: text = (NULL :: character varying):: text
                      ) 
                      OR (
                        (a.cust_del_flag IS NULL) 
                        AND (null IS NULL)
                      )
                    ) THEN 'O' :: character varying WHEN (
                      (
                        (a.cust_del_flag):: text = ('' :: character varying):: text
                      ) 
                      OR (
                        (a.cust_del_flag IS NULL) 
                        AND ('' IS NULL)
                      )
                    ) THEN 'O' :: character varying ELSE a.cust_del_flag END
                  ):: text
                ) AS cust_del_flag 
              FROM 
                edw_customer_sales_dim a, 
                (
                  SELECT 
                    DISTINCT dly_sls_cust_attrb_lkp.sls_org 
                  FROM 
                    dly_sls_cust_attrb_lkp
                ) b 
              WHERE 
                (
                  (a.sls_org):: text = (b.sls_org):: text
                ) 
              GROUP BY 
                a.cust_num
            ) req_cust_rec, 
            (
              edw_customer_sales_dim cust_sales 
              LEFT JOIN dly_sls_cust_attrb_lkp pac_lkup ON (
                (
                  (cust_sales.sls_grp):: text = (pac_lkup.sls_grp):: text
                )
              )
            ) 
          WHERE 
            (
              (
                (
                  (cust_sales.cust_num):: text = (cust.cust_num):: text
                ) 
                AND (
                  (cust_sales.cust_num):: text = (req_cust_rec.cust_num):: text
                )
              ) 
              AND (
                (
                  CASE WHEN (
                    (
                      (cust_sales.cust_del_flag):: text = (NULL :: character varying):: text
                    ) 
                    OR (
                      (cust_sales.cust_del_flag IS NULL) 
                      AND (null )
                    )
                  ) THEN 'O' :: character varying WHEN (
                    (
                      (cust_sales.cust_del_flag):: text = ('' :: character varying):: text
                    ) 
                    OR (
                      (cust_sales.cust_del_flag IS NULL) 
                      AND ('' IS NULL)
                    )
                  ) THEN 'O' :: character varying ELSE cust_sales.cust_del_flag END
                ):: text = req_cust_rec.cust_del_flag
              )
            )
        ) customer 
      GROUP BY 
        customer.cust_no, 
        customer.cmp_id
    ) cust 
    LEFT JOIN dly_sls_cust_attrb_lkp pac_lkup ON (
      (
        (
          (cust.sales_grp_cd):: character varying
        ):: text = (pac_lkup.sls_grp):: text
      )
    )
  ) 
WHERE 
  (
    (
      ltrim(
        (cust.cust_no):: text, 
        ('0' :: character varying):: text
      ) not like ('7%' :: character varying):: text
    ) 
    OR (
      cust.cust_no IN (
        SELECT 
          DISTINCT customer_control_tp_accrual_reversal_ac.cust_no 
        FROM 
          customer_control_tp_accrual_reversal_ac
      )
    )
  )
  )
select * from final