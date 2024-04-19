with vw_customer_dim_v2 as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.VW_CUSTOMER_DIM_V2
),
vw_customer_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.VW_CUSTOMER_DIM
),
final as (
SELECT 
  derived_table1.cust_no, 
  derived_table1.cmp_id, 
  derived_table1.channel_cd, 
  derived_table1.channel_desc, 
  derived_table1.ctry_key, 
  derived_table1.country, 
  derived_table1.state_cd, 
  derived_table1.post_cd, 
  derived_table1.cust_suburb, 
  derived_table1.cust_nm, 
  derived_table1.sls_org, 
  derived_table1.cust_del_flag, 
  derived_table1.sales_office_cd, 
  derived_table1.sales_office_desc, 
  derived_table1.sales_grp_cd, 
  derived_table1.sales_grp_desc, 
  derived_table1.mercia_ref, 
  derived_table1.curr_cd 
FROM 
  (
    SELECT 
      a.cust_no, 
      a.cmp_id, 
      a.channel_cd, 
      a.channel_desc, 
      a.ctry_key, 
      a.country, 
      a.state_cd, 
      a.post_cd, 
      a.cust_suburb, 
      a.cust_nm, 
      a.sls_org, 
      a.cust_del_flag, 
      a.sales_office_cd, 
      a.sales_office_desc, 
      a.sales_grp_cd, 
      a.sales_grp_desc, 
      a.mercia_ref, 
      a.curr_cd 
    FROM 
      (
        vw_customer_dim_v2 a 
        LEFT JOIN vw_customer_dim b ON (
          (
            (
              ltrim(
                (a.cust_no):: text, 
                ('0' :: character varying):: text
              ) = ltrim(
                (b.cust_no):: text, 
                ('0' :: character varying):: text
              )
            ) 
            AND (
              (a.cmp_id):: text = (b.cmp_id):: text
            )
          )
        )
      ) 
    WHERE 
      (
        (
          ltrim(
            (b.cust_no):: text, 
            ('0' :: character varying):: text
          ) IS NULL
        ) 
        OR (b.cmp_id IS NULL)
      )
  ) derived_table1
  )
  select * from final
