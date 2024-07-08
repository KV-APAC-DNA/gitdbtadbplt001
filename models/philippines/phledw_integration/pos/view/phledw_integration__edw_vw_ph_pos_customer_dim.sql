with itg_mds_ph_pos_customers as (
select * from {{ ref('phlitg_integration__itg_mds_ph_pos_customers') }}
),
final as (
    SELECT 
      'PH' AS cntry_cd, 
      'Philippines' AS cntry_nm, 
      (
        itg_mds_ph_pos_customers.cust_cd
      ):: character varying(255) AS cust_cd, 
      null AS cust_nm, 
      null AS sold_to, 
      (
        itg_mds_ph_pos_customers.brnch_cd
      ):: character varying(255) AS brnch_cd, 
      itg_mds_ph_pos_customers.brnch_nm, 
      null AS brnch_frmt, 
      null AS brnch_typ, 
      null AS dept_cd, 
      null AS dept_nm, 
      itg_mds_ph_pos_customers.address1, 
      itg_mds_ph_pos_customers.address2, 
      (
        itg_mds_ph_pos_customers.region_cd
      ):: character varying(255) AS region_cd, 
      itg_mds_ph_pos_customers.region_nm, 
      (
        itg_mds_ph_pos_customers.prov_cd
      ):: character varying(255) AS prov_cd, 
      itg_mds_ph_pos_customers.prov_nm, 
      (
        itg_mds_ph_pos_customers.city_cd
      ):: character varying(255) AS city_cd, 
      itg_mds_ph_pos_customers.city_nm, 
      (
        itg_mds_ph_pos_customers.mncplty_cd
      ):: character varying(255) AS mncplty_cd, 
      itg_mds_ph_pos_customers.mncplty_nm 
    FROM 
      itg_mds_ph_pos_customers 
    WHERE 
      (
        (
          itg_mds_ph_pos_customers.active
        ):: text = ('Y' :: character varying):: text
      ) 
    
  )
select * from final
