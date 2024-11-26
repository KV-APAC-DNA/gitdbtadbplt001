with ventasys_hcp_master AS (
   select * from DEV_DNA_CORE.HCPITG_INTEGRATION.ITG_HCP360_IN_VENTASYS_HCP_MASTER
),


final as (

    SELECT

        hcp.core_noncore AS hcpmaster_core_noncore,

        hcp.cust_spec AS hcpmaster_specialty,

        COUNT(DISTINCT hcp.v_custid) AS hcpmaster_unique_doctors_count

    FROM ventasys_hcp_master hcp

WHERE hcp.is_active = 'Y'

GROUP BY 1, 2 
)

select * from final