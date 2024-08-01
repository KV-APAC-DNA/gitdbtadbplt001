with 
edw_hcp360_in_ventasys_call_fact as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_call_fact') }}
),
edw_hcp360_in_ventasys_hcp_dim_latest as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_hcp_dim_latest') }}
),
final as 
(
    SELECT derived_table2.hcp_id
	,derived_table2.hcp_master_id
	,derived_table2.territory_id
	,derived_table2.customer_name
	,derived_table2.customer_type
	,derived_table2.qualification
	,derived_table2.speciality
	,derived_table2.core_noncore
	,derived_table2.classification
	,derived_table2.is_fbm_adopted
	,derived_table2.planned_visits_per_month
	,derived_table2.cell_phone
	,derived_table2.phone
	,derived_table2.email
	,derived_table2.city
	,derived_table2.STATE
	,derived_table2.is_active
	,derived_table2.first_rx_date
	,derived_table2.cust_entered_date
	,derived_table2.valid_from
	,derived_table2.valid_to
	,derived_table2.crt_dttm
	,derived_table2.updt_dttm
FROM (
	SELECT a.hcp_id
		,a.hcp_master_id
		,a.territory_id
		,a.customer_name
		,a.customer_type
		,a.qualification
		,a.speciality
		,a.core_noncore
		,a.classification
		,a.is_fbm_adopted
		,a.planned_visits_per_month
		,a.cell_phone
		,a.phone
		,a.email
		,a.city
		,a.STATE
		,a.is_active
		,a.first_rx_date
		,a.cust_entered_date
		,a.valid_from1 AS valid_from
		,a.valid_to
		,a.crt_dttm
		,a.updt_dttm
	FROM (
		SELECT hcp1.hcp_id
			,hcp1.hcp_master_id
			,hcp1.territory_id
			,hcp1.customer_name
			,hcp1.customer_type
			,hcp1.qualification
			,hcp1.speciality
			,hcp1.core_noncore
			,hcp1.classification
			,hcp1.is_fbm_adopted
			,hcp1.planned_visits_per_month
			,hcp1.cell_phone
			,hcp1.phone
			,hcp1.email
			,hcp1.city
			,hcp1.STATE
			,hcp1.is_active
			,hcp1.first_rx_date
			,hcp1.cust_entered_date
			,hcp1.valid_from
			,hcp1.valid_to
			,hcp1.crt_dttm
			,hcp1.updt_dttm
			,CASE 
				WHEN (hcp1.valid_from > hcp2."call")
					THEN (hcp2."call")::TIMESTAMP without TIME zone
				ELSE hcp1.valid_from
				END AS valid_from1
			,row_number() OVER (
				PARTITION BY hcp1.hcp_id ORDER BY hcp1.valid_from
				) AS rn
		FROM edw_hcp360_in_ventasys_hcp_dim_latest hcp1
			,(
				SELECT h.hcp_id
					,min(c.call_date) AS "call"
				FROM (
					edw_hcp360_in_ventasys_hcp_dim_latest h LEFT JOIN edw_hcp360_in_ventasys_call_fact c ON (((h.hcp_id)::TEXT = (c.hcp_id)::TEXT))
					)
				GROUP BY h.hcp_id
				) hcp2
		WHERE ((hcp1.hcp_id)::TEXT = (hcp2.hcp_id)::TEXT)
		) a
	WHERE (a.rn = 1)
	
	UNION
	
	SELECT derived_table1.hcp_id
		,derived_table1.hcp_master_id
		,derived_table1.territory_id
		,derived_table1.customer_name
		,derived_table1.customer_type
		,derived_table1.qualification
		,derived_table1.speciality
		,derived_table1.core_noncore
		,derived_table1.classification
		,derived_table1.is_fbm_adopted
		,derived_table1.planned_visits_per_month
		,derived_table1.cell_phone
		,derived_table1.phone
		,derived_table1.email
		,derived_table1.city
		,derived_table1.STATE
		,derived_table1.is_active
		,derived_table1.first_rx_date
		,derived_table1.cust_entered_date
		,derived_table1.valid_from
		,derived_table1.valid_to
		,derived_table1.crt_dttm
		,derived_table1.updt_dttm
	FROM (
		SELECT hcp1.hcp_id
			,hcp1.hcp_master_id
			,hcp1.territory_id
			,hcp1.customer_name
			,hcp1.customer_type
			,hcp1.qualification
			,hcp1.speciality
			,hcp1.core_noncore
			,hcp1.classification
			,hcp1.is_fbm_adopted
			,hcp1.planned_visits_per_month
			,hcp1.cell_phone
			,hcp1.phone
			,hcp1.email
			,hcp1.city
			,hcp1.STATE
			,hcp1.is_active
			,hcp1.first_rx_date
			,hcp1.cust_entered_date
			,hcp1.valid_from
			,hcp1.valid_to
			,hcp1.crt_dttm
			,hcp1.updt_dttm
			,row_number() OVER (
				PARTITION BY hcp1.hcp_id ORDER BY hcp1.valid_from
				) AS rn
		FROM edw_hcp360_in_ventasys_hcp_dim_latest hcp1
		) derived_table1
	WHERE (derived_table1.rn <> 1)
	) derived_table2
)
select * from final