with 
edw_customer_base_dim as 
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
itg_region as 
(
    select * from {{ source('inditg_integration', 'itg_region') }}
),
itg_zone as 
(
    select * from {{ source('inditg_integration', 'itg_zone') }}
),
itg_zone_classification as 
(
    select * from {{ source('inditg_integration', 'itg_zone_classification') }}
),
itg_territory as 
(
    select * from {{ source('inditg_integration', 'itg_territory') }}
),
itg_territory_classification as 
(
    select * from {{ source('inditg_integration', 'itg_territory_classification') }}
),
itg_state as 
(
    select * from {{ source('inditg_integration', 'itg_state') }}
),
itg_town as 
(
    select * from {{ source('inditg_integration', 'itg_town') }}
),
itg_town_classification as 
(
    select * from {{ source('inditg_integration', 'itg_town_classification') }}
),
itg_customer as 
(
    select * from {{ source('inditg_integration', 'itg_customer') }}
),
itg_customertype as 
(
    select * from {{ source('inditg_integration', 'itg_customertype') }}
),
itg_rdssize as 
(
    select * from {{ source('inditg_integration', 'itg_rdssize') }}
),
itg_muser as 
(
    select * from {{ source('inditg_integration', 'itg_muser') }}
),
itg_distributoractivation as 
(
    select * from {{ ref('inditg_integration__itg_distributoractivation') }}
),
final as 
(
    SELECT 
    (ltrim((a.sapid)::TEXT, ('0'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS customer_code,
	CASE 
		WHEN (a.regioncode IS NULL)
			THEN (- ((1)::NUMERIC)::NUMERIC(18, 0))
		ELSE a.regioncode
		END AS region_code,
	COALESCE(c.regionname, 'Unknown'::CHARACTER VARYING) AS region_name,
	CASE 
		WHEN (a.zonecode IS NULL)
			THEN (- ((1)::NUMERIC)::NUMERIC(18, 0))
		ELSE a.zonecode
		END AS zone_code,
	COALESCE(d.zonename, 'Unknown'::CHARACTER VARYING) AS zone_name,
	COALESCE(e.classification, 'Unknown'::CHARACTER VARYING) AS zone_classification,
	CASE 
		WHEN (a.territorycode IS NULL)
			THEN (- ((1)::NUMERIC)::NUMERIC(18, 0))
		ELSE a.territorycode
		END AS territory_code,
	COALESCE(f.territoryname, 'Unknown'::CHARACTER VARYING) AS territory_name,
	COALESCE(g.classification, 'Unknown'::CHARACTER VARYING) AS territory_classification,
	CASE 
		WHEN (a.statecode IS NULL)
			THEN (- ((1)::NUMERIC)::NUMERIC(18, 0))
		ELSE a.statecode
		END AS state_code,
	COALESCE(h.statename, 'Unknown'::CHARACTER VARYING) AS state_name,
	CASE 
		WHEN (a.towncode IS NULL)
			THEN (- ((1)::NUMERIC)::NUMERIC(18, 0))
		ELSE a.towncode
		END AS town_code,
	COALESCE(i.townname, 'Unknown'::CHARACTER VARYING) AS town_name,
	COALESCE(j.town_classification, 'Unknown'::CHARACTER VARYING) AS town_classification,
	COALESCE(b.city, 'Unknown'::CHARACTER VARYING) AS city,
	CASE 
		WHEN (a.typecode IS NULL)
			THEN (- ((1)::NUMERIC)::NUMERIC(18, 0))
		ELSE a.typecode
		END AS type_code,
	COALESCE("k".customertypename, 'Unknown'::CHARACTER VARYING) AS type_name,
	COALESCE(b.nm_1, 'Unknown'::CHARACTER VARYING) AS customer_name,
	COALESCE(b.nm_2, 'Unknown'::CHARACTER VARYING) AS customer_address1,
	COALESCE(b.nm_3, 'Unknown'::CHARACTER VARYING) AS customer_address2,
	COALESCE(b.nm_4, 'Unknown'::CHARACTER VARYING) AS customer_address3,
	CASE 
		WHEN (n.activestatus = 1)
			THEN 'Y'::CHARACTER VARYING
		ELSE 'N'::CHARACTER VARYING
		END AS active_flag,
	a.activedt AS active_start_date,
	COALESCE(a.wholesalercode, 'Unknown'::CHARACTER VARYING) AS wholesalercode,
	COALESCE(a.superstockist, 'Unknown'::CHARACTER VARYING) AS super_stockiest,
	COALESCE(a.isdirectacct, 'Unknown'::CHARACTER VARYING) AS direct_account_flag,
	CASE 
		WHEN (a.abicode IS NULL)
			THEN - 1
		ELSE a.abicode
		END AS abi_code,
	(
		CASE 
			WHEN (
					(m.mfirstname IS NULL)
					AND (m.mlastname IS NULL)
					)
				THEN ('Unknown'::CHARACTER VARYING)::TEXT
			ELSE (((m.mfirstname)::TEXT || (' '::CHARACTER VARYING)::TEXT) || (m.mlastname)::TEXT)
			END
		)::CHARACTER VARYING AS abi_name,
	COALESCE(l.rdssize, 'Unknown'::CHARACTER VARYING) AS rds_size,
	a.crt_dttm,
	a.updt_dttm
FROM (
	(
		(
			(
				(
					(
						(
							(
								(
									(
										(
											(
												(
													edw_customer_base_dim b JOIN itg_customer a ON (((a.sapid)::TEXT = ltrim((b.cust_num)::TEXT, ('0'::CHARACTER VARYING)::TEXT)))
													) LEFT JOIN itg_region c ON ((a.regioncode = c.regioncode))
												) LEFT JOIN itg_zone d ON ((a.zonecode = d.zonecode))
											) LEFT JOIN itg_zone_classification e ON ((a.zonecode = e.zonecode))
										) LEFT JOIN itg_territory f ON ((a.territorycode = f.territorycode))
									) LEFT JOIN itg_territory_classification g ON ((a.territorycode = g.territorycode))
								) LEFT JOIN itg_state h ON ((a.statecode = h.statecode))
							) LEFT JOIN itg_town i ON ((a.towncode = i.towncode))
						) LEFT JOIN itg_town_classification j ON ((upper((i.townname)::TEXT) = upper((j.town_name)::TEXT)))
					) LEFT JOIN itg_customertype "k" ON ((a.typecode = "k".customertypecode))
				) LEFT JOIN itg_rdssize l ON (((a.sapid)::TEXT = ((l.rdscode)::CHARACTER VARYING)::TEXT))
			) LEFT JOIN itg_muser m ON ((((a.abicode)::NUMERIC)::NUMERIC(18, 0) = m.musercode))
		) LEFT JOIN itg_distributoractivation n ON (((a.sapid)::TEXT = (n.distcode)::TEXT))
	)
WHERE ((b.ctry_key)::TEXT = ('IN'::CHARACTER VARYING)::TEXT)

UNION

SELECT '-1' AS customer_code,
	- 1 AS region_code,
	'Unknown' AS region_name,
	- 1 AS zone_code,
	'Unknown' AS zone_name,
	'Unknown' AS zone_classification,
	- 1 AS territory_code,
	'Unknown' AS territory_name,
	'Unknown' AS territory_classification,
	- 1 AS state_code,
	'Unknown' AS state_name,
	- 1 AS town_code,
	'Unknown' AS town_name,
	'Unknown' AS town_classification,
	'Unknown' AS city,
	- 1 AS type_code,
	'Unknown' AS type_name,
	'Unknown' AS customer_name,
	'Unknown' AS customer_address1,
	'Unknown' AS customer_address2,
	'Unknown' AS customer_address3,
	'Unknown' AS active_flag,
	NULL AS active_start_date,
	'Unknown' AS wholesalercode,
	'Unknown' AS super_stockiest,
	'Unknown' AS direct_account_flag,
	- 1 AS abi_code,
	'Unknown' AS abi_name,
	'Unknown' AS rds_size,
	current_timestamp() AS crt_dttm,
	current_timestamp() AS updt_dttm
)
select * from final