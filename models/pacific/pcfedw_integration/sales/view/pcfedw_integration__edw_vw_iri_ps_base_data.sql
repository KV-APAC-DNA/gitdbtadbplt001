with edw_perenso_account_dim as(
select * from {{ ref('pcfedw_integration__edw_perenso_account_dim') }}
),
edw_iri_scan_sales_agg as(
select * from {{ ref('pcfedw_integration__edw_iri_scan_sales_agg') }}
),
edw_ps_msl_items as(
select * from {{ ref('pcfedw_integration__edw_ps_msl_items') }}
),
base_data as(
SELECT derived_table1.jj_mnth_id
			,derived_table1.wk_end_dt
			,derived_table1.iri_market
			,derived_table2.ean
		FROM (
			SELECT DISTINCT edw_iri_scan_sales_agg.jj_mnth_id
				,edw_iri_scan_sales_agg.wk_end_dt
				,edw_iri_scan_sales_agg.iri_market
			FROM edw_iri_scan_sales_agg
			WHERE (upper((edw_iri_scan_sales_agg.iri_market)::TEXT) = ('AU MY CHEMIST GROUP SCAN'::CHARACTER VARYING)::TEXT)
			) derived_table1
			,(
				SELECT DISTINCT edw_ps_msl_items.ean
				FROM edw_ps_msl_items
				WHERE (upper((edw_ps_msl_items.retail_environment)::TEXT) = ('BIG BOX'::CHARACTER VARYING)::TEXT)
				) derived_table2
		
		UNION ALL
		
		SELECT derived_table3.jj_mnth_id
			,derived_table3.wk_end_dt
			,derived_table3.iri_market
			,derived_table4.ean
		FROM (
			SELECT DISTINCT edw_iri_scan_sales_agg.jj_mnth_id
				,edw_iri_scan_sales_agg.wk_end_dt
				,edw_iri_scan_sales_agg.iri_market
			FROM edw_iri_scan_sales_agg
			WHERE (
					(upper((edw_iri_scan_sales_agg.iri_market)::TEXT) = ('AU WOOLWORTHS SCAN'::CHARACTER VARYING)::TEXT)
					OR (upper((edw_iri_scan_sales_agg.iri_market)::TEXT) = ('AU COLES GROUP SCAN'::CHARACTER VARYING)::TEXT)
					)
			) derived_table3
			,(
				SELECT DISTINCT edw_ps_msl_items.ean
				FROM edw_ps_msl_items
				WHERE (upper((edw_ps_msl_items.retail_environment)::TEXT) = ('AU MAJOR CHAIN SUPER'::CHARACTER VARYING)::TEXT)
				) derived_table4
),
perenso_acct as(
SELECT acct_dim.acct_id AS acct_key
			,acct_dim.acct_display_name
			,acct_dim.acct_country
			,acct_dim.acct_state
			,acct_dim.acct_banner
			,CASE 
				WHEN (
						(upper((acct_dim.acct_banner)::TEXT) = ('MY CHEMIST'::CHARACTER VARYING)::TEXT)
						OR (upper((acct_dim.acct_banner)::TEXT) = ('CHEMIST WAREHOUSE'::CHARACTER VARYING)::TEXT)
						)
					THEN ('CHEMIST WAREHOUSE'::CHARACTER VARYING)::TEXT
				ELSE upper((acct_dim.acct_banner)::TEXT)
				END AS acct_banner_map
			,CASE 
				WHEN (upper((acct_dim.acct_ind_groc_territory)::TEXT) <> ('NOT ASSIGNED'::CHARACTER VARYING)::TEXT)
					THEN acct_dim.acct_ind_groc_territory
				WHEN (upper((acct_dim.acct_au_pharma_territory)::TEXT) <> ('NOT ASSIGNED'::CHARACTER VARYING)::TEXT)
					THEN acct_dim.acct_au_pharma_territory
				WHEN (upper((acct_dim.acct_nz_pharma_territory)::TEXT) <> ('NOT ASSIGNED'::CHARACTER VARYING)::TEXT)
					THEN acct_dim.acct_nz_pharma_territory
				ELSE 'NOT ASSIGNED'::CHARACTER VARYING
				END AS acct_terriroty
			,CASE 
				WHEN (
						((acct_dim.acct_grade)::TEXT like 'A%'::TEXT)
						OR ((acct_dim.acct_grade)::TEXT like '% A %'::TEXT)
						)
					THEN 'A'::CHARACTER VARYING
				WHEN (
						((acct_dim.acct_grade)::TEXT like 'B%'::TEXT)
						OR ((acct_dim.acct_grade)::TEXT like '% B %'::TEXT)
						)
					THEN 'B'::CHARACTER VARYING
				WHEN (
						(
							((acct_dim.acct_grade)::TEXT like 'C%'::TEXT)
							OR ((acct_dim.acct_grade)::TEXT like '% C %'::TEXT)
							)
						AND ((acct_dim.acct_grade)::TEXT not like 'Ch%'::TEXT)
						)
					THEN 'C'::CHARACTER VARYING
				WHEN (upper((acct_dim.acct_grade)::TEXT) = 'INACTIVE ACCOUNTS'::TEXT)
					THEN acct_dim.acct_grade
				ELSE 'D'::CHARACTER VARYING
				END AS store_grade
		FROM edw_perenso_account_dim acct_dim

),
final as(
    SELECT base_data.jj_mnth_id
        ,base_data.wk_end_dt
        ,base_data.iri_market
        ,base_data.ean
        ,perenso_acct.acct_key
        ,perenso_acct.acct_display_name
        ,perenso_acct.acct_country
        ,perenso_acct.acct_state
        ,perenso_acct.acct_banner
        ,perenso_acct.acct_terriroty
        ,perenso_acct.store_grade
    FROM (
        base_data LEFT JOIN perenso_acct ON (
                (
                    (
                        CASE 
                            WHEN (upper((base_data.iri_market)::TEXT) = ('AU WOOLWORTHS SCAN'::CHARACTER VARYING)::TEXT)
                                THEN 'WOOLWORTHS'::CHARACTER VARYING
                            WHEN (upper((base_data.iri_market)::TEXT) = ('AU COLES GROUP SCAN'::CHARACTER VARYING)::TEXT)
                                THEN 'COLES'::CHARACTER VARYING
                            WHEN (upper((base_data.iri_market)::TEXT) = ('AU MY CHEMIST GROUP SCAN'::CHARACTER VARYING)::TEXT)
                                THEN 'CHEMIST WAREHOUSE'::CHARACTER VARYING
                            ELSE NULL::CHARACTER VARYING
                            END
                        )::TEXT = perenso_acct.acct_banner_map
                    )
                )
        )
)
select * from final