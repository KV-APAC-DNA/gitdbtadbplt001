with edw_vw_vn_mt_sellin_sellout_analysis as(
    select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_sellin_sellout_analysis') }}
),
transformed as(
SELECT edw_vw_vn_mt_sellin_sellout_analysis.data_source
	,edw_vw_vn_mt_sellin_sellout_analysis.data_type
	,edw_vw_vn_mt_sellin_sellout_analysis.mnth_id
	,edw_vw_vn_mt_sellin_sellout_analysis.productid
	,edw_vw_vn_mt_sellin_sellout_analysis.product_name
	,edw_vw_vn_mt_sellin_sellout_analysis.barcode
	,edw_vw_vn_mt_sellin_sellout_analysis.custcode
	,edw_vw_vn_mt_sellin_sellout_analysis.name
	,edw_vw_vn_mt_sellin_sellout_analysis.sub_channel
	,edw_vw_vn_mt_sellin_sellout_analysis.region
	,edw_vw_vn_mt_sellin_sellout_analysis.province
	,edw_vw_vn_mt_sellin_sellout_analysis.kam
	,edw_vw_vn_mt_sellin_sellout_analysis.retail_environment
	,edw_vw_vn_mt_sellin_sellout_analysis.group_account
	,edw_vw_vn_mt_sellin_sellout_analysis.account
	,edw_vw_vn_mt_sellin_sellout_analysis.franchise
	,edw_vw_vn_mt_sellin_sellout_analysis.category
	,edw_vw_vn_mt_sellin_sellout_analysis.sub_category
	,edw_vw_vn_mt_sellin_sellout_analysis.sub_brand
	,edw_vw_vn_mt_sellin_sellout_analysis.sales_amt_lcy
	,edw_vw_vn_mt_sellin_sellout_analysis.sales_amt_usd
	,edw_vw_vn_mt_sellin_sellout_analysis.target_lcy
	,edw_vw_vn_mt_sellin_sellout_analysis.target_usd
FROM edw_vw_vn_mt_sellin_sellout_analysis
WHERE (
		((edw_vw_vn_mt_sellin_sellout_analysis.data_type)::TEXT = ('SELL-IN'::CHARACTER VARYING)::TEXT)
		AND (
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
													(edw_vw_vn_mt_sellin_sellout_analysis.product_name IS NULL)
													OR (edw_vw_vn_mt_sellin_sellout_analysis.sub_channel IS NULL)
													)
												OR (edw_vw_vn_mt_sellin_sellout_analysis.region IS NULL)
												)
											OR (edw_vw_vn_mt_sellin_sellout_analysis.province IS NULL)
											)
										OR (edw_vw_vn_mt_sellin_sellout_analysis.kam IS NULL)
										)
									OR (edw_vw_vn_mt_sellin_sellout_analysis.retail_environment IS NULL)
									)
								OR (edw_vw_vn_mt_sellin_sellout_analysis.group_account IS NULL)
								)
							OR (edw_vw_vn_mt_sellin_sellout_analysis.account IS NULL)
							)
						OR (edw_vw_vn_mt_sellin_sellout_analysis.franchise IS NULL)
						)
					OR (edw_vw_vn_mt_sellin_sellout_analysis.category IS NULL)
					)
				OR (edw_vw_vn_mt_sellin_sellout_analysis.sub_category IS NULL)
				)
			OR (edw_vw_vn_mt_sellin_sellout_analysis.sub_brand IS NULL)
			)
		)
)
select * from transformed