with mt_item_cdd as(
	select * from {{ ref('jpnedw_integration__mt_item_cdd') }}
),
mt_frnch_cdd as(
	select * from {{ ref('jpnedw_integration__mt_frnch_cdd') }}
),
ph_l1 as(
		SELECT mt_frnch_cdd.ph_cd
		,mt_frnch_cdd.ph_lvl
		,mt_frnch_cdd.ph_nm
		,mt_frnch_cdd.ph_srt
		,mt_frnch_cdd.update_dt
		,mt_frnch_cdd.update_user
	FROM mt_frnch_cdd
	WHERE ((mt_frnch_cdd.ph_lvl)::TEXT = ('1'::CHARACTER VARYING)::TEXT)

),
ph_l2 as(
		SELECT mt_frnch_cdd.ph_cd
		,mt_frnch_cdd.ph_lvl
		,mt_frnch_cdd.ph_nm
		,mt_frnch_cdd.ph_srt
		,mt_frnch_cdd.update_dt
		,mt_frnch_cdd.update_user
	FROM mt_frnch_cdd
	WHERE ((mt_frnch_cdd.ph_lvl)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
),
ph_l3 as(
		SELECT mt_frnch_cdd.ph_cd
		,mt_frnch_cdd.ph_lvl
		,mt_frnch_cdd.ph_nm
		,mt_frnch_cdd.ph_srt
		,mt_frnch_cdd.update_dt
		,mt_frnch_cdd.update_user
	FROM mt_frnch_cdd
	WHERE ((mt_frnch_cdd.ph_lvl)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
),
ph_l4 as(
		SELECT mt_frnch_cdd.ph_cd
		,mt_frnch_cdd.ph_lvl
		,mt_frnch_cdd.ph_nm
		,mt_frnch_cdd.ph_srt
		,mt_frnch_cdd.update_dt
		,mt_frnch_cdd.update_user
	FROM mt_frnch_cdd
	WHERE ((mt_frnch_cdd.ph_lvl)::TEXT = ('4'::CHARACTER VARYING)::TEXT)

),
ph_l5 as(
			SELECT mt_frnch_cdd.ph_cd
			,mt_frnch_cdd.ph_lvl
			,mt_frnch_cdd.ph_nm
			,mt_frnch_cdd.ph_srt
			,mt_frnch_cdd.update_dt
			,mt_frnch_cdd.update_user
		FROM mt_frnch_cdd
		WHERE ((mt_frnch_cdd.ph_lvl)::TEXT = ('5'::CHARACTER VARYING)::TEXT)
),
transformed as(
SELECT item_cdd.item_cd
	,ph_l1.ph_cd AS frnch_group_cd
	,ph_l1.ph_nm AS frnch_group_nm
	,ph_l1.ph_srt AS frnch_group_srt
	,ph_l2.ph_cd AS frnch_cd
	,ph_l2.ph_nm AS frnch_nm
	,ph_l2.ph_srt AS frnch_srt
	,ph_l3.ph_cd AS mjr_prod_cd
	,ph_l3.ph_nm AS mjr_prod_nm
	,ph_l3.ph_srt AS mjr_prod_srt
	,ph_l4.ph_cd AS mjr_prod_cd2
	,ph_l4.ph_nm AS mjr_prod_nm2
	,ph_l4.ph_srt AS mjr_prod_srt2
	,ph_l5.ph_cd AS min_prod_cd
	,ph_l5.ph_nm AS min_prod_nm
	,ph_l5.ph_srt AS min_prod_srt
FROM (
		(
		(
			(
				(
					(
						SELECT mt_item_cdd.item_cd
							,mt_item_cdd.pc
							,mt_item_cdd.unit_prc
							,mt_item_cdd.jpcd_ph
							,mt_item_cdd.jan_cd
							,mt_item_cdd.update_dt
							,mt_item_cdd.update_user
						FROM mt_item_cdd
						) item_cdd 
						LEFT JOIN ph_l1 ON (
							(
								"substring" (
									(item_cdd.jpcd_ph)::TEXT
									,1
									,1
									) = (ph_l1.ph_cd)::TEXT
								)
							)
					) LEFT JOIN ph_l2 ON (
						(
							"substring" (
								(item_cdd.jpcd_ph)::TEXT
								,1
								,2
								) = (ph_l2.ph_cd)::TEXT
							)
						)
				) LEFT JOIN ph_l3 ON (
					(
						"substring" (
							(item_cdd.jpcd_ph)::TEXT
							,1
							,3
							) = (ph_l3.ph_cd)::TEXT
						)
					)
			) LEFT JOIN ph_l4 ON (
				(
					"substring" (
						(item_cdd.jpcd_ph)::TEXT
						,1
						,5
						) = (ph_l4.ph_cd)::TEXT
					)
				)
		) LEFT JOIN ph_l5 ON (
			(
				"substring" (
					(item_cdd.jpcd_ph)::TEXT
					,1
					,7
					) = (ph_l5.ph_cd)::TEXT
				)
			)
	)
)
select * from transformed