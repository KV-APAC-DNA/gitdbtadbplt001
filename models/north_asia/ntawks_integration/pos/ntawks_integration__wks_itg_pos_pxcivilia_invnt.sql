with sdl_tw_pos_px_civila as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_px_civila') }}
),
itg_pos_invnt as (
    select * from {{ source('ntaitg_integration', 'itg_pos_invnt_temp') }}
),
final as 
(
    SELECT 
        SRC.pos_date AS invnt_dt,
		NULL AS vend_cd,
		NULL AS vend_nm,
		SRC.civilian_product_code AS vend_prod_cd,
		SRC.product_name AS vend_prod_nm,
		SRC.ean_code AS ean_num,
		SRC.store_code AS str_cd,
		SRC.store_name_chinese AS str_nm,
		SRC.Stock_inventory_qty AS invnt_qty,
		SRC.Stock_inventory_amt AS invnt_amt,
		SRC.unit_price AS unit_prc_amt,
		NULL AS per_box_qty,
		NULL AS cust_invnt_qty,
		NULL AS box_invnt_qty,
		NULL AS wk_hold_sls,
		NULL AS wk_hold,
		NULL AS fst_recv_dt,
		NULL AS dsct_dt,
		SRC.DC AS DC,
		NULL AS stk_cls,
		'TWD' AS crncy_cd,
		'PX 全聯' AS src_sys_cd,
		'TW' AS ctry_cd,
		TGT.CRT_DTTM AS TGT_CRT_DTTM,
		SRC.UPD_DTTM,
		CASE 
			WHEN TGT.CRT_DTTM IS NULL
				THEN 'I'
			ELSE 'U'
			END AS CHNG_FLG 
    FROM sdl_tw_pos_px_civila SRC 
    LEFT OUTER JOIN 
    (
		SELECT invnt_dt,
			vend_prod_cd,
			str_cd,
			src_sys_cd,
			ctry_cd,
			CRT_DTTM
		FROM itg_pos_invnt
		) TGT ON SRC.pos_date = TGT.invnt_dt
		AND SRC.civilian_product_code = TGT.vend_prod_cd
		AND SRC.store_code = TGT.str_cd
		AND 'PX 全聯' = TGT.src_sys_cd
		AND 'TW' = TGT.ctry_cd
)
select * from final