with sdl_tw_pos_poya as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_poya') }}
),
itg_pos_invnt as (
    select * from {{ source('ntaitg_integration', 'itg_pos_invnt_temp') }}
),
final as 
(
    SELECT 
        SRC.start_date AS invnt_dt,
		SRC.vendor_code AS vend_cd,
		NULL AS vend_nm,
		SRC.customer_product_code AS vend_prod_cd,
		SRC.Product_description AS vend_prod_nm,
		SRC.ean_code AS ean_num,
		NULL AS str_cd,
		NULL AS str_nm,
		SRC.inventory AS invnt_qty,
		NULL AS invnt_amt,
		NULL AS unit_prc_amt,
		NULL AS per_box_qty,
		NULL AS cust_invnt_qty,
		NULL AS box_invnt_qty,
		NULL AS wk_hold_sls,
		NULL AS wk_hold,
		NULL AS fst_recv_dt,
		NULL AS dsct_dt,
		NULL AS DC,
		NULL AS stk_cls,
		'TWD' AS crncy_cd,
		'Poya 寶雅' AS src_sys_cd,
		'TW' AS ctry_cd,
		TGT.CRT_DTTM AS TGT_CRT_DTTM,
		SRC.UPD_DTTM,
		CASE 
			WHEN TGT.CRT_DTTM IS NULL
				THEN 'I'
			ELSE 'U'
			END AS CHNG_FLG
    FROM sdl_tw_pos_poya SRC 
    LEFT OUTER JOIN 
    (
		SELECT invnt_dt,
			vend_prod_cd,
			src_sys_cd,
			ctry_cd,
			CRT_DTTM
		FROM itg_pos_invnt
	) TGT ON SRC.start_date = TGT.invnt_dt
		AND SRC.customer_product_code = TGT.vend_prod_cd
		AND 'Poya 寶雅' = TGT.src_sys_cd
		AND 'TW' = TGT.ctry_cd
)
select * from final