with source as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_cosmed') }}
),
transformed as(
    select SRC.strt_dt AS pos_dt,
		NULL AS vend_cd,
		NULL AS vend_nm,
		NULL AS prod_nm,
		SRC.product_code AS vend_prod_cd,
		SRC.product_name AS vend_prod_nm,
		NULL AS brnd_nm,
		NULL AS ean_num,
		NULL AS str_cd,
		NULL AS str_nm,
		sales_quantity AS sls_qty,
		NULL AS sls_amt,
		NULL AS unit_prc_amt,
		NULL AS sls_excl_vat_amt,
		NULL AS stk_rtrn_amt,
		NULL AS stk_recv_amt,
		NULL AS avg_sell_qty,
		NULL AS cum_ship_qty,
		NULL AS cum_rtrn_qty,
		NULL AS web_ordr_takn_qty,
		NULL AS web_ordr_acpt_qty,
		NULL AS dc_invnt_qty,
		NULL AS invnt_qty,
		NULL AS invnt_amt,
		NULL AS invnt_dt,
		NULL AS serial_num,
		NULL AS prod_delv_type,
		NULL AS prod_type,
		NULL AS dept_cd,
		NULL AS dept_nm,
		NULL AS spec_1_desc,
		NULL AS spec_2_desc,
		NULL AS cat_big,
		NULL AS cat_mid,
		NULL AS cat_small,
		NULL AS dc_prod_cd,
		NULL AS cust_dtls,
		NULL AS dist_cd,
		'TWD' AS crncy_cd,
		NULL AS src_txn_sts,
		NULL AS src_seq_num,
		'Cosmed 康是美' AS src_sys_cd,
		'TW' AS ctry_cd,
		NULL AS UPD_DTTM
		FROM
		----NA_SDL.source SRC
		(
		SELECT product_code,
			product_name,
			wk1_strt_dt AS strt_dt,
			wk1_qty AS sales_quantity
		FROM source
		
		UNION ALL
		
		SELECT product_code,
			product_name,
			wk2_strt_dt AS strt_dt,
			wk2_qty AS sales_quantity
		FROM source
		
		UNION ALL
		
		SELECT product_code,
			product_name,
			wk3_strt_dt AS strt_dt,
			wk3_qty AS sales_quantity
		FROM source
		
		UNION ALL
		
		SELECT product_code,
			product_name,
			wk4_strt_dt AS strt_dt,
			wk4_qty AS sales_quantity
		FROM source
		) SRC 
)
select * from transformed