{{
  config(
    materialized='view'
  )
}}
with edw_retailer_calendar_dim
as (
    select *
    from ({{ ref('indedw_integration__edw_retailer_calendar_dim') }})
    ),
edw_product_dim
as (
    select *
    from ({{ ref('indedw_integration__edw_product_dim') }})
    ),
edw_customer_dim
as (
    select *
    from ({{ ref('indedw_integration__edw_customer_dim') }})
    ),
itg_query_parameters
as (
    select *
    from {{ source('aspitg_integration', 'itg_query_parameters') }}
    ),
itg_tblpf_sit
as (
    select *
    from ({{ ref('inditg_integration__itg_tblpf_sit') }})
    ),
itg_tblpf_clstkm
as (
    select *
    from ({{ ref('inditg_integration__itg_tblpf_clstkm') }})
    ),
itg_pf_retail_mth_ds
as (
    select *
    from ({{ ref('inditg_integration__itg_pf_retail_mth_ds') }})
    ),
tblpf_prisalesm
as (
    select *
    from ({{ ref('inditg_integration__tblpf_prisalesm') }})
    ),
itg_plant
as (
    select *
    from ({{ ref('inditg_integration__itg_plant') }})
    ),
itg_pf_retail_mth_ds_136843_yearend_2021
as (
    select *
    from {{ source('inditg_integration', 'itg_pf_retail_mth_ds_136843_yearend_2021') }}
    ),
itg_pf_retail_mth_ds_132641_jul_yearend_2021
as (
    select *
    from {{ source('inditg_integration', 'itg_pf_retail_mth_ds_132641_jul_yearend_2021') }}
    ),
itg_pf_retail_mth_ds_132641_aug_yearend_2021
as (
    select *
    from {{ source('inditg_integration', 'itg_pf_retail_mth_ds_132641_aug_yearend_2021') }}
    ),
itg_rstockdiscrepancy_withproduct
as (
    select *
    from ({{ ref('inditg_integration__itg_rstockdiscrepancy_withproduct') }})
    ),
    itg_tblpf_idt as (
select * from ({{ ref('inditg_integration__itg_tblpf_idt') }})
),
union_1
AS (
    SELECT cal_dim.mon AS "month",
        cal_dim.yr AS "year",
        prisal.distcode AS customer_code,
        prisal.prdcode AS product_code,
        COALESCE(prisal.invoice_qty, ((0)::NUMERIC)::NUMERIC(18, 0)) AS invoice_qty,
        COALESCE(prisal.invoice_val, ((0)::NUMERIC)::NUMERIC(18, 0)) AS invoice_val,
        COALESCE(prisal.zf2d_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zf2d_value,
        COALESCE(prisal.zl2d_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zl2d_value,
        COALESCE(prisal.zg2d_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zg2d_value,
        COALESCE(prisal.zc2d_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zc2d_value,
        COALESCE(prisal.zf2e_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zf2e_value,
        COALESCE(prisal.zl3d_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zl3d_value,
        COALESCE(prisal.zg3d_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zg3d_value,
        COALESCE(prisal.zc3d_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zc3d_value,
        COALESCE(prisal.zl22_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zl22_value,
        COALESCE(prisal.zg22_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zg22_value,
        COALESCE(prisal.zc22_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zc22_value,
        COALESCE(prisal.s1_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS s1_value,
        COALESCE(prisal.s2_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS s2_value,
        COALESCE(prisal.zrsm_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zrsm_value,
        COALESCE(prisal.zsmd_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS zsmd_value,
        0 AS sec_prd_qty,
        0 AS sec_ptr_value,
        0 AS sec_prd_nr_value,
        0 AS sec_lp_value,
        0 AS sec_prd_qty_ret,
        0 AS sec_ptr_value_ret,
        0 AS sec_prd_nr_value_ret,
        0 AS sec_lp_value_ret,
        0 AS sec_tr_in_qty,
        0 AS sec_tr_out_qty,
        0 AS sec_tr_in_val,
        0 AS sec_tr_out_val,
        0 AS sec_non_wave_open_qty,
        0 AS sec_gross_amt,
        0 AS sec_gross_amt_ret,
        0 AS sec_late_prd_qty,
        0 AS sec_late_ptr_value,
        0 AS sec_late_prd_nr_value,
        0 AS sec_late_lp_value,
        0 AS sec_late_prd_qty_ret,
        0 AS sec_late_ptr_value_ret,
        0 AS sec_late_prd_nr_value_ret,
        0 AS sec_late_lp_value_ret,
        0 AS sec_late_tr_in_qty,
        0 AS sec_late_tr_out_qty,
        0 AS sec_late_tr_in_val,
        0 AS sec_late_tr_out_val,
        0 AS sec_late_non_wave_open_qty,
        0 AS sec_late_gross_amt,
        0 AS sec_late_gross_amt_ret,
        0 AS dbrestore_last_month_sec_late_prd_qty,
        0 AS dbrestore_last_month_sec_late_ptr_value,
        0 AS dbrestore_last_month_sec_late_prd_nr_value,
        0 AS dbrestore_last_month_sec_late_prd_qty_ret,
        0 AS dbrestore_last_month_sec_late_ptr_value_ret,
        0 AS dbrestore_last_month_sec_late_prd_nr_value_ret,
        0 AS dbrestore_last_month_sec_late_gross_amt,
        0 AS dbrestore_last_month_sec_late_gross_amt_ret,
        0 AS dbrestore_current_sec_late_prd_qty,
        0 AS dbrestore_current_sec_late_ptr_value,
        0 AS dbrestore_current_sec_late_prd_nr_value,
        0 AS dbrestore_current_sec_late_prd_qty_ret,
        0 AS dbrestore_current_sec_late_ptr_value_ret,
        0 AS dbrestore_current_sec_late_prd_nr_value_ret,
        0 AS dbrestore_current_sec_late_gross_amt,
        0 AS dbrestore_current_sec_late_gross_amt_ret,
        0 AS actual_sec_late_prd_qty,
        0 AS actual_sec_late_prd_qty_ret,
        0 AS actual_sec_late_ptr_value,
        0 AS actual_sec_late_ptr_value_ret,
        0 AS actual_sec_late_prd_nr_value,
        0 AS actual_sec_late_prd_nr_value_ret,
        0 AS actual_sec_late_gross_amt,
        0 AS actual_sec_late_gross_amt_ret,
        0 AS retailing_quantity,
        0 AS retailing_ptr_value,
        0 AS retailing_nr_value,
        0 AS retailing_gross_value,
        0 AS opnstk_value_nr,
        0 AS opnstk_qty,
        0 AS opnstk_nr,
        0 AS salable_opnstk,
        0 AS unsalable_opnstk,
        0 AS offer_opnstk,
        0 AS cl_stck_value_nr,
        0 AS cl_stck_qty,
        0 AS cl_stck_nr,
        0 AS salable_cl_stk,
        0 AS unsalable_cl_stk,
        0 AS offer_cl_stk,
        0 AS sit_value,
        0 AS opening_sit_value,
        0 AS salable_idt,
        0 AS salable_stkadj,
        0 AS unsalable_idt,
        0 AS unsalable_stkadj,
        0 AS offer_idt,
        0 AS offer_stkadj,
        0 AS purchasereturn
    FROM (
        (
            SELECT DISTINCT (
                    "substring" (
                        ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                        5,
                        2
                        )
                    )::INTEGER AS mon,
                (
                    "substring" (
                        ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                        1,
                        4
                        )
                    )::INTEGER AS yr,
                (
                    "substring" (
                        (
                            (
                                to_char(add_months((
                                            to_date((
                                                    "substring" (
                                                        ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                                        1,
                                                        4
                                                        ) || "substring" (
                                                        ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                                        5,
                                                        2
                                                        )
                                                    ), ('YYYYMM'::CHARACTER VARYING)::TEXT)
                                            )::TIMESTAMP without TIME zone, (- (1)::BIGINT)))
                                )::CHARACTER VARYING
                            )::TEXT,
                        5,
                        2
                        )
                    )::INTEGER AS prev_mon,
                (
                    "substring" (
                        (
                            (
                                to_char(add_months((
                                            to_date((
                                                    "substring" (
                                                        ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                                        1,
                                                        4
                                                        ) || "substring" (
                                                        ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                                        5,
                                                        2
                                                        )
                                                    ), ('YYYYMM'::CHARACTER VARYING)::TEXT)
                                            )::TIMESTAMP without TIME zone, (- (1)::BIGINT)))
                                )::CHARACTER VARYING
                            )::TEXT,
                        1,
                        4
                        )
                    )::INTEGER AS prev_yr
            FROM edw_retailer_calendar_dim
            ) cal_dim JOIN (
            SELECT tblpf_prisalesm.mon,
                tblpf_prisalesm.yr,
                tblpf_prisalesm.distcode,
                tblpf_prisalesm.prdcode,
                sum(tblpf_prisalesm.qty) AS invoice_qty,
                sum(tblpf_prisalesm.value) AS invoice_val,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZF2D'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zf2d_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZL2D'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zl2d_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZG2D'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zg2d_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZC2D'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zc2d_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZF2E'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zf2e_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZL3D'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zl3d_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZG3D'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zg3d_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZC3D'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zc3d_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZL22'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zl22_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZG22'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zg22_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZC22'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zc22_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('S1'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS s1_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('S2'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS s2_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZRSM'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zrsm_value,
                sum(CASE 
                        WHEN (upper((tblpf_prisalesm.billtype)::TEXT) = ('ZSMD'::CHARACTER VARYING)::TEXT)
                            THEN tblpf_prisalesm.value
                        ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                        END) AS zsmd_value
            FROM tblpf_prisalesm
            GROUP BY tblpf_prisalesm.mon,
                tblpf_prisalesm.yr,
                tblpf_prisalesm.distcode,
                tblpf_prisalesm.prdcode
            ) prisal ON (
                (
                    (cal_dim.mon = prisal.mon)
                    AND (cal_dim.yr = prisal.yr)
                    )
                )
        )

    UNION ALL

    SELECT itg_pf_retail_mth_ds.month,
        itg_pf_retail_mth_ds.year,
        itg_pf_retail_mth_ds.customer_code,
        itg_pf_retail_mth_ds.product_code,
        0 AS invoice_qty,
        0 AS invoice_val,
        0 AS zf2d_value,
        0 AS zl2d_value,
        0 AS zg2d_value,
        0 AS zc2d_value,
        0 AS zf2e_value,
        0 AS zl3d_value,
        0 AS zg3d_value,
        0 AS zc3d_value,
        0 AS zl22_value,
        0 AS zg22_value,
        0 AS zc22_value,
        0 AS s1_value,
        0 AS s2_value,
        0 AS zrsm_value,
        0 AS zsmd_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_lp_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_lp_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_lp_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_lp_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_in_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_in_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_out_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_out_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_in_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_in_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_out_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_out_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_non_wave_open_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_non_wave_open_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_lp_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_lp_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_lp_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_lp_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_in_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_in_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_out_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_out_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_in_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_in_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_out_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_out_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_non_wave_open_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_non_wave_open_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_gross_amt_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_qty,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_qty_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_ptr_value,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_ptr_value_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_nr_value,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_nr_value_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_gross_amt,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_gross_amt_ret,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_quantity,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_ptr_value,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_nr_value,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_gross_value,
        0 AS opnstk_value_nr,
        0 AS opnstk_qty,
        0 AS opnstk_nr,
        0 AS salable_opnstk,
        0 AS unsalable_opnstk,
        0 AS offer_opnstk,
        0 AS cl_stck_value_nr,
        0 AS cl_stck_qty,
        0 AS cl_stck_nr,
        0 AS salable_cl_stk,
        0 AS unsalable_cl_stk,
        0 AS offer_cl_stk,
        0 AS sit_value,
        0 AS opening_sit_value,
        0 AS salable_idt,
        0 AS salable_stkadj,
        0 AS unsalable_idt,
        0 AS unsalable_stkadj,
        0 AS offer_idt,
        0 AS offer_stkadj,
        0 AS purchasereturn
    FROM itg_pf_retail_mth_ds
    GROUP BY itg_pf_retail_mth_ds.month,
        itg_pf_retail_mth_ds.year,
        itg_pf_retail_mth_ds.customer_code,
        itg_pf_retail_mth_ds.product_code
    ),
union_2
AS (
    SELECT 12,
        itg_pf_retail_mth_ds.year,
        itg_pf_retail_mth_ds.customer_code,
        itg_pf_retail_mth_ds.product_code,
        0 AS invoice_qty,
        0 AS invoice_val,
        0 AS zf2d_value,
        0 AS zl2d_value,
        0 AS zg2d_value,
        0 AS zc2d_value,
        0 AS zf2e_value,
        0 AS zl3d_value,
        0 AS zg3d_value,
        0 AS zc3d_value,
        0 AS zl22_value,
        0 AS zg22_value,
        0 AS zc22_value,
        0 AS s1_value,
        0 AS s2_value,
        0 AS zrsm_value,
        0 AS zsmd_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_lp_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_lp_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_lp_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_lp_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_in_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_in_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_out_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_out_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_in_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_in_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_out_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_out_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_non_wave_open_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_non_wave_open_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_lp_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_lp_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_lp_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_lp_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_in_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_in_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_out_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_out_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_in_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_in_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_out_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_out_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_non_wave_open_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_non_wave_open_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_gross_amt_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_qty,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_qty_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_ptr_value,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_ptr_value_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_nr_value,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_nr_value_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_gross_amt,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_gross_amt_ret,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_quantity,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_ptr_value,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_nr_value,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_gross_value,
        0 AS opnstk_value_nr,
        0 AS opnstk_qty,
        0 AS opnstk_nr,
        0 AS salable_opnstk,
        0 AS unsalable_opnstk,
        0 AS offer_opnstk,
        0 AS cl_stck_value_nr,
        0 AS cl_stck_qty,
        0 AS cl_stck_nr,
        0 AS salable_cl_stk,
        0 AS unsalable_cl_stk,
        0 AS offer_cl_stk,
        0 AS sit_value,
        0 AS opening_sit_value,
        0 AS salable_idt,
        0 AS salable_stkadj,
        0 AS unsalable_idt,
        0 AS unsalable_stkadj,
        0 AS offer_idt,
        0 AS offer_stkadj,
        0 AS purchasereturn
    FROM itg_pf_retail_mth_ds_136843_yearend_2021 itg_pf_retail_mth_ds
    GROUP BY itg_pf_retail_mth_ds.year,
        itg_pf_retail_mth_ds.customer_code,
        itg_pf_retail_mth_ds.product_code
    ),
union_3
AS (
    SELECT 12,
        itg_pf_retail_mth_ds.year,
        itg_pf_retail_mth_ds.customer_code,
        itg_pf_retail_mth_ds.product_code,
        0 AS invoice_qty,
        0 AS invoice_val,
        0 AS zf2d_value,
        0 AS zl2d_value,
        0 AS zg2d_value,
        0 AS zc2d_value,
        0 AS zf2e_value,
        0 AS zl3d_value,
        0 AS zg3d_value,
        0 AS zc3d_value,
        0 AS zl22_value,
        0 AS zg22_value,
        0 AS zc22_value,
        0 AS s1_value,
        0 AS s2_value,
        0 AS zrsm_value,
        0 AS zsmd_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_lp_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_lp_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_lp_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_lp_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_in_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_in_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_out_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_out_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_in_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_in_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_out_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_out_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_non_wave_open_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_non_wave_open_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_lp_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_lp_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_lp_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_lp_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_in_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_in_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_out_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_out_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_in_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_in_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_out_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_out_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_non_wave_open_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_non_wave_open_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_gross_amt_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_qty,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_qty_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_ptr_value,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_ptr_value_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_nr_value,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_nr_value_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_gross_amt,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_gross_amt_ret,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_quantity,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_ptr_value,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_nr_value,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_gross_value,
        0 AS opnstk_value_nr,
        0 AS opnstk_qty,
        0 AS opnstk_nr,
        0 AS salable_opnstk,
        0 AS unsalable_opnstk,
        0 AS offer_opnstk,
        0 AS cl_stck_value_nr,
        0 AS cl_stck_qty,
        0 AS cl_stck_nr,
        0 AS salable_cl_stk,
        0 AS unsalable_cl_stk,
        0 AS offer_cl_stk,
        0 AS sit_value,
        0 AS opening_sit_value,
        0 AS salable_idt,
        0 AS salable_stkadj,
        0 AS unsalable_idt,
        0 AS unsalable_stkadj,
        0 AS offer_idt,
        0 AS offer_stkadj,
        0 AS purchasereturn
    FROM itg_pf_retail_mth_ds_132641_jul_yearend_2021 itg_pf_retail_mth_ds
    GROUP BY itg_pf_retail_mth_ds.year,
        itg_pf_retail_mth_ds.customer_code,
        itg_pf_retail_mth_ds.product_code
    ),
union_4
AS (
    SELECT 12,
        itg_pf_retail_mth_ds.year,
        itg_pf_retail_mth_ds.customer_code,
        itg_pf_retail_mth_ds.product_code,
        0 AS invoice_qty,
        0 AS invoice_val,
        0 AS zf2d_value,
        0 AS zl2d_value,
        0 AS zg2d_value,
        0 AS zc2d_value,
        0 AS zf2e_value,
        0 AS zl3d_value,
        0 AS zg3d_value,
        0 AS zc3d_value,
        0 AS zl22_value,
        0 AS zg22_value,
        0 AS zc22_value,
        0 AS s1_value,
        0 AS s2_value,
        0 AS zrsm_value,
        0 AS zsmd_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_lp_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_lp_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_lp_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_lp_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_in_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_in_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_out_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_out_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_in_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_in_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_tr_out_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_out_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_non_wave_open_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_non_wave_open_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_lp_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_lp_value,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_lp_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_lp_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_in_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_in_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_out_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_out_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_in_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_in_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_tr_out_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_out_val,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_non_wave_open_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_non_wave_open_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_gross_amt_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_qty,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_ptr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_nr_value,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_qty_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_ptr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_nr_value_ret,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_gross_amt,
        sum(COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_gross_amt_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_qty,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_qty_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_qty_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_ptr_value,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_ptr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_ptr_value_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_nr_value,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_prd_nr_value_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_prd_nr_value_ret,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_gross_amt,
        sum(((COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_prev, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.dbrestore_sec_late_gross_amt_ret_current, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS actual_sec_late_gross_amt_ret,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_quantity,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_ptr_value,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_nr_value,
        sum((((COALESCE(itg_pf_retail_mth_ds.sec_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0)) + COALESCE(itg_pf_retail_mth_ds.sec_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) + COALESCE(itg_pf_retail_mth_ds.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0)))) AS retailing_gross_value,
        0 AS opnstk_value_nr,
        0 AS opnstk_qty,
        0 AS opnstk_nr,
        0 AS salable_opnstk,
        0 AS unsalable_opnstk,
        0 AS offer_opnstk,
        0 AS cl_stck_value_nr,
        0 AS cl_stck_qty,
        0 AS cl_stck_nr,
        0 AS salable_cl_stk,
        0 AS unsalable_cl_stk,
        0 AS offer_cl_stk,
        0 AS sit_value,
        0 AS opening_sit_value,
        0 AS salable_idt,
        0 AS salable_stkadj,
        0 AS unsalable_idt,
        0 AS unsalable_stkadj,
        0 AS offer_idt,
        0 AS offer_stkadj,
        0 AS purchasereturn
    FROM itg_pf_retail_mth_ds_132641_aug_yearend_2021 itg_pf_retail_mth_ds
    GROUP BY itg_pf_retail_mth_ds.year,
        itg_pf_retail_mth_ds.customer_code,
        itg_pf_retail_mth_ds.product_code
    ),
union_5
AS (
    SELECT clstkm_kpi.mon AS "month",
        clstkm_kpi.yr AS "year",
        clstkm_kpi.distcode AS customer_code,
        clstkm_kpi.prdcode AS product_code,
        0 AS invoice_qty,
        0 AS invoice_val,
        0 AS zf2d_value,
        0 AS zl2d_value,
        0 AS zg2d_value,
        0 AS zc2d_value,
        0 AS zf2e_value,
        0 AS zl3d_value,
        0 AS zg3d_value,
        0 AS zc3d_value,
        0 AS zl22_value,
        0 AS zg22_value,
        0 AS zc22_value,
        0 AS s1_value,
        0 AS s2_value,
        0 AS zrsm_value,
        0 AS zsmd_value,
        0 AS sec_prd_qty,
        0 AS sec_ptr_value,
        0 AS sec_prd_nr_value,
        0 AS sec_lp_value,
        0 AS sec_prd_qty_ret,
        0 AS sec_ptr_value_ret,
        0 AS sec_prd_nr_value_ret,
        0 AS sec_lp_value_ret,
        0 AS sec_tr_in_qty,
        0 AS sec_tr_out_qty,
        0 AS sec_tr_in_val,
        0 AS sec_tr_out_val,
        0 AS sec_non_wave_open_qty,
        0 AS sec_gross_amt,
        0 AS sec_gross_amt_ret,
        0 AS sec_late_prd_qty,
        0 AS sec_late_ptr_value,
        0 AS sec_late_prd_nr_value,
        0 AS sec_late_lp_value,
        0 AS sec_late_prd_qty_ret,
        0 AS sec_late_ptr_value_ret,
        0 AS sec_late_prd_nr_value_ret,
        0 AS sec_late_lp_value_ret,
        0 AS sec_late_tr_in_qty,
        0 AS sec_late_tr_out_qty,
        0 AS sec_late_tr_in_val,
        0 AS sec_late_tr_out_val,
        0 AS sec_late_non_wave_open_qty,
        0 AS sec_late_gross_amt,
        0 AS sec_late_gross_amt_ret,
        0 AS dbrestore_last_month_sec_late_prd_qty,
        0 AS dbrestore_last_month_sec_late_ptr_value,
        0 AS dbrestore_last_month_sec_late_prd_nr_value,
        0 AS dbrestore_last_month_sec_late_prd_qty_ret,
        0 AS dbrestore_last_month_sec_late_ptr_value_ret,
        0 AS dbrestore_last_month_sec_late_prd_nr_value_ret,
        0 AS dbrestore_last_month_sec_late_gross_amt,
        0 AS dbrestore_last_month_sec_late_gross_amt_ret,
        0 AS dbrestore_current_sec_late_prd_qty,
        0 AS dbrestore_current_sec_late_ptr_value,
        0 AS dbrestore_current_sec_late_prd_nr_value,
        0 AS dbrestore_current_sec_late_prd_qty_ret,
        0 AS dbrestore_current_sec_late_ptr_value_ret,
        0 AS dbrestore_current_sec_late_prd_nr_value_ret,
        0 AS dbrestore_current_sec_late_gross_amt,
        0 AS dbrestore_current_sec_late_gross_amt_ret,
        0 AS actual_sec_late_prd_qty,
        0 AS actual_sec_late_prd_qty_ret,
        0 AS actual_sec_late_ptr_value,
        0 AS actual_sec_late_ptr_value_ret,
        0 AS actual_sec_late_prd_nr_value,
        0 AS actual_sec_late_prd_nr_value_ret,
        0 AS actual_sec_late_gross_amt,
        0 AS actual_sec_late_gross_amt_ret,
        0 AS retailing_quantity,
        0 AS retailing_ptr_value,
        0 AS retailing_nr_value,
        0 AS retailing_gross_value,
        COALESCE(clstkm_kpi.opnstk_value_nr, ((0)::NUMERIC)::NUMERIC(18, 0)) AS opnstk_value_nr,
        COALESCE(clstkm_kpi.opnstk_qty, ((0)::NUMERIC)::NUMERIC(18, 0)) AS opnstk_qty,
        COALESCE(clstkm_kpi.opnstk_nr, ((0)::NUMERIC)::NUMERIC(18, 0)) AS opnstk_nr,
        COALESCE(clstkm_kpi.salable_opnstk, ((0)::NUMERIC)::NUMERIC(18, 0)) AS salable_opnstk,
        COALESCE(clstkm_kpi.unsalable_opnstk, ((0)::NUMERIC)::NUMERIC(18, 0)) AS unsalable_opnstk,
        COALESCE(clstkm_kpi.offer_opnstk, ((0)::NUMERIC)::NUMERIC(18, 0)) AS offer_opnstk,
        COALESCE(clstkm_kpi.cl_stck_value_nr, ((0)::NUMERIC)::NUMERIC(18, 0)) AS cl_stck_value_nr,
        COALESCE(clstkm_kpi.cl_stck_qty, ((0)::NUMERIC)::NUMERIC(18, 0)) AS cl_stck_qty,
        COALESCE(clstkm_kpi.cl_stck_nr, ((0)::NUMERIC)::NUMERIC(18, 0)) AS cl_stck_nr,
        COALESCE(clstkm_kpi.salable_cl_stk, ((0)::NUMERIC)::NUMERIC(18, 0)) AS salable_cl_stk,
        COALESCE(clstkm_kpi.unsalable_cl_stk, ((0)::NUMERIC)::NUMERIC(18, 0)) AS unsalable_cl_stk,
        COALESCE(clstkm_kpi.offer_cl_stk, ((0)::NUMERIC)::NUMERIC(18, 0)) AS offer_cl_stk,
        0 AS sit_value,
        0 AS opening_sit_value,
        0 AS salable_idt,
        0 AS salable_stkadj,
        0 AS unsalable_idt,
        0 AS unsalable_stkadj,
        0 AS offer_idt,
        0 AS offer_stkadj,
        0 AS purchasereturn
    FROM (
        SELECT CASE 
    WHEN cal_dim.mon IS NULL THEN 
        TO_NUMBER(SUBSTR(TO_CHAR(DATE_TRUNC('MONTH', DATEADD(MONTH, 1, TO_DATE(opnstkm.yr || LPAD(opnstkm.mon::TEXT, 2, '0'), 'YYYYMM'))), 'YYYYMM'), 5, 2))
    ELSE 
        cal_dim.mon 
END AS mon,
            CASE 
    WHEN cal_dim.yr IS NULL THEN 
        TO_NUMBER(SUBSTR(TO_CHAR(DATE_TRUNC('MONTH', DATEADD(MONTH, 1, TO_DATE(opnstkm.yr || LPAD(opnstkm.mon::TEXT, 2, '0'), 'YYYYMM'))), 'YYYYMM'), 1, 4))
    ELSE 
        cal_dim.yr 
END AS yr,
            CASE 
                WHEN (clstkm.distcode IS NULL)
                    THEN opnstkm.distcode
                ELSE clstkm.distcode
                END AS distcode,
            CASE 
                WHEN (clstkm.prdcode IS NULL)
                    THEN opnstkm.prdcode
                ELSE clstkm.prdcode
                END AS prdcode,
            opnstkm.opnstk_value_nr,
            opnstkm.opnstk_qty,
            opnstkm.opnstk_nr,
            clstkm.cl_stck_value_nr,
            clstkm.cl_stck_qty,
            clstkm.cl_stck_nr,
            opnstkm.salable_opnstk,
            clstkm.salable_cl_stk,
            opnstkm.unsalable_opnstk,
            clstkm.unsalable_cl_stk,
            opnstkm.offer_opnstk,
            clstkm.offer_cl_stk
        FROM (
            (
                (
                    SELECT DISTINCT (
                            "substring" (
                                ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                5,
                                2
                                )
                            )::INTEGER AS mon,
                        (
                            "substring" (
                                ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                1,
                                4
                                )
                            )::INTEGER AS yr,
                        TO_NUMBER(SUBSTR(
        TO_CHAR(DATE_TRUNC('month', ADD_MONTHS(TO_DATE(SUBSTR(CAST(edw_retailer_calendar_dim.mth_mm AS VARCHAR), 1, 4) || SUBSTR(CAST(edw_retailer_calendar_dim.mth_mm AS VARCHAR), 5, 2), 'YYYYMM'), -1)), 'YYYYMM'), 
        5, 
        2
    )) AS prev_mon,


                       TO_NUMBER(SUBSTR(
        TO_CHAR(DATE_TRUNC('month', ADD_MONTHS(TO_DATE(SUBSTR(CAST(edw_retailer_calendar_dim.mth_mm AS VARCHAR), 1, 4) || SUBSTR(CAST(edw_retailer_calendar_dim.mth_mm AS VARCHAR), 5, 2), 'YYYYMM'), -1)), 'YYYYMM'), 
        1, 
        4
    )) AS prev_yr
                    FROM edw_retailer_calendar_dim
                    ) cal_dim JOIN (
                    SELECT itg_tblpf_clstkm.distcode,
                        itg_tblpf_clstkm.prdcode,
                        itg_tblpf_clstkm.mon,
                        itg_tblpf_clstkm.yr,
                        sum(itg_tblpf_clstkm.salclsnrvalue) AS salable_cl_stk,
                        sum(itg_tblpf_clstkm.offerclsnrvalue) AS offer_cl_stk,
                        sum(itg_tblpf_clstkm.unsalclsnrvalue) AS unsalable_cl_stk,
                        sum(itg_tblpf_clstkm.clstckqty) AS cl_stck_qty,
                        sum(itg_tblpf_clstkm.nr) AS cl_stck_nr,
                        sum(itg_tblpf_clstkm.value) AS cl_stck_value_nr
                    FROM itg_tblpf_clstkm
                    GROUP BY itg_tblpf_clstkm.distcode,
                        itg_tblpf_clstkm.prdcode,
                        itg_tblpf_clstkm.mon,
                        itg_tblpf_clstkm.yr
                    ) clstkm ON (
                        (
                            (cal_dim.mon = clstkm.mon)
                            AND (cal_dim.yr = clstkm.yr)
                            )
                        )
                ) FULL JOIN (
                SELECT itg_tblpf_clstkm.distcode,
                    itg_tblpf_clstkm.prdcode,
                    itg_tblpf_clstkm.mon,
                    itg_tblpf_clstkm.yr,
                    sum(itg_tblpf_clstkm.salclsnrvalue) AS salable_opnstk,
                    sum(itg_tblpf_clstkm.unsalclsnrvalue) AS unsalable_opnstk,
                    sum(itg_tblpf_clstkm.offerclsnrvalue) AS offer_opnstk,
                    sum(itg_tblpf_clstkm.clstckqty) AS opnstk_qty,
                    sum(itg_tblpf_clstkm.nr) AS opnstk_nr,
                    sum(itg_tblpf_clstkm.value) AS opnstk_value_nr
                FROM itg_tblpf_clstkm
                GROUP BY itg_tblpf_clstkm.distcode,
                    itg_tblpf_clstkm.prdcode,
                    itg_tblpf_clstkm.mon,
                    itg_tblpf_clstkm.yr
                ) opnstkm ON (
                    (
                        (
                            (
                                ((clstkm.distcode)::TEXT = (opnstkm.distcode)::TEXT)
                                AND ((clstkm.prdcode)::TEXT = (opnstkm.prdcode)::TEXT)
                                )
                            AND (cal_dim.prev_mon = opnstkm.mon)
                            )
                        AND (cal_dim.prev_yr = opnstkm.yr)
                        )
                    )
            )
        ) clstkm_kpi
    ),
union_6
AS (
    SELECT sit_kpi.mon AS "month",
        sit_kpi.yr AS "year",
        sit_kpi.distcode AS customer_code,
        sit_kpi.prdcode AS product_code,
        0 AS invoice_qty,
        0 AS invoice_val,
        0 AS zf2d_value,
        0 AS zl2d_value,
        0 AS zg2d_value,
        0 AS zc2d_value,
        0 AS zf2e_value,
        0 AS zl3d_value,
        0 AS zg3d_value,
        0 AS zc3d_value,
        0 AS zl22_value,
        0 AS zg22_value,
        0 AS zc22_value,
        0 AS s1_value,
        0 AS s2_value,
        0 AS zrsm_value,
        0 AS zsmd_value,
        0 AS sec_prd_qty,
        0 AS sec_ptr_value,
        0 AS sec_prd_nr_value,
        0 AS sec_lp_value,
        0 AS sec_prd_qty_ret,
        0 AS sec_ptr_value_ret,
        0 AS sec_prd_nr_value_ret,
        0 AS sec_lp_value_ret,
        0 AS sec_tr_in_qty,
        0 AS sec_tr_out_qty,
        0 AS sec_tr_in_val,
        0 AS sec_tr_out_val,
        0 AS sec_non_wave_open_qty,
        0 AS sec_gross_amt,
        0 AS sec_gross_amt_ret,
        0 AS sec_late_prd_qty,
        0 AS sec_late_ptr_value,
        0 AS sec_late_prd_nr_value,
        0 AS sec_late_lp_value,
        0 AS sec_late_prd_qty_ret,
        0 AS sec_late_ptr_value_ret,
        0 AS sec_late_prd_nr_value_ret,
        0 AS sec_late_lp_value_ret,
        0 AS sec_late_tr_in_qty,
        0 AS sec_late_tr_out_qty,
        0 AS sec_late_tr_in_val,
        0 AS sec_late_tr_out_val,
        0 AS sec_late_non_wave_open_qty,
        0 AS sec_late_gross_amt,
        0 AS sec_late_gross_amt_ret,
        0 AS dbrestore_last_month_sec_late_prd_qty,
        0 AS dbrestore_last_month_sec_late_ptr_value,
        0 AS dbrestore_last_month_sec_late_prd_nr_value,
        0 AS dbrestore_last_month_sec_late_prd_qty_ret,
        0 AS dbrestore_last_month_sec_late_ptr_value_ret,
        0 AS dbrestore_last_month_sec_late_prd_nr_value_ret,
        0 AS dbrestore_last_month_sec_late_gross_amt,
        0 AS dbrestore_last_month_sec_late_gross_amt_ret,
        0 AS dbrestore_current_sec_late_prd_qty,
        0 AS dbrestore_current_sec_late_ptr_value,
        0 AS dbrestore_current_sec_late_prd_nr_value,
        0 AS dbrestore_current_sec_late_prd_qty_ret,
        0 AS dbrestore_current_sec_late_ptr_value_ret,
        0 AS dbrestore_current_sec_late_prd_nr_value_ret,
        0 AS dbrestore_current_sec_late_gross_amt,
        0 AS dbrestore_current_sec_late_gross_amt_ret,
        0 AS actual_sec_late_prd_qty,
        0 AS actual_sec_late_prd_qty_ret,
        0 AS actual_sec_late_ptr_value,
        0 AS actual_sec_late_ptr_value_ret,
        0 AS actual_sec_late_prd_nr_value,
        0 AS actual_sec_late_prd_nr_value_ret,
        0 AS actual_sec_late_gross_amt,
        0 AS actual_sec_late_gross_amt_ret,
        0 AS retailing_quantity,
        0 AS retailing_ptr_value,
        0 AS retailing_nr_value,
        0 AS retailing_gross_value,
        0 AS opnstk_value_nr,
        0 AS opnstk_qty,
        0 AS opnstk_nr,
        0 AS salable_opnstk,
        0 AS unsalable_opnstk,
        0 AS offer_opnstk,
        0 AS cl_stck_value_nr,
        0 AS cl_stck_qty,
        0 AS cl_stck_nr,
        0 AS salable_cl_stk,
        0 AS unsalable_cl_stk,
        0 AS offer_cl_stk,
        COALESCE(sit_kpi.sit_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS sit_value,
        COALESCE(sit_kpi.opening_sit_value, ((0)::NUMERIC)::NUMERIC(18, 0)) AS opening_sit_value,
        0 AS salable_idt,
        0 AS salable_stkadj,
        0 AS unsalable_idt,
        0 AS unsalable_stkadj,
        0 AS offer_idt,
        0 AS offer_stkadj,
        0 AS purchasereturn
    FROM (
        SELECT CASE 
                WHEN (sit.distcode IS NULL)
                    THEN prevsit.distcode
                ELSE sit.distcode
                END AS distcode,
            CASE 
                WHEN (sit.prdcode IS NULL)
                    THEN prevsit.prdcode
                ELSE sit.prdcode
                END AS prdcode,
CASE 
    WHEN cal_dim.mon IS NULL THEN 
        TO_NUMBER(SUBSTR(
            TO_CHAR(DATE_TRUNC('month', ADD_MONTHS(TO_DATE(prevsit.year || LPAD(prevsit.month::TEXT, 2, '0'), 'YYYYMM'), 1)), 'YYYYMM'), 
            5, 
            2
        ))
    ELSE cal_dim.mon
END AS mon,

            CASE 
    WHEN cal_dim.yr IS NULL THEN 
        TO_NUMBER(SUBSTR(
            TO_CHAR(DATE_TRUNC('month', ADD_MONTHS(TO_DATE(prevsit.year || LPAD(prevsit.month::TEXT, 2, '0'), 'YYYYMM'), 1)), 'YYYYMM'), 
            1, 
            4
        ))
    ELSE cal_dim.yr
END AS yr,
            sit.sit_value,
            prevsit.opening_sit_value
        FROM (
            (
                (
                    SELECT DISTINCT (
                            "substring" (
                                ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                5,
                                2
                                )
                            )::INTEGER AS mon,
                        (
                            "substring" (
                                ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                1,
                                4
                                )
                            )::INTEGER AS yr,
                        TO_NUMBER(SUBSTR(
        TO_CHAR(DATE_TRUNC('month', ADD_MONTHS(TO_DATE(SUBSTR(CAST(edw_retailer_calendar_dim.mth_mm AS VARCHAR), 1, 4) || SUBSTR(CAST(edw_retailer_calendar_dim.mth_mm AS VARCHAR), 5, 2), 'YYYYMM'), -1)), 'YYYYMM'), 
        5, 
        2
    )) AS prev_mon,
                        TO_NUMBER(SUBSTR(
        TO_CHAR(DATE_TRUNC('month', ADD_MONTHS(TO_DATE(SUBSTR(CAST(edw_retailer_calendar_dim.mth_mm AS VARCHAR), 1, 4) || SUBSTR(CAST(edw_retailer_calendar_dim.mth_mm AS VARCHAR), 5, 2), 'YYYYMM'), -1)), 'YYYYMM'), 
        1, 
        4
    )) AS prev_yr
                    FROM edw_retailer_calendar_dim
                    ) cal_dim JOIN (
                    SELECT DISTINCT itg_tblpf_sit.distcode,
                        itg_tblpf_sit.prdcode,
                        itg_tblpf_sit.month,
                        itg_tblpf_sit.year,
                        sum(itg_tblpf_sit.sitvalue) OVER (
                            PARTITION BY itg_tblpf_sit.distcode,
                            itg_tblpf_sit.prdcode,
                            itg_tblpf_sit.month,
                            itg_tblpf_sit.year ORDER BY NULL ROWS BETWEEN UNBOUNDED PRECEDING
                                    AND UNBOUNDED FOLLOWING
                            ) AS sit_value
                    FROM itg_tblpf_sit
                    ) sit ON (
                        (
                            (((cal_dim.mon)::CHARACTER VARYING)::TEXT = (sit.month)::TEXT)
                            AND (((cal_dim.yr)::CHARACTER VARYING)::TEXT = (sit.year)::TEXT)
                            )
                        )
                ) FULL JOIN (
                SELECT DISTINCT itg_tblpf_sit.distcode,
                    itg_tblpf_sit.prdcode,
                    itg_tblpf_sit.month,
                    itg_tblpf_sit.year,
                    sum(itg_tblpf_sit.sitvalue) OVER (
                        PARTITION BY itg_tblpf_sit.distcode,
                        itg_tblpf_sit.prdcode,
                        itg_tblpf_sit.month,
                        itg_tblpf_sit.year ORDER BY NULL ROWS BETWEEN UNBOUNDED PRECEDING
                                AND UNBOUNDED FOLLOWING
                        ) AS opening_sit_value
                FROM itg_tblpf_sit
                ) prevsit ON (
                    (
                        (
                            (
                                ((sit.distcode)::TEXT = (prevsit.distcode)::TEXT)
                                AND ((sit.prdcode)::TEXT = (prevsit.prdcode)::TEXT)
                                )
                            AND (((cal_dim.prev_mon)::CHARACTER VARYING)::TEXT = (prevsit.month)::TEXT)
                            )
                        AND (((cal_dim.prev_yr)::CHARACTER VARYING)::TEXT = (prevsit.year)::TEXT)
                        )
                    )
            )
        ) sit_kpi
    ),
id1 as 
(
                    SELECT DISTINCT itg_tblpf_idt.distcode,
                        itg_tblpf_idt.prdcode,
                        itg_tblpf_idt.month ,
                        itg_tblpf_idt.year,
                        sum(CASE 
                                WHEN (
                                        (upper((itg_tblpf_idt.stkmgmttype)::TEXT) = ('IDT IN'::CHARACTER VARYING)::TEXT)
                                        AND (upper((itg_tblpf_idt.stocktype)::TEXT) = ('SALABLE'::CHARACTER VARYING)::TEXT)
                                        )
                                    THEN COALESCE((itg_tblpf_idt.baseqty * itg_tblpf_idt.nr), ((0)::NUMERIC)::NUMERIC(18, 0))
                                ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                                END) OVER (
                            PARTITION BY itg_tblpf_idt.distcode,
                            itg_tblpf_idt.prdcode,
                            itg_tblpf_idt.year,
                            itg_tblpf_idt.month  order by null ROWS BETWEEN UNBOUNDED PRECEDING
                                AND UNBOUNDED FOLLOWING
                            ) AS salable_idt_in,
                        sum(CASE 
                                WHEN (
                                        (upper((itg_tblpf_idt.stkmgmttype)::TEXT) = ('IDT IN'::CHARACTER VARYING)::TEXT)
                                        AND (upper((itg_tblpf_idt.stocktype)::TEXT) = ('UNSALABLE'::CHARACTER VARYING)::TEXT)
                                        )
                                    THEN COALESCE((itg_tblpf_idt.baseqty * itg_tblpf_idt.nr), ((0)::NUMERIC)::NUMERIC(18, 0))
                                ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                                END) OVER (
                            PARTITION BY itg_tblpf_idt.distcode,
                            itg_tblpf_idt.prdcode,
                            itg_tblpf_idt.year,
                            itg_tblpf_idt.month  order by null ROWS BETWEEN UNBOUNDED PRECEDING
                                AND UNBOUNDED FOLLOWING
                            ) AS unsalable_idt_in,
                        sum(CASE 
                                WHEN (
                                        (upper((itg_tblpf_idt.stkmgmttype)::TEXT) = ('IDT IN'::CHARACTER VARYING)::TEXT)
                                        AND (upper((itg_tblpf_idt.stocktype)::TEXT) = ('OFFER'::CHARACTER VARYING)::TEXT)
                                        )
                                    THEN COALESCE((itg_tblpf_idt.baseqty * itg_tblpf_idt.nr), ((0)::NUMERIC)::NUMERIC(18, 0))
                                ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                                END) OVER (
                            PARTITION BY itg_tblpf_idt.distcode,
                            itg_tblpf_idt.prdcode,
                            itg_tblpf_idt.year,
                            itg_tblpf_idt.month order by null  ROWS BETWEEN UNBOUNDED PRECEDING
                                AND UNBOUNDED FOLLOWING
                            ) AS offer_idt_in,
                        sum(CASE 
                                WHEN (
                                        (upper((itg_tblpf_idt.stkmgmttype)::TEXT) = ('IDT OUT'::CHARACTER VARYING)::TEXT)
                                        AND (upper((itg_tblpf_idt.stocktype)::TEXT) = ('SALABLE'::CHARACTER VARYING)::TEXT)
                                        )
                                    THEN COALESCE((itg_tblpf_idt.baseqty * itg_tblpf_idt.nr), ((0)::NUMERIC)::NUMERIC(18, 0))
                                ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                                END) OVER (
                            PARTITION BY itg_tblpf_idt.distcode,
                            itg_tblpf_idt.prdcode,
                            itg_tblpf_idt.year,
                            itg_tblpf_idt.month  order by null  ROWS BETWEEN UNBOUNDED PRECEDING
                                AND UNBOUNDED FOLLOWING
                            ) AS salable_idt_out,
                        sum(CASE 
                                WHEN (
                                        (upper((itg_tblpf_idt.stkmgmttype)::TEXT) = ('IDT OUT'::CHARACTER VARYING)::TEXT)
                                        AND (upper((itg_tblpf_idt.stocktype)::TEXT) = ('UNSALABLE'::CHARACTER VARYING)::TEXT)
                                        )
                                    THEN COALESCE((itg_tblpf_idt.baseqty * itg_tblpf_idt.nr), ((0)::NUMERIC)::NUMERIC(18, 0))
                                ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                                END) OVER (
                            PARTITION BY itg_tblpf_idt.distcode,
                            itg_tblpf_idt.prdcode,
                            itg_tblpf_idt.year,
                            itg_tblpf_idt.month  order by null  ROWS BETWEEN UNBOUNDED PRECEDING
                                AND UNBOUNDED FOLLOWING
                            ) AS unsalable_idt_out,
                        sum(CASE 
                                WHEN (
                                        (upper((itg_tblpf_idt.stkmgmttype)::TEXT) = ('IDT OUT'::CHARACTER VARYING)::TEXT)
                                        AND (upper((itg_tblpf_idt.stocktype)::TEXT) = ('OFFER'::CHARACTER VARYING)::TEXT)
                                        )
                                    THEN COALESCE((itg_tblpf_idt.baseqty * itg_tblpf_idt.nr), ((0)::NUMERIC)::NUMERIC(18, 0))
                                ELSE ((0)::NUMERIC)::NUMERIC(18, 0)
                                END) OVER (
                            PARTITION BY itg_tblpf_idt.distcode,
                            itg_tblpf_idt.prdcode,
                            itg_tblpf_idt.year,
                            itg_tblpf_idt.month order by null  ROWS BETWEEN UNBOUNDED PRECEDING
                                AND UNBOUNDED FOLLOWING
                            ) AS offer_idt_out
                    FROM itg_tblpf_idt
                    WHERE (upper((itg_tblpf_idt.STATUS)::TEXT) = ('CONFIRM'::CHARACTER VARYING)::TEXT)
                    ),
cal_dim as
(
                    SELECT DISTINCT (
                            "substring" (
                                ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                5,
                                2
                                )
                            )::INTEGER AS mon,
                        (
                            "substring" (
                                ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                1,
                                4
                                )
                            )::INTEGER AS yr,
                        (
                            "substring" (
                                (
                                    (
                                        (add_months((
                                                    to_date((
                                                            "substring" (
                                                                ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                                                1,
                                                                4
                                                                ) || "substring" (
                                                                ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                                                5,
                                                                2
                                                                )
                                                            ), ('YYYYMM'::CHARACTER VARYING)::TEXT)
                                                    )::TIMESTAMP without TIME zone, (- (1)::BIGINT)))
                                        )::CHARACTER VARYING
                                    )::TEXT,
                                5,
                                2
                                )
                            )::INTEGER AS prev_mon,
                        (
                            "substring" (
                                (
                                    (
                                        (add_months((
                                                    to_date((
                                                            "substring" (
                                                                ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                                                1,
                                                                4
                                                                ) || "substring" (
                                                                ((edw_retailer_calendar_dim.mth_mm)::CHARACTER VARYING)::TEXT,
                                                                5,
                                                                2
                                                                )
                                                            ), ('YYYYMM'::CHARACTER VARYING)::TEXT)
                                                    )::TIMESTAMP without TIME zone, (- (1)::BIGINT)))
                                        )::CHARACTER VARYING
                                    )::TEXT,
                                1,
                                4
                                )
                            )::INTEGER AS prev_yr
                    FROM edw_retailer_calendar_dim
                    ),
stkd1 as

    (
                    SELECT itg_rstockdiscrepancy_withproduct.distcode,
                        itg_rstockdiscrepancy_withproduct.prdcode,
                        itg_rstockdiscrepancy_withproduct.month ,
                        itg_rstockdiscrepancy_withproduct.year,
                        (
                            (
                                (sum((((COALESCE(itg_rstockdiscrepancy_withproduct.salstockin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0)))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.salstkjurin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.salbattfrin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))
                                        ))
                                ) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.sallcntfrin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))
                            ) AS salin_cal,
                        (
                            (
                                (
                                    (sum((((COALESCE(itg_rstockdiscrepancy_withproduct.salstockout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0)))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.salstkjurout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.salbattfrout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))
                                            ))
                                    ) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.sallcntfrout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))
                                ) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.salreplacement, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))
                            ) AS salout_cal,
                        (
                            (
                                (
                                    (sum((((COALESCE(itg_rstockdiscrepancy_withproduct.unsalstockin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0)))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.damagein, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.unsalstkjurin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))
                                            ))
                                    ) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.unsalbattfrin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))
                                ) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.unsallcntfrin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))
                            ) AS unsalin_cal,
                        (
                            (
                                (
                                    (sum((((COALESCE(itg_rstockdiscrepancy_withproduct.unsalstockout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0)))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.damageout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.unsalstkjurout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))
                                            ))
                                    ) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.unsalbattfrout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))
                                ) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.unsallcntfrout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))
                            ) AS unsalout_cal,
                        (
                            (
                                (sum((((COALESCE(itg_rstockdiscrepancy_withproduct.offerstockin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0)))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.offerstkjurin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.offerbattfrin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))
                                        ))
                                ) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.offerlcntfrin, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))
                            ) AS offin_cal,
                        (
                            (
                                (
                                    (sum((((COALESCE(itg_rstockdiscrepancy_withproduct.offerstockout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0)))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.offerstkjurout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.offerbattfrout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))
                                            ))
                                    ) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.offerlcntfrout, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))
                                ) + sum((((COALESCE(itg_rstockdiscrepancy_withproduct.offerreplacement, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))))
                            ) AS offout_cal,
                        sum((
                                ((((COALESCE(itg_rstockdiscrepancy_withproduct.salpurreturn, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))) + (((COALESCE(itg_rstockdiscrepancy_withproduct.unsalpurreturn, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0)))) + (((COALESCE(itg_rstockdiscrepancy_withproduct.offerpurreturn, 0))::NUMERIC)::NUMERIC(18, 0) * COALESCE(itg_rstockdiscrepancy_withproduct.nr, ((0)::NUMERIC)::NUMERIC(18, 0))
                                    )
                                )) AS purchasereturn
                    FROM itg_rstockdiscrepancy_withproduct
                    GROUP BY itg_rstockdiscrepancy_withproduct.distcode,
                        itg_rstockdiscrepancy_withproduct.prdcode,
                        itg_rstockdiscrepancy_withproduct.month,
                        itg_rstockdiscrepancy_withproduct.year
                    ),
                    stkd as
                    (
                select * from  cal_dim JOIN  stkd1 ON (
                        (
                            (cal_dim.mon = stkd1.month )
                            AND (cal_dim.yr = stkd1.year)
                            )
                        )
                ),
                id as 
                (
                select * from
            (
                 cal_dim JOIN id1 ON (
                        (
                            (((cal_dim.mon)::CHARACTER VARYING)::TEXT = (id1.month )::TEXT)
                            AND (((cal_dim.yr)::CHARACTER VARYING)::TEXT = (id1.year)::TEXT)
                            )
                        )
                )
                ),
union_7
AS (
    SELECT idt_kpi.mon AS month ,
        idt_kpi.yr AS year,
        idt_kpi.distcode AS customer_code,
        idt_kpi.prdcode AS product_code,
        0 AS invoice_qty,
        0 AS invoice_val,
        0 AS zf2d_value,
        0 AS zl2d_value,
        0 AS zg2d_value,
        0 AS zc2d_value,
        0 AS zf2e_value,
        0 AS zl3d_value,
        0 AS zg3d_value,
        0 AS zc3d_value,
        0 AS zl22_value,
        0 AS zg22_value,
        0 AS zc22_value,
        0 AS s1_value,
        0 AS s2_value,
        0 AS zrsm_value,
        0 AS zsmd_value,
        0 AS sec_prd_qty,
        0 AS sec_ptr_value,
        0 AS sec_prd_nr_value,
        0 AS sec_lp_value,
        0 AS sec_prd_qty_ret,
        0 AS sec_ptr_value_ret,
        0 AS sec_prd_nr_value_ret,
        0 AS sec_lp_value_ret,
        0 AS sec_tr_in_qty,
        0 AS sec_tr_out_qty,
        0 AS sec_tr_in_val,
        0 AS sec_tr_out_val,
        0 AS sec_non_wave_open_qty,
        0 AS sec_gross_amt,
        0 AS sec_gross_amt_ret,
        0 AS sec_late_prd_qty,
        0 AS sec_late_ptr_value,
        0 AS sec_late_prd_nr_value,
        0 AS sec_late_lp_value,
        0 AS sec_late_prd_qty_ret,
        0 AS sec_late_ptr_value_ret,
        0 AS sec_late_prd_nr_value_ret,
        0 AS sec_late_lp_value_ret,
        0 AS sec_late_tr_in_qty,
        0 AS sec_late_tr_out_qty,
        0 AS sec_late_tr_in_val,
        0 AS sec_late_tr_out_val,
        0 AS sec_late_non_wave_open_qty,
        0 AS sec_late_gross_amt,
        0 AS sec_late_gross_amt_ret,
        0 AS dbrestore_last_month_sec_late_prd_qty,
        0 AS dbrestore_last_month_sec_late_ptr_value,
        0 AS dbrestore_last_month_sec_late_prd_nr_value,
        0 AS dbrestore_last_month_sec_late_prd_qty_ret,
        0 AS dbrestore_last_month_sec_late_ptr_value_ret,
        0 AS dbrestore_last_month_sec_late_prd_nr_value_ret,
        0 AS dbrestore_last_month_sec_late_gross_amt,
        0 AS dbrestore_last_month_sec_late_gross_amt_ret,
        0 AS dbrestore_current_sec_late_prd_qty,
        0 AS dbrestore_current_sec_late_ptr_value,
        0 AS dbrestore_current_sec_late_prd_nr_value,
        0 AS dbrestore_current_sec_late_prd_qty_ret,
        0 AS dbrestore_current_sec_late_ptr_value_ret,
        0 AS dbrestore_current_sec_late_prd_nr_value_ret,
        0 AS dbrestore_current_sec_late_gross_amt,
        0 AS dbrestore_current_sec_late_gross_amt_ret,
        0 AS actual_sec_late_prd_qty,
        0 AS actual_sec_late_prd_qty_ret,
        0 AS actual_sec_late_ptr_value,
        0 AS actual_sec_late_ptr_value_ret,
        0 AS actual_sec_late_prd_nr_value,
        0 AS actual_sec_late_prd_nr_value_ret,
        0 AS actual_sec_late_gross_amt,
        0 AS actual_sec_late_gross_amt_ret,
        0 AS retailing_quantity,
        0 AS retailing_ptr_value,
        0 AS retailing_nr_value,
        0 AS retailing_gross_value,
        0 AS opnstk_value_nr,
        0 AS opnstk_qty,
        0 AS opnstk_nr,
        0 AS salable_opnstk,
        0 AS unsalable_opnstk,
        0 AS offer_opnstk,
        0 AS cl_stck_value_nr,
        0 AS cl_stck_qty,
        0 AS cl_stck_nr,
        0 AS salable_cl_stk,
        0 AS unsalable_cl_stk,
        0 AS offer_cl_stk,
        0 AS sit_value,
        0 AS opening_sit_value,
        COALESCE(idt_kpi.salable_idt, ((0)::NUMERIC)::NUMERIC(18, 0)) AS salable_idt,
        COALESCE(idt_kpi.salable_stkadj, ((0)::NUMERIC)::NUMERIC(18, 0)) AS salable_stkadj,
        COALESCE(idt_kpi.unsalable_idt, ((0)::NUMERIC)::NUMERIC(18, 0)) AS unsalable_idt,
        COALESCE(idt_kpi.unsalable_stkadj, ((0)::NUMERIC)::NUMERIC(18, 0)) AS unsalable_stkadj,
        COALESCE(idt_kpi.offer_idt, ((0)::NUMERIC)::NUMERIC(18, 0)) AS offer_idt,
        COALESCE(idt_kpi.offer_stkadj, ((0)::NUMERIC)::NUMERIC(18, 0)) AS offer_stkadj,
        COALESCE(idt_kpi.purchasereturn, ((0)::NUMERIC)::NUMERIC(18, 0)) AS purchasereturn
    FROM (
        SELECT CASE 
                WHEN (id.distcode IS NULL)
                    THEN stkd.distcode
                ELSE id.distcode
                END AS distcode,
            CASE 
                WHEN (id.prdcode IS NULL)
                    THEN stkd.prdcode
                ELSE id.prdcode
                END AS prdcode,
            CASE 
                WHEN (id.month  IS NULL)
                    THEN stkd.month 
                ELSE (id.month )::INTEGER
                END AS mon,
            CASE 
                WHEN (id.year IS NULL)
                    THEN stkd.year
                ELSE (id.year)::INTEGER
                END AS yr,
            (COALESCE(id.salable_idt_in, ((0)::NUMERIC)::NUMERIC(18, 0)) - COALESCE(id.salable_idt_out, ((0)::NUMERIC)::NUMERIC(18, 0))) AS salable_idt,
            COALESCE((((stkd.salin_cal - COALESCE(id.salable_idt_in, ((0)::NUMERIC)::NUMERIC(18, 0))) - stkd.salout_cal) - COALESCE(id.salable_idt_out, ((0)::NUMERIC)::NUMERIC(18, 0))), ((0)::NUMERIC)::NUMERIC(18, 0)) AS salable_stkadj,
            COALESCE((id.unsalable_idt_in - id.unsalable_idt_out), ((0)::NUMERIC)::NUMERIC(18, 0)) AS unsalable_idt,
            COALESCE((((stkd.unsalin_cal - COALESCE(id.unsalable_idt_in, ((0)::NUMERIC)::NUMERIC(18, 0))) - stkd.unsalout_cal) - COALESCE(id.unsalable_idt_out, ((0)::NUMERIC)::NUMERIC(18, 0))), ((0)::NUMERIC)::NUMERIC(18, 0)) AS unsalable_stkadj,
            COALESCE((id.offer_idt_in - id.offer_idt_out), ((0)::NUMERIC)::NUMERIC(18, 0)) AS offer_idt,
            COALESCE((((stkd.offin_cal - COALESCE(id.offer_idt_in, ((0)::NUMERIC)::NUMERIC(18, 0))) - stkd.offout_cal) - COALESCE(id.offer_idt_out, ((0)::NUMERIC)::NUMERIC(18, 0))), ((0)::NUMERIC)::NUMERIC(18, 0)) AS offer_stkadj,
            COALESCE(stkd.purchasereturn, ((0)::NUMERIC)::NUMERIC(18, 0)) AS purchasereturn
        FROM ( id FULL JOIN stkd ON (
                    (
                        (
                            (
                                ((id.distcode)::TEXT = (stkd.distcode)::TEXT)
                                AND ((id.prdcode)::TEXT = (stkd.prdcode)::TEXT)
                                )
                            AND ((id.month )::TEXT = ((stkd.month )::CHARACTER VARYING)::TEXT)
                            )
                        AND ((id.year)::TEXT = ((stkd.year)::CHARACTER VARYING)::TEXT)
                        )
                    )
            )
        ) idt_kpi
    ),
trans
AS (
    SELECT tr."month",
        tr."year",
        cal.mth_mm,
        cal.mth_yyyymm,
        cal.qtr,
        cal.yyyyqtr,
        cal.fisc_yr,
        cal.month_nm_shrt,
        tr.customer_code,
        COALESCE(cust.customer_name, 'Unknown'::CHARACTER VARYING) AS customer_name,
        COALESCE(cust.region_name, 'Unknown'::CHARACTER VARYING) AS region_name,
        COALESCE(cust.zone_name, 'Unknown'::CHARACTER VARYING) AS zone_name,
        COALESCE(cust.territory_name, 'Unknown'::CHARACTER VARYING) AS territory_name,
        COALESCE(cust.territory_classification, 'Unknown'::CHARACTER VARYING) AS territory_classification,
        COALESCE(cust.zone_classification, 'Unknown'::CHARACTER VARYING) AS zone_classification,
        COALESCE(cust.town_classification, 'Unknown'::CHARACTER VARYING) AS town_classification,
        cust.state_code,
        COALESCE(cust.state_name, 'Unknown'::CHARACTER VARYING) AS state_name,
        COALESCE(cust.psnonps, 'Unknown'::VARCHAR) AS psnonps,
        --COALESCE(cust.psnonps, 'Unknown'::char) AS psnonps,
        CASE 
            WHEN (upper(((cust.psnonps)::CHARACTER VARYING)::TEXT) = ('Y'::CHARACTER VARYING)::TEXT)
                THEN 'Wave'::CHARACTER VARYING
            WHEN (upper(((cust.psnonps)::CHARACTER VARYING)::TEXT) = ('N'::CHARACTER VARYING)::TEXT)
                THEN 'Non Wave'::CHARACTER VARYING
            ELSE 'Unknown'::CHARACTER VARYING
            END AS source,
        COALESCE(cust.super_stockiest, 'Unknown'::CHARACTER VARYING) AS super_stockiest,
        COALESCE(cust.type_name, 'Unknown'::CHARACTER VARYING) AS type_name,
        tr.product_code,
        COALESCE(prd.product_name, 'Unknown'::CHARACTER VARYING) AS product_name,
        COALESCE(prd.franchise_name, 'Unknown'::CHARACTER VARYING) AS franchise_name,
        COALESCE(prd.brand_name, 'Unknown'::CHARACTER VARYING) AS brand_name,
        COALESCE(prd.product_category_name, 'Unknown'::CHARACTER VARYING) AS product_category_name,
        COALESCE(prd.variant_name, 'Unknown'::CHARACTER VARYING) AS variant_name,
        COALESCE(prd.mothersku_name, 'Unknown') AS mothersku_name,
        --COALESCE(prd.mothersku_name, 'Unknown'::CHARACTER VARYING) AS mothersku_name,
        NULL AS abi_ntid,

        NULL AS flm_ntid,

        NULL AS bdm_ntid,

        NULL AS rsm_ntid,
        plt.plantid,
        COALESCE(plt.plantname, 'Unknown'::CHARACTER VARYING) AS plantname,
        tr.invoice_qty,
        tr.invoice_val,
        tr.zf2d_value,
        tr.zl2d_value,
        tr.zg2d_value,
        tr.zc2d_value,
        tr.zf2e_value,
        tr.zl3d_value,
        tr.zg3d_value,
        tr.zc3d_value,
        tr.zl22_value,
        tr.zg22_value,
        tr.zc22_value,
        tr.s1_value,
        tr.s2_value,
        tr.zrsm_value,
        tr.zsmd_value,
        tr.sec_prd_qty,
        tr.sec_ptr_value,
        tr.sec_prd_nr_value,
        tr.sec_lp_value,
        tr.sec_prd_qty_ret,
        tr.sec_ptr_value_ret,
        tr.sec_prd_nr_value_ret,
        tr.sec_lp_value_ret,
        tr.sec_tr_in_qty,
        tr.sec_tr_out_qty,
        tr.sec_tr_in_val,
        tr.sec_tr_out_val,
        tr.sec_non_wave_open_qty,
        tr.sec_gross_amt,
        tr.sec_gross_amt_ret,
        tr.sec_late_prd_qty,
        tr.sec_late_ptr_value,
        tr.sec_late_prd_nr_value,
        tr.sec_late_lp_value,
        tr.sec_late_prd_qty_ret,
        tr.sec_late_ptr_value_ret,
        tr.sec_late_prd_nr_value_ret,
        tr.sec_late_lp_value_ret,
        tr.sec_late_tr_in_qty,
        tr.sec_late_tr_out_qty,
        tr.sec_late_tr_in_val,
        tr.sec_late_tr_out_val,
        tr.sec_late_non_wave_open_qty,
        tr.sec_late_gross_amt,
        tr.sec_late_gross_amt_ret,
        tr.dbrestore_last_month_sec_late_prd_qty,
        tr.dbrestore_last_month_sec_late_ptr_value,
        tr.dbrestore_last_month_sec_late_prd_nr_value,
        tr.dbrestore_last_month_sec_late_prd_qty_ret,
        tr.dbrestore_last_month_sec_late_ptr_value_ret,
        tr.dbrestore_last_month_sec_late_prd_nr_value_ret,
        tr.dbrestore_last_month_sec_late_gross_amt,
        tr.dbrestore_last_month_sec_late_gross_amt_ret,
        tr.dbrestore_current_sec_late_prd_qty,
        tr.dbrestore_current_sec_late_ptr_value,
        tr.dbrestore_current_sec_late_prd_nr_value,
        tr.dbrestore_current_sec_late_prd_qty_ret,
        tr.dbrestore_current_sec_late_ptr_value_ret,
        tr.dbrestore_current_sec_late_prd_nr_value_ret,
        tr.dbrestore_current_sec_late_gross_amt,
        tr.dbrestore_current_sec_late_gross_amt_ret,
        tr.actual_sec_late_prd_qty,
        tr.actual_sec_late_prd_qty_ret,
        tr.actual_sec_late_ptr_value,
        tr.actual_sec_late_ptr_value_ret,
        tr.actual_sec_late_prd_nr_value,
        tr.actual_sec_late_prd_nr_value_ret,
        tr.actual_sec_late_gross_amt,
        tr.actual_sec_late_gross_amt_ret,
        tr.retailing_quantity,
        tr.retailing_ptr_value,
        tr.retailing_nr_value,
        tr.retailing_gross_value,
        tr.opnstk_value_nr,
        tr.opnstk_qty,
        tr.opnstk_nr,
        tr.salable_opnstk,
        tr.unsalable_opnstk,
        tr.offer_opnstk,
        tr.cl_stck_value_nr,
        tr.cl_stck_qty,
        tr.cl_stck_nr,
        tr.salable_cl_stk,
        tr.unsalable_cl_stk,
        tr.offer_cl_stk,
        tr.sit_value,
        tr.opening_sit_value,
        tr.salable_idt,
        tr.salable_stkadj,
        tr.unsalable_idt,
        tr.unsalable_stkadj,
        tr.offer_idt,
        tr.offer_stkadj,
        tr.purchasereturn
    FROM (
        (
            (
                (
                    (
                        SELECT tr1."month",
                            tr1."year",
                            tr1.customer_code,
                            tr1.product_code,
                            sum(COALESCE(tr1.invoice_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS invoice_qty,
                            sum(COALESCE(tr1.invoice_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS invoice_val,
                            sum(COALESCE(tr1.zf2d_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zf2d_value,
                            sum(COALESCE(tr1.zl2d_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zl2d_value,
                            sum(COALESCE(tr1.zg2d_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zg2d_value,
                            sum(COALESCE(tr1.zc2d_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zc2d_value,
                            sum(COALESCE(tr1.zf2e_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zf2e_value,
                            sum(COALESCE(tr1.zl3d_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zl3d_value,
                            sum(COALESCE(tr1.zg3d_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zg3d_value,
                            sum(COALESCE(tr1.zc3d_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zc3d_value,
                            sum(COALESCE(tr1.zl22_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zl22_value,
                            sum(COALESCE(tr1.zg22_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zg22_value,
                            sum(COALESCE(tr1.zc22_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zc22_value,
                            sum(COALESCE(tr1.s1_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS s1_value,
                            sum(COALESCE(tr1.s2_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS s2_value,
                            sum(COALESCE(tr1.zrsm_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zrsm_value,
                            sum(COALESCE(tr1.zsmd_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS zsmd_value,
                            sum(COALESCE(tr1.sec_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_qty,
                            sum(COALESCE(tr1.sec_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_ptr_value,
                            sum(COALESCE(tr1.sec_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_nr_value,
                            sum(COALESCE(tr1.sec_lp_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_lp_value,
                            sum(COALESCE(tr1.sec_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_qty_ret,
                            sum(COALESCE(tr1.sec_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_ptr_value_ret,
                            sum(COALESCE(tr1.sec_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_prd_nr_value_ret,
                            sum(COALESCE(tr1.sec_lp_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_lp_value_ret,
                            sum(COALESCE(tr1.sec_tr_in_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_in_qty,
                            sum(COALESCE(tr1.sec_tr_out_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_out_qty,
                            sum(COALESCE(tr1.sec_tr_in_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_in_val,
                            sum(COALESCE(tr1.sec_tr_out_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_tr_out_val,
                            sum(COALESCE(tr1.sec_non_wave_open_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_non_wave_open_qty,
                            sum(COALESCE(tr1.sec_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_gross_amt,
                            sum(COALESCE(tr1.sec_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_gross_amt_ret,
                            sum(COALESCE(tr1.sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_qty,
                            sum(COALESCE(tr1.sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_ptr_value,
                            sum(COALESCE(tr1.sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_nr_value,
                            sum(COALESCE(tr1.sec_late_lp_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_lp_value,
                            sum(COALESCE(tr1.sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_qty_ret,
                            sum(COALESCE(tr1.sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_ptr_value_ret,
                            sum(COALESCE(tr1.sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_prd_nr_value_ret,
                            sum(COALESCE(tr1.sec_late_lp_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_lp_value_ret,
                            sum(COALESCE(tr1.sec_late_tr_in_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_in_qty,
                            sum(COALESCE(tr1.sec_late_tr_out_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_out_qty,
                            sum(COALESCE(tr1.sec_late_tr_in_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_in_val,
                            sum(COALESCE(tr1.sec_late_tr_out_val, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_tr_out_val,
                            sum(COALESCE(tr1.sec_late_non_wave_open_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_non_wave_open_qty,
                            sum(COALESCE(tr1.sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_gross_amt,
                            sum(COALESCE(tr1.sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sec_late_gross_amt_ret,
                            sum(COALESCE(tr1.dbrestore_last_month_sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_qty,
                            sum(COALESCE(tr1.dbrestore_last_month_sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_ptr_value,
                            sum(COALESCE(tr1.dbrestore_last_month_sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_nr_value,
                            sum(COALESCE(tr1.dbrestore_last_month_sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_qty_ret,
                            sum(COALESCE(tr1.dbrestore_last_month_sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_ptr_value_ret,
                            sum(COALESCE(tr1.dbrestore_last_month_sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_prd_nr_value_ret,
                            sum(COALESCE(tr1.dbrestore_last_month_sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_gross_amt,
                            sum(COALESCE(tr1.dbrestore_last_month_sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_last_month_sec_late_gross_amt_ret,
                            sum(COALESCE(tr1.dbrestore_current_sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_qty,
                            sum(COALESCE(tr1.dbrestore_current_sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_ptr_value,
                            sum(COALESCE(tr1.dbrestore_current_sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_nr_value,
                            sum(COALESCE(tr1.dbrestore_current_sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_qty_ret,
                            sum(COALESCE(tr1.dbrestore_current_sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_ptr_value_ret,
                            sum(COALESCE(tr1.dbrestore_current_sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_prd_nr_value_ret,
                            sum(COALESCE(tr1.dbrestore_current_sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_gross_amt,
                            sum(COALESCE(tr1.dbrestore_current_sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS dbrestore_current_sec_late_gross_amt_ret,
                            sum(COALESCE(tr1.actual_sec_late_prd_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS actual_sec_late_prd_qty,
                            sum(COALESCE(tr1.actual_sec_late_prd_qty_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS actual_sec_late_prd_qty_ret,
                            sum(COALESCE(tr1.actual_sec_late_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS actual_sec_late_ptr_value,
                            sum(COALESCE(tr1.actual_sec_late_ptr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS actual_sec_late_ptr_value_ret,
                            sum(COALESCE(tr1.actual_sec_late_prd_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS actual_sec_late_prd_nr_value,
                            sum(COALESCE(tr1.actual_sec_late_prd_nr_value_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS actual_sec_late_prd_nr_value_ret,
                            sum(COALESCE(tr1.actual_sec_late_gross_amt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS actual_sec_late_gross_amt,
                            sum(COALESCE(tr1.actual_sec_late_gross_amt_ret, ((0)::NUMERIC)::NUMERIC(18, 0))) AS actual_sec_late_gross_amt_ret,
                            sum(COALESCE(tr1.retailing_quantity, ((0)::NUMERIC)::NUMERIC(18, 0))) AS retailing_quantity,
                            sum(COALESCE(tr1.retailing_ptr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS retailing_ptr_value,
                            sum(COALESCE(tr1.retailing_nr_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS retailing_nr_value,
                            sum(COALESCE(tr1.retailing_gross_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS retailing_gross_value,
                            sum(COALESCE(tr1.opnstk_value_nr, ((0)::NUMERIC)::NUMERIC(18, 0))) AS opnstk_value_nr,
                            sum(COALESCE(tr1.opnstk_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS opnstk_qty,
                            sum(COALESCE(tr1.opnstk_nr, ((0)::NUMERIC)::NUMERIC(18, 0))) AS opnstk_nr,
                            sum(COALESCE(tr1.salable_opnstk, ((0)::NUMERIC)::NUMERIC(18, 0))) AS salable_opnstk,
                            sum(COALESCE(tr1.unsalable_opnstk, ((0)::NUMERIC)::NUMERIC(18, 0))) AS unsalable_opnstk,
                            sum(COALESCE(tr1.offer_opnstk, ((0)::NUMERIC)::NUMERIC(18, 0))) AS offer_opnstk,
                            sum(COALESCE(tr1.cl_stck_value_nr, ((0)::NUMERIC)::NUMERIC(18, 0))) AS cl_stck_value_nr,
                            sum(COALESCE(tr1.cl_stck_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) AS cl_stck_qty,
                            sum(COALESCE(tr1.cl_stck_nr, ((0)::NUMERIC)::NUMERIC(18, 0))) AS cl_stck_nr,
                            sum(COALESCE(tr1.salable_cl_stk, ((0)::NUMERIC)::NUMERIC(18, 0))) AS salable_cl_stk,
                            sum(COALESCE(tr1.unsalable_cl_stk, ((0)::NUMERIC)::NUMERIC(18, 0))) AS unsalable_cl_stk,
                            sum(COALESCE(tr1.offer_cl_stk, ((0)::NUMERIC)::NUMERIC(18, 0))) AS offer_cl_stk,
                            sum(COALESCE(tr1.sit_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS sit_value,
                            sum(COALESCE(tr1.opening_sit_value, ((0)::NUMERIC)::NUMERIC(18, 0))) AS opening_sit_value,
                            sum(COALESCE(tr1.salable_idt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS salable_idt,
                            sum(COALESCE(tr1.salable_stkadj, ((0)::NUMERIC)::NUMERIC(18, 0))) AS salable_stkadj,
                            sum(COALESCE(tr1.unsalable_idt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS unsalable_idt,
                            sum(COALESCE(tr1.unsalable_stkadj, ((0)::NUMERIC)::NUMERIC(18, 0))) AS unsalable_stkadj,
                            sum(COALESCE(tr1.offer_idt, ((0)::NUMERIC)::NUMERIC(18, 0))) AS offer_idt,
                            sum(COALESCE(tr1.offer_stkadj, ((0)::NUMERIC)::NUMERIC(18, 0))) AS offer_stkadj,
                            sum(COALESCE(tr1.purchasereturn, ((0)::NUMERIC)::NUMERIC(18, 0))) AS purchasereturn
                        FROM (
                            SELECT *
                            FROM union_1

                            UNION ALL

                            SELECT *
                            FROM union_2

                            UNION ALL

                            SELECT *
                            FROM union_3

                            UNION ALL

                            SELECT *
                            FROM union_4

                            UNION ALL

                            SELECT *
                            FROM union_5

                            UNION ALL

                            SELECT *
                            FROM union_6

                            UNION ALL

                            SELECT *
                            FROM union_7
                            ) tr1
                        GROUP BY tr1."month",
                            tr1."year",
                            tr1.customer_code,
                            tr1.product_code
                        ) tr LEFT JOIN (
                        SELECT DISTINCT edw_retailer_calendar_dim.mth_yyyymm,
                            edw_retailer_calendar_dim.mth_mm,
                            edw_retailer_calendar_dim.qtr,
                            edw_retailer_calendar_dim.yyyyqtr,
                            edw_retailer_calendar_dim.fisc_yr,
                            edw_retailer_calendar_dim.month_nm_shrt
                        FROM edw_retailer_calendar_dim
                        ) cal ON (
                            (
                                (tr."month" = cal.mth_yyyymm)
                                AND (tr."year" = cal.fisc_yr)
                                )
                            )
                    ) LEFT JOIN edw_product_dim prd ON (((tr.product_code)::TEXT = (prd.product_code)::TEXT))
                ) LEFT JOIN edw_customer_dim cust ON (((tr.customer_code)::TEXT = (cust.customer_code)::TEXT))
            ) LEFT JOIN (
            SELECT DISTINCT itg_plant.plantcode,
                itg_plant.plantid,
                itg_plant.plantname,
                row_number() OVER (
                    PARTITION BY itg_plant.plantcode ORDER BY itg_plant.crt_dttm DESC
                    ) AS rn
            FROM itg_plant
            ) plt ON (
                (
                    (cust.suppliedby = plt.plantcode)
                    AND (plt.rn = 1)
                    )
                )
        )
    WHERE (
            (tr."year")::DOUBLE PRECISION >= (
                date_part(year, (to_date(current_timestamp::TIMESTAMP without TIME zone))::TIMESTAMP without TIME zone) - (
                    (
                        SELECT (itg_query_parameters.parameter_value)::INTEGER AS parameter_value
                        FROM itg_query_parameters
                        WHERE (
                                (upper((itg_query_parameters.country_code)::TEXT) = ('IN'::CHARACTER VARYING)::TEXT)
                                AND (upper((itg_query_parameters.parameter_name)::TEXT) = ('INDIA_INVENTORY_HEALTH_V_PF_SALES_STOCK_DATA_RETENTION_YEARS'::CHARACTER VARYING)::TEXT)
                                )
                        )
                    )::DOUBLE PRECISION
                )
            )
    ),
final as
(
    select
    "month"::number(38,0) as month,
	"year"::number(38,0) as year,
	mth_mm::number(38,0) as mth_mm,
	mth_yyyymm::number(38,0) as mth_yyyymm,
	qtr::number(38,0) as qtr,
	yyyyqtr::number(38,0) as yyyyqtr,
	fisc_yr::number(38,0) as fisc_yr,
	month_nm_shrt::varchar(3) as month_nm_shrt,
	customer_code::varchar(50) as customer_code,
	customer_name::varchar(150) as customer_name,
	region_name::varchar(50) as region_name,
	zone_name::varchar(50) as zone_name,
	territory_name::varchar(50) as territory_name,
	territory_classification::varchar(50) as territory_classification,
	zone_classification::varchar(50) as zone_classification,
	town_classification::varchar(100) as town_classification,
	state_code::number(18,0) as state_code,
	state_name::varchar(50) as state_name,
	psnonps::varchar(7) as psnonps,
	source::varchar(8) as source,
	super_stockiest::varchar(50) as super_stockiest,
	type_name::varchar(50) as type_name,
	product_code::varchar(50) as product_code,
	product_name::varchar(50) as product_name,
	franchise_name::varchar(50) as franchise_name,
	brand_name::varchar(50) as brand_name,
	product_category_name::varchar(150) as product_category_name,
	variant_name::varchar(150) as variant_name,
	mothersku_name::varchar(150) as mothersku_name,
	abi_ntid::varchar(16777216) as abi_ntid,
	flm_ntid::varchar(16777216) as flm_ntid,
	bdm_ntid::varchar(16777216) as bdm_ntid,
	rsm_ntid::varchar(16777216) as rsm_ntid,
	plantid::varchar(50) as plantid,
	plantname::varchar(50) as plantname,
	invoice_qty::number(38,4) as invoice_qty,
	invoice_val::number(38,4) as invoice_val,
	zf2d_value::number(38,4) as zf2d_value,
	zl2d_value::number(38,4) as zl2d_value,
	zg2d_value::number(38,4) as zg2d_value,
	zc2d_value::number(38,4) as zc2d_value,
	zf2e_value::number(38,4) as zf2e_value,
	zl3d_value::number(38,4) as zl3d_value,
	zg3d_value::number(38,4) as zg3d_value,
	zc3d_value::number(38,4) as zc3d_value,
	zl22_value::number(38,4) as zl22_value,
	zg22_value::number(38,4) as zg22_value,
	zc22_value::number(38,4) as zc22_value,
	s1_value::number(38,4) as s1_value,
	s2_value::number(38,4) as s2_value,
	zrsm_value::number(38,4) as zrsm_value,
	zsmd_value::number(38,4) as zsmd_value,
	sec_prd_qty::number(38,4) as sec_prd_qty,
	sec_ptr_value::number(38,4) as sec_ptr_value,
	sec_prd_nr_value::number(38,3) as sec_prd_nr_value,
	sec_lp_value::number(38,3) as sec_lp_value,
	sec_prd_qty_ret::number(38,4) as sec_prd_qty_ret,
	sec_ptr_value_ret::number(38,4) as sec_ptr_value_ret,
	sec_prd_nr_value_ret::number(38,3) as sec_prd_nr_value_ret,
	sec_lp_value_ret::number(38,3) as sec_lp_value_ret,
	sec_tr_in_qty::number(38,3) as sec_tr_in_qty,
	sec_tr_out_qty::number(38,3) as sec_tr_out_qty,
	sec_tr_in_val::number(38,3) as sec_tr_in_val,
	sec_tr_out_val::number(38,3) as sec_tr_out_val,
	sec_non_wave_open_qty::number(38,3) as sec_non_wave_open_qty,
	sec_gross_amt::number(38,4) as sec_gross_amt,
	sec_gross_amt_ret::number(38,4) as sec_gross_amt_ret,
	sec_late_prd_qty::number(38,4) as sec_late_prd_qty,
	sec_late_ptr_value::number(38,4) as sec_late_ptr_value,
	sec_late_prd_nr_value::number(38,3) as sec_late_prd_nr_value,
	sec_late_lp_value::number(38,3) as sec_late_lp_value,
	sec_late_prd_qty_ret::number(38,4) as sec_late_prd_qty_ret,
	sec_late_ptr_value_ret::number(38,4) as sec_late_ptr_value_ret,
	sec_late_prd_nr_value_ret::number(38,3) as sec_late_prd_nr_value_ret,
	sec_late_lp_value_ret::number(38,3) as sec_late_lp_value_ret,
	sec_late_tr_in_qty::number(38,3) as sec_late_tr_in_qty,
	sec_late_tr_out_qty::number(38,3) as sec_late_tr_out_qty,
	sec_late_tr_in_val::number(38,3) as sec_late_tr_in_val,
	sec_late_tr_out_val::number(38,3) as sec_late_tr_out_val,
	sec_late_non_wave_open_qty::number(38,3) as sec_late_non_wave_open_qty,
	sec_late_gross_amt::number(38,4) as sec_late_gross_amt,
	sec_late_gross_amt_ret::number(38,4) as sec_late_gross_amt_ret,
	dbrestore_last_month_sec_late_prd_qty::number(38,4) as dbrestore_last_month_sec_late_prd_qty,
	dbrestore_last_month_sec_late_ptr_value::number(38,4) as dbrestore_last_month_sec_late_ptr_value,
	dbrestore_last_month_sec_late_prd_nr_value::number(38,3) as dbrestore_last_month_sec_late_prd_nr_value,
	dbrestore_last_month_sec_late_prd_qty_ret::number(38,4) as dbrestore_last_month_sec_late_prd_qty_ret,
	dbrestore_last_month_sec_late_ptr_value_ret::number(38,4) as dbrestore_last_month_sec_late_ptr_value_ret,
	dbrestore_last_month_sec_late_prd_nr_value_ret::number(38,3) as dbrestore_last_month_sec_late_prd_nr_value_ret,
	dbrestore_last_month_sec_late_gross_amt::number(38,4) as dbrestore_last_month_sec_late_gross_amt,
	dbrestore_last_month_sec_late_gross_amt_ret::number(38,4) as dbrestore_last_month_sec_late_gross_amt_ret,
	dbrestore_current_sec_late_prd_qty::number(38,4) as dbrestore_current_sec_late_prd_qty,
	dbrestore_current_sec_late_ptr_value::number(38,4) as dbrestore_current_sec_late_ptr_value,
	dbrestore_current_sec_late_prd_nr_value::number(38,3) as dbrestore_current_sec_late_prd_nr_value,
	dbrestore_current_sec_late_prd_qty_ret::number(38,4) as dbrestore_current_sec_late_prd_qty_ret,
	dbrestore_current_sec_late_ptr_value_ret::number(38,4) as dbrestore_current_sec_late_ptr_value_ret,
	dbrestore_current_sec_late_prd_nr_value_ret::number(38,3) as dbrestore_current_sec_late_prd_nr_value_ret,
	dbrestore_current_sec_late_gross_amt::number(38,4) as dbrestore_current_sec_late_gross_amt,
	dbrestore_current_sec_late_gross_amt_ret::number(38,4) as dbrestore_current_sec_late_gross_amt_ret,
	actual_sec_late_prd_qty::number(38,4) as actual_sec_late_prd_qty,
	actual_sec_late_prd_qty_ret::number(38,4) as actual_sec_late_prd_qty_ret,
	actual_sec_late_ptr_value::number(38,4) as actual_sec_late_ptr_value,
	actual_sec_late_ptr_value_ret::number(38,4) as actual_sec_late_ptr_value_ret,
	actual_sec_late_prd_nr_value::number(38,3) as actual_sec_late_prd_nr_value,
	actual_sec_late_prd_nr_value_ret::number(38,3) as actual_sec_late_prd_nr_value_ret,
	actual_sec_late_gross_amt::number(38,4) as actual_sec_late_gross_amt,
	actual_sec_late_gross_amt_ret::number(38,4) as actual_sec_late_gross_amt_ret,
	retailing_quantity::number(38,4) as retailing_quantity,
	retailing_ptr_value::number(38,4) as retailing_ptr_value,
	retailing_nr_value::number(38,3) as retailing_nr_value,
	retailing_gross_value::number(38,4) as retailing_gross_value,
	opnstk_value_nr::number(38,3) as opnstk_value_nr,
	opnstk_qty::number(38,3) as opnstk_qty,
	opnstk_nr::number(38,3) as opnstk_nr,
	salable_opnstk::number(38,3) as salable_opnstk,
	unsalable_opnstk::number(38,3) as unsalable_opnstk,
	offer_opnstk::number(38,3) as offer_opnstk,
	cl_stck_value_nr::number(38,3) as cl_stck_value_nr,
	cl_stck_qty::number(38,3) as cl_stck_qty,
	cl_stck_nr::number(38,3) as cl_stck_nr,
	salable_cl_stk::number(38,3) as salable_cl_stk,
	unsalable_cl_stk::number(38,3) as unsalable_cl_stk,
	offer_cl_stk::number(38,3) as offer_cl_stk,
	sit_value::number(38,4) as sit_value,
	opening_sit_value::number(38,4) as opening_sit_value,
	salable_idt::number(38,2) as salable_idt,
	salable_stkadj::number(38,6) as salable_stkadj,
	unsalable_idt::number(38,2) as unsalable_idt,
	unsalable_stkadj::number(38,6) as unsalable_stkadj,
	offer_idt::number(38,2) as offer_idt,
	offer_stkadj::number(38,6) as offer_stkadj,
	purchasereturn::number(38,6) as purchasereturn
    from trans

    )
select * from final