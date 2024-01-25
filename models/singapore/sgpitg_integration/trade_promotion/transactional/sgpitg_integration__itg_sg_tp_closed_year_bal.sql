{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["month_number"]
        )
}}
--Import CTE
with source as (
    select *
    from {{ source('sgpsdl_raw', 'sdl_sg_tp_closed_year_bal') }}
),

--Logical CTE

--Final CTE
final as (
    select
  file_name::varchar(255) as file_name,
  sheet_name::varchar(255) as sheet_name,
  month_number::varchar(255) as month_number,
  sales_rep::varchar(255) as sales_rep,
  customer_l1::varchar(255) as customer_l1,
  customer::varchar(255) as customer,
  customer_code::varchar(255) as customer_code,
  channel::varchar(255) as channel,
  franchise::varchar(255) as franchise,
  brand::varchar(255) as brand,
  brand_profit_center::varchar(255) as brand_profit_center,
  promo_type::varchar(255) as promo_type,
  gl_account::varchar(255) as gl_account,
  description::varchar(255) as description,
  requested_date::varchar(255) as requested_date,
  month_of_activity::varchar(255) as month_of_activity,
  promo_start_date::varchar(255) as promo_start_date,
  promo_end_date::varchar(255) as promo_end_date,
  committed_or_accrual_wo_gst::number(17,3) as committed_or_accrual_wo_gst,
  tp_number::varchar(255) as tp_number,
  zp_cmm_invoice::varchar(255) as zp_cmm_invoice,
  retailers_billing::varchar(255) as retailers_billing,
  jnj_actuals_wo_gst::number(17,3) as jnj_actuals_wo_gst,
  month_of_actual::varchar(255) as month_of_actual,
  cn_number::varchar(255) as cn_number,
  cn_date::varchar(255) as cn_date,
  reversal_amount::number(17,3) as reversal_amount,
  outstanding_accrual::number(17,3) as outstanding_accrual,
  jnj_total_committed_wo_gst::number(17,3) as jnj_total_committed_wo_gst,
  jnj_total_unclaimed_wo_gst::number(17,3) as jnj_total_unclaimed_wo_gst,
  comments_or_reversed_accrued_amt::number(17,3) as comments_or_reversed_accrued_amt,
  month_of_reversal::varchar(255) as month_of_reversal,
  supporting::varchar(255) as supporting,
  tp_impact::number(17,3) as tp_impact,
  left_accrual::number(17,3) as left_accrual,
  month_of_doc_scanning::varchar(255) as month_of_doc_scanning,
  remarks::varchar(255) as remarks,
  working_sc::varchar(255) as working_sc,
  conso_pr::varchar(255) as conso_pr,
  create_pr::varchar(255) as create_pr,
  payment_ref::varchar(255) as payment_ref,
  cdl_dttm::varchar(255) as cdl_dttm,
  curr_dt::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::date as updt_dttm,
  run_id::number(14,0) as run_id
  from source
)

--Final select
select * from final
