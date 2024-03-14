with sdl_my_afgr as (
    select * from {{ ref('myswks_integration__wks_sdl_my_afgr') }}
),
itg_my_customer_dim as (
    select * from {{ref('mysitg_integration__itg_my_customer_dim') }}
),

transformed as 
(
    select
        imcd.cust_id,
        imcd.cust_nm,
        afgr_num,
        cust_dn_num,
        cast(dn_amt_exc_gst_val as decimal(20, 4)) as dn_amt_exc_gst_val,
        cast(afgr_amt as decimal(20, 4)) as afgr_amt,
        to_date(dt_to_sc) as dt_to_sc,
        sc_validation,
        rtn_ord_num,
        to_date(rtn_ord_dt) as rtn_ord_dt,
        cast(rtn_ord_amt as decimal(20, 4)) as rtn_ord_amt,
        to_date(cn_exp_issue_dt) as cn_exp_issue_dt,
        bill_num,
        to_date(bill_dt) as bill_dt,
        cast(cn_amt as decimal(20, 4)) as cn_amt,
        case
            when not bill_num is null
            then bill_dt
            when bill_num is null and not cn_exp_issue_dt is null
            then cn_exp_issue_dt
            when bill_num is null and cn_exp_issue_dt is null
            then (current_timestamp())
        end as doc_dt,
        case
            when dt_to_sc is null
            then 'Pending SFE'
            when rtn_ord_num is null
            then 'Pending SC'
            when not rtn_ord_num is null and bill_num is null and bill_dt is null
            then 'Return Created'
            when not bill_num is null and not bill_dt is null
            then 'CN Issued'
        end as afgr_status,
        case
            when afgr_status = 'CN Issued'
            then cn_amt
            when afgr_status <> 'CN Issued' and not rtn_ord_num is null
            then rtn_ord_amt
            else afgr_amt
        end as afgr_val,
        (
        afgr_amt - coalesce(cn_amt, 0)
        ) as afgr_cn_diff,
        imcd.chnl as chnl,
        sma.cdl_dttm,
        current_timestamp() as crtd_dttm,
        current_timestamp() as updt_dttm
    from sdl_my_afgr as sma, itg_my_customer_dim as imcd
    where imcd.cust_id = sma.cust_id
),
final as 
(
    select 
        cust_id::varchar(30) as cust_id,
        cust_nm::varchar(100) as cust_nm,
        afgr_num::varchar(30) as afgr_num,
        cust_dn_num::varchar(100) as cust_dn_num,
        dn_amt_exc_gst_val::number(20,4) as dn_amt_exc_gst_val,
        afgr_amt::number(20,4) as afgr_amt,
        dt_to_sc::date as dt_to_sc,
        sc_validation::varchar(30) as sc_validation,
        rtn_ord_num::varchar(30) as rtn_ord_num,
        rtn_ord_dt::date as rtn_ord_dt,
        rtn_ord_amt::number(20,4) as rtn_ord_amt,
        cn_exp_issue_dt::date as cn_exp_issue_dt,
        bill_num::varchar(30) as bill_num,
        bill_dt::date as bill_dt,
        cn_amt::number(20,4) as cn_amt,
        doc_dt::date as doc_dt,
        afgr_status::varchar(30) as afgr_status,
        afgr_val::number(20,4) as afgr_val,
        afgr_cn_diff::number(20,4) as afgr_cn_diff,
        chnl::varchar(30) as chnl,
        cdl_dttm::varchar(30) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final