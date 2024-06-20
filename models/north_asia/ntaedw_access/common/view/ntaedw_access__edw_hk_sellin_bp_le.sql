with source as (
    select * from {{ ref('ntaedw_integration__edw_hk_sellin_bp_le') }}
),
final as (
    select
        subsource_type as "subsource_type",
        fisc_yr_per as "fisc_yr_per",
        mega_brnd_desc as "mega_brnd_desc",
        brnd_desc as "brnd_desc",
        "go to model",
        "banner format",
        "sub channel",
        "parent customer",
        usd_rt as "usd_rt",
        lcl_rt as "lcl_rt",
        tp_acc_start as "tp_acc_start",
        tp_perc as "tp_perc",
        time_gone as "time_gone",
        preww7_act as "preww7_act",
        nts_act as "nts_act",
        tp_act as "tp_act",
        tp_act_calc as "tp_act_calc",
        nts_bp as "nts_bp",
        tp_bp as "tp_bp",
        nts_le as "nts_le",
        tp_le as "tp_le"
    from source
)
select * from final