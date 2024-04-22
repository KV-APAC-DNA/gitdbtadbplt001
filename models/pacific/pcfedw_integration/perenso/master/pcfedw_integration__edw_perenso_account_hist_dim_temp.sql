{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{{build_edw_perenso_account_hist_dim_temp()}}"
    )
}}
select * from {{this}}