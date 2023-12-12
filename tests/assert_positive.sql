select *,'Weight_MSL null'
  from {{ ref('EDW_PERFECT_STORE_REBASE_WT_BASE') }}
where weight_msl < 0
