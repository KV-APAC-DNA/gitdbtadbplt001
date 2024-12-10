{{ config(
   materialized='incremental',
  incremental_strategy='append',
  transient=false,
  post_hook="{{ get_harmonized_brand('TRADITIONAL_COMPETITIVE_TV_GRP_SPENDS', 'Y') }}"
) }}

select * 
from {{ source(
      "indsdl_raw",
      "sdl_lidar_ff_tv_grp_spends"
    ) }}