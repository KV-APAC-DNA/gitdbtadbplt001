{{
    config(
        materialized="incremental",
        incremental_strategy="append"
)}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_kr_dads_coupang_search_keyword') }} 
),
final as (
SELECT 
By_search_term_ranking
,Product_Ranking_Criteria
,Category_1
,Category_2
,Category_3
,ranking
,Query
,Product_standings
,goods
,My_Products,
null as file_name,
file_date as file_date,
crtd_dttm as crtd_dttm
from source
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
{% endif %}
)
select * from final