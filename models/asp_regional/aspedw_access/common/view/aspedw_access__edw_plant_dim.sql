with source as (
    select * from {{ ref('aspedw_integration__edw_plant_dim') }}
),
final as (
    select
    plnt as "plnt",
plnt_nm as "plnt_nm",
ctry_key as "ctry_key",
src_sys as "src_sys",
prchsng_org as "prchsng_org",
rgn as "rgn",
co_cd as "co_cd",
fctry_cal as "fctry_cal",
mkt_clus as "mkt_clus",
crt_dttm as "crt_dttm",
updt_dttm as "updt_dttm"
from source
)
select * from final 