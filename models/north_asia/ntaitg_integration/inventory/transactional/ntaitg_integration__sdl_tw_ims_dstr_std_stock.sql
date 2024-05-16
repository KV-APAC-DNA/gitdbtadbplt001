with wks_tw_ims_dstr_std_stock_107479_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_107479_stock') }}
),
wks_tw_ims_dstr_std_stock_107482_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_107482_stock') }}
),
wks_tw_ims_dstr_std_stock_107483_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_107483_stock') }}
),
wks_tw_ims_dstr_std_stock_107485_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_107485_stock') }}
),
wks_tw_ims_dstr_std_stock_107501_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_107501_stock') }}
),
wks_tw_ims_dstr_std_stock_107507_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_107507_stock') }}
),
wks_tw_ims_dstr_std_stock_107510_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_107510_stock') }}
),
wks_tw_ims_dstr_std_stock_116047_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_116047_stock') }}
),
wks_tw_ims_dstr_std_stock_120812_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_120812_stock') }}
),
wks_tw_ims_dstr_std_stock_122296_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_122296_stock') }}
),
wks_tw_ims_dstr_std_stock_123291_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_123291_stock') }}
),
wks_tw_ims_dstr_std_stock_131953_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_131953_stock') }}
),
wks_tw_ims_dstr_std_stock_132222_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_132222_stock') }}
),
wks_tw_ims_dstr_std_stock_132349_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_132349_stock') }}
),
wks_tw_ims_dstr_std_stock_132508_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_132508_stock') }}
),
wks_tw_ims_dstr_std_stock_134478_pxstock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_134478_pxstock') }}
),
wks_tw_ims_dstr_std_stock_134478_pxStore as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_134478_pxstore') }}
),
wks_tw_ims_dstr_std_stock_134478_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_134478_stock') }}
),
wks_tw_ims_dstr_std_stock_135307_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_135307_stock') }}
),
wks_tw_ims_dstr_std_stock_135561_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_135561_stock') }}
),
wks_tw_ims_dstr_std_stock_136454_stock as (
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_stock_136454_stock') }}
),
transformed as(
    select * from wks_tw_ims_dstr_std_stock_107479_stock
    union
    select * from wks_tw_ims_dstr_std_stock_107482_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_107483_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_107485_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_107501_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_107507_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_107510_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_116047_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_120812_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_122296_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_123291_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_131953_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_132222_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_132349_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_132508_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_134478_pxstock
    union 
    select * from wks_tw_ims_dstr_std_stock_134478_pxStore
    union 
    select * from wks_tw_ims_dstr_std_stock_134478_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_135307_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_135561_stock
    union 
    select * from wks_tw_ims_dstr_std_stock_136454_stock
)
select * from transformed