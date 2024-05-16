with 
wks_tw_ims_dstr_std_customer_107479_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_107479_customer') }}
),
wks_tw_ims_dstr_std_customer_107485_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_107485_customer') }}
),
wks_tw_ims_dstr_std_customer_107501_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_107501_customer') }}
),
wks_tw_ims_dstr_std_customer_107507_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_107507_customer') }}
),
wks_tw_ims_dstr_std_customer_107510_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_107510_customer') }}
),
wks_tw_ims_dstr_std_customer_116047_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_116047_customer') }}
),
wks_tw_ims_dstr_std_customer_120812_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_120812_customer') }}
),
wks_tw_ims_dstr_std_customer_122296_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_122296_customer') }}
),
wks_tw_ims_dstr_std_customer_123291_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_123291_customer') }}
),
wks_tw_ims_dstr_std_customer_131953_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_131953_customer') }}
),
wks_tw_ims_dstr_std_customer_132349_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_132349_customer') }}
),
wks_tw_ims_dstr_std_customer_132508_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_132508_customer') }}
),
wks_tw_ims_dstr_std_customer_135307_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_135307_customer') }}
),
wks_tw_ims_dstr_std_customer_135561_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_135561_customer') }}
),
wks_tw_ims_dstr_std_customer_107482_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_107482_customer') }}
),
wks_tw_ims_dstr_std_customer_107483_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_107483_customer') }}
),
wks_tw_ims_dstr_std_customer_132222_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_132222_customer') }}
),
wks_tw_ims_dstr_std_customer_136454_customer as 
(
    select * from {{ ref('ntawks_integration__wks_tw_ims_dstr_std_customer_136454_customer') }}
),
final as 
(
    select * from wks_tw_ims_dstr_std_customer_107479_customer union all
    select * from wks_tw_ims_dstr_std_customer_107485_customer union all
    select * from wks_tw_ims_dstr_std_customer_107501_customer union all
    select * from wks_tw_ims_dstr_std_customer_107507_customer union all
    select * from wks_tw_ims_dstr_std_customer_107510_customer union all
    select * from wks_tw_ims_dstr_std_customer_116047_customer union all
    select * from wks_tw_ims_dstr_std_customer_120812_customer union all
    select * from wks_tw_ims_dstr_std_customer_122296_customer union all
    select * from wks_tw_ims_dstr_std_customer_123291_customer union all
    select * from wks_tw_ims_dstr_std_customer_131953_customer union all
    select * from wks_tw_ims_dstr_std_customer_132349_customer union all
    select * from wks_tw_ims_dstr_std_customer_132508_customer union all
    select * from wks_tw_ims_dstr_std_customer_135307_customer union all
    select * from wks_tw_ims_dstr_std_customer_135561_customer union all
    select * from wks_tw_ims_dstr_std_customer_107482_customer union all
    select * from wks_tw_ims_dstr_std_customer_107483_customer union all
    select * from wks_tw_ims_dstr_std_customer_132222_customer union all
    select * from wks_tw_ims_dstr_std_customer_136454_customer 
)
select * from final