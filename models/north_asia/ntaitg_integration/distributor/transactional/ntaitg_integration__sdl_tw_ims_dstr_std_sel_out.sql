with wks_sdl_tw_ims_dstr_std_sel_out_107479_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_107479_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_107482_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_107482_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_107483_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_107483_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_107485_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_107485_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_107501_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_107501_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_107507_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_107507_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_107510_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_107510_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_116047_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_116047_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_120812_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_120812_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_122296_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_122296_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_123291_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_123291_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_131953_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_131953_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_132222_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_132222_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_132349_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_132349_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_132508_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_132508_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_134478_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_134478_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_135307_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_135307_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_135561_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_135561_sellout') }}
),
wks_sdl_tw_ims_dstr_std_sel_out_136454_sellout as 
(
    select * from {{ ref('ntawks_integration__wks_sdl_tw_ims_dstr_std_sel_out_136454_sellout') }}
),
final as 
(
    select * from wks_sdl_tw_ims_dstr_std_sel_out_107479_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_107482_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_107483_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_107485_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_107501_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_107507_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_107510_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_116047_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_120812_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_122296_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_123291_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_131953_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_132222_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_132349_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_132508_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_134478_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_135307_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_135561_sellout
    union all
    select * from wks_sdl_tw_ims_dstr_std_sel_out_136454_sellout
)
select * from final