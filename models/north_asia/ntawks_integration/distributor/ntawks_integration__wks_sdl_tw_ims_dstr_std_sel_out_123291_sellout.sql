with source as 
(
    select * from {{ source('ntasdl_raw', 'sdl_tw_ims_dstr_std_sel_out_123291_sellout') }}
)
select * from source