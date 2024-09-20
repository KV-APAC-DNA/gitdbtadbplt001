with source as 
(
    select * from {{ source('ntasdl_raw', 'sdl_tw_ims_dstr_std_sel_out_135307_sellout') }}
    where file_name not in (
        select distinct file_name from 
        {{ source('ntawks_integration', 'TRATBL_sdl_tw_ims_dstr_std_sel_out_135307_sellout__null_test') }}
        union all
        select distinct file_name from 
        {{ source('ntawks_integration', 'TRATBL_sdl_tw_ims_dstr_std_sel_out_135307_sellout__test_date_format_odd_eve_leap') }}
    )
)
select * from source