with source as (
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_mt_account_key') }}
),
final as( 
    select
        code::varchar(255) as accounting_code,
        key_figure_number::number(20,3) as "key_figure_#",
        name::varchar(100) as key_figure_nm,
        key_figure_name_kanji::varchar(100) as key_figure_nm_knj,
        key_figure_name_dsp::varchar(100) as key_figure_nm_dsp, 
        csw_category_1::varchar(255) as csw_category_1,
        csw_sort_key::number(20,3) as csw_sort_key,
        sign_of_number::number(20,3) as sign_of_number,
        reference_table::varchar(255) as reference_table,
        reference_column::varchar(255) as reference_column,
        reference_field::varchar(255) as reference_field,
        field_condition::varchar(255) as field_condition,
        delete_flag::varchar(1) as delete_flag,
        current_timestamp()::timestamp_ntz(9) as update_dt,
        NULL::varchar(30) as update_user
    from source
)


select * from final
