{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "delete
                    from {{this}} a using {{ ref('pcfwks_integration__wks_perenso_acct_hist_dim') }} wks
                    where wks.acct_id = a.acct_id
                    and   wks.acct_store_code = a.acct_store_code
                    and   a.hist_flg = 'N';"
    )
}}

with source as (
    select * from {{ ref('pcfwks_integration__wks_perenso_acct_hist_dim') }}
),
final as (
    select
    acct_id::number(10,0) as acct_id,
    acct_display_name::varchar(256) as acct_display_name,
    acct_type_desc::varchar(50) as acct_type_desc,
    acct_street_1::varchar(256) as acct_street_1,
    acct_street_2::varchar(256) as acct_street_2,
    acct_street_3::varchar(256) as acct_street_3,
    acct_suburb::varchar(25) as acct_suburb,
    acct_postcode::varchar(25) as acct_postcode,
    acct_phone_number::varchar(50) as acct_phone_number,
    acct_fax_number::varchar(50) as acct_fax_number,
    acct_email::varchar(256) as acct_email,
    acct_country::varchar(256) as acct_country,
    acct_region::varchar(256) as acct_region,
    acct_state::varchar(256) as acct_state,
    acct_banner_country::varchar(256) as acct_banner_country,
    acct_banner_division::varchar(256) as acct_banner_division,
    acct_banner_type::varchar(256) as acct_banner_type,
    acct_banner::varchar(256) as acct_banner,
    acct_type::varchar(256) as acct_type,
    acct_sub_type::varchar(256) as acct_sub_type,
    acct_grade::varchar(256) as acct_grade,
    acct_nz_pharma_country::varchar(256) as acct_nz_pharma_country,
    acct_nz_pharma_state::varchar(256) as acct_nz_pharma_state,
    acct_nz_pharma_territory::varchar(256) as acct_nz_pharma_territory,
    acct_nz_groc_country::varchar(256) as acct_nz_groc_country,
    acct_nz_groc_state::varchar(256) as acct_nz_groc_state,
    acct_nz_groc_territory::varchar(256) as acct_nz_groc_territory,
    acct_ssr_country::varchar(256) as acct_ssr_country,
    acct_ssr_state::varchar(256) as acct_ssr_state,
    acct_ssr_team_leader::varchar(256) as acct_ssr_team_leader,
    acct_ssr_territory::varchar(256) as acct_ssr_territory,
    acct_ssr_sub_territory::varchar(256) as acct_ssr_sub_territory,
    acct_ind_groc_country::varchar(256) as acct_ind_groc_country,
    acct_ind_groc_state::varchar(256) as acct_ind_groc_state,
    acct_ind_groc_territory::varchar(256) as acct_ind_groc_territory,
    acct_ind_groc_sub_territory::varchar(256) as acct_ind_groc_sub_territory,
    acct_au_pharma_country::varchar(256) as acct_au_pharma_country,
    acct_au_pharma_state::varchar(256) as acct_au_pharma_state,
    acct_au_pharma_territory::varchar(256) as acct_au_pharma_territory,
    acct_au_pharma_ssr_country::varchar(256) as acct_au_pharma_ssr_country,
    acct_au_pharma_ssr_state::varchar(256) as acct_au_pharma_ssr_state,
    acct_au_pharma_ssr_territory::varchar(256) as acct_au_pharma_ssr_territory,
    acct_store_code::varchar(256) as acct_store_code,
    acct_fax_opt_out::varchar(256) as acct_fax_opt_out,
    acct_email_opt_out::varchar(256) as acct_email_opt_out,
    acct_contact_method::varchar(256) as acct_contact_method,
    ssr_grade::varchar(256) as ssr_grade,
    start_date::date as start_date,
    end_date::date as end_date,
    hist_flg::varchar(5) as hist_flg,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    upd_dttm::timestamp_ntz(9) as upd_dttm
from source 
)
select * from final