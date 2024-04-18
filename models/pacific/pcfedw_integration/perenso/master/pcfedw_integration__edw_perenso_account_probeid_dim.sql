with
edw_perenso_account_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.edw_perenso_account_dim
),
itg_perenso_account_reln_id as (
select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.itg_perenso_account_reln_id 
),
itg_perenso_account_fields as (
select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.itg_perenso_account_fields
),
transformed as (
select edw.acct_id,

       edw.acct_display_name,

       edw.acct_type_desc,

       edw.acct_street_1,

       edw.acct_street_2,

       edw.acct_street_3,

       edw.acct_suburb,

       edw.acct_postcode,

       edw.acct_phone_number,

       edw.acct_fax_number,

       edw.acct_email,

       edw.acct_country,

       edw.acct_region,

       edw.acct_state,

       edw.acct_banner_country,

       edw.acct_banner_division,

       edw.acct_banner_type,

       edw.acct_banner,

       edw.acct_type,

       edw.acct_sub_type,

       edw.acct_grade,

       edw.acct_nz_pharma_country,

       edw.acct_nz_pharma_state,

       edw.acct_nz_pharma_territory,

       edw.acct_nz_groc_country,

       edw.acct_nz_groc_state,

       edw.acct_nz_groc_territory,

       edw.acct_ssr_country,

       edw.acct_ssr_state,

       edw.acct_ssr_team_leader,

       edw.acct_ssr_territory,

       edw.acct_ssr_sub_territory,

       edw.acct_ind_groc_country,

       edw.acct_ind_groc_state,

       edw.acct_ind_groc_territory,

       edw.acct_ind_groc_sub_territory,

       edw.acct_au_pharma_country,

       edw.acct_au_pharma_state,

       edw.acct_au_pharma_territory,

       edw.acct_au_pharma_ssr_country,

       edw.acct_au_pharma_ssr_state,

       edw.acct_au_pharma_ssr_territory,

       reln.id as account_probe_id

from edw_perenso_account_dim edw,

     itg_perenso_account_reln_id reln,

     itg_perenso_account_fields fields

where edw.acct_id = reln.acct_key

and   reln.field_key = fields.field_key

and   upper(fields.field_desc) = 'IMS PROBE'
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
account_probe_id::varchar(100) as account_probe_id
-- acct_store_code::varchar(256) as acct_store_code,
-- acct_fax_opt_out::varchar(256) as acct_fax_opt_out,
-- acct_email_opt_out::varchar(256) as acct_email_opt_out,
-- acct_contact_method::varchar(256) as acct_contact_method
from transformed
)
select * from final