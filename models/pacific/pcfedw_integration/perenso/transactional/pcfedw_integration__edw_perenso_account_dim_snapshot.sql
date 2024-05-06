{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with itg_perenso_account as(
     select * from {{ ref('pcfitg_integration__itg_perenso_account') }}
),
itg_perenso_account_type as(
     select * from {{ ref('pcfitg_integration__itg_perenso_account_type') }}
),
wks_perenso_acct_intermideate as(
     select * from {{ ref('pcfwks_integration__wks_perenso_acct_intermideate') }}
),
itg_perenso_acct_mapping as(
     select * from {{ source('pcfitg_integration', 'itg_perenso_acct_mapping') }}
),
grp as(
        select 
            i.acct_key,
             max(case when upper(m.column_name) = 'ACCT_COUNTRY' then i.grp_desc else null end) as acct_country,
             max(case when upper(m.column_name) = 'ACCT_REGION' then i.grp_desc else null end) as acct_region,
             max(case when upper(m.column_name) = 'ACCT_STATE' then i.grp_desc else null end) as acct_state,
             max(case when upper(m.column_name) = 'ACCT_BANNER_COUNTRY' then i.grp_desc else null end) as acct_banner_country,
             max(case when upper(m.column_name) = 'ACCT_BANNER_DIVISION' then i.grp_desc else null end) as acct_banner_division,
             max(case when upper(m.column_name) = 'ACCT_BANNER_TYPE' then i.grp_desc else null end) as acct_banner_type,
             max(case when upper(m.column_name) = 'ACCT_BANNER' then i.grp_desc else null end) as acct_banner,
             max(case when upper(m.column_name) = 'ACCT_TYPE' then i.grp_desc else null end) as acct_type,
             max(case when upper(m.column_name) = 'ACCT_SUB_TYPE' then i.grp_desc else null end) as acct_sub_type,
             max(case when upper(m.column_name) = 'ACCT_GRADE' then i.grp_desc else null end) as acct_grade,
             max(case when upper(m.column_name) = 'ACCT_NZ_PHARMA_COUNTRY' then i.grp_desc else null end) as acct_nz_pharma_country,
             max(case when upper(m.column_name) = 'ACCT_NZ_PHARMA_STATE' then i.grp_desc else null end) as acct_nz_pharma_state,
             max(case when upper(m.column_name) = 'ACCT_NZ_PHARMA_TERRITORY' then i.grp_desc else null end) as acct_nz_pharma_territory,
             max(case when upper(m.column_name) = 'ACCT_NZ_GROC_COUNTRY' then i.grp_desc else null end) as acct_nz_groc_country,
             max(case when upper(m.column_name) = 'ACCT_NZ_GROC_STATE' then i.grp_desc else null end) as acct_nz_groc_state,
             max(case when upper(m.column_name) = 'ACCT_NZ_GROC_TERRITORY' then i.grp_desc else null end) as acct_nz_groc_territory,
             max(case when upper(m.column_name) = 'ACCT_SSR_COUNTRY' then i.grp_desc else null end) as acct_ssr_country,
             max(case when upper(m.column_name) = 'ACCT_SSR_STATE' then i.grp_desc else null end) as acct_ssr_state,
             max(case when upper(m.column_name) = 'ACCT_SSR_TEAM_LEADER' then i.grp_desc else null end) as acct_ssr_team_leader,
             max(case when upper(m.column_name) = 'ACCT_SSR_TERRITORY' then i.grp_desc else null end) as acct_ssr_territory,
             max(case when upper(m.column_name) = 'ACCT_SSR_SUB_TERRITORY' then i.grp_desc else null end) as acct_ssr_sub_territory,
             max(case when upper(m.column_name) = 'ACCT_IND_GROC_COUNTRY' then i.grp_desc else null end) as acct_ind_groc_country,
             max(case when upper(m.column_name) = 'ACCT_IND_GROC_STATE' then i.grp_desc else null end) as acct_ind_groc_state,
             max(case when upper(m.column_name) = 'ACCT_IND_GROC_TERRITORY' then i.grp_desc else null end) as acct_ind_groc_territory,
             max(case when upper(m.column_name) = 'ACCT_IND_GROC_SUB_TERRITORY' then i.grp_desc else null end) as acct_ind_groc_sub_territory,
             max(case when upper(m.column_name) = 'ACCT_AU_PHARMA_COUNTRY' then i.grp_desc else null end) as acct_au_pharma_country,
             max(case when upper(m.column_name) = 'ACCT_AU_PHARMA_STATE' then i.grp_desc else null end) as acct_au_pharma_state,
             max(case when upper(m.column_name) = 'ACCT_AU_PHARMA_TERRITORY' then i.grp_desc else null end) as acct_au_pharma_territory,
             max(case when upper(m.column_name) = 'ACCT_AU_PHARMA_SSR_COUNTRY' then i.grp_desc else null end) as acct_au_pharma_ssr_country,
             max(case when upper(m.column_name) = 'ACCT_AU_PHARMA_SSR_STATE' then i.grp_desc else null end) as acct_au_pharma_ssr_state,
             max(case when upper(m.column_name) = 'ACCT_AU_PHARMA_SSR_TERRITORY' then i.grp_desc else null end) as acct_au_pharma_ssr_territory,
             max(case when upper(m.column_name) = 'ACCT_STORE_CODE' then i.grp_desc else null end) as acct_store_code,
			 max(case when upper(m.column_name) = 'ACCT_FAX_OPT_OUT' then i.grp_desc else null end) as acct_fax_opt_out,
			 max(case when upper(m.column_name) = 'ACCT_EMAIL_OPT_OUT' then i.grp_desc else null end) as acct_email_opt_out,
			 max(case when upper(m.column_name) = 'ACCT_CONTACT_METHOD' then i.grp_desc else null end) as acct_contact_method
      from itg_perenso_acct_mapping m,
           wks_perenso_acct_intermideate i
      where m.field_key = i.field_key
      and   m.acct_grp_lev_key = i.acct_grp_lev_key
      group by i.acct_key
),
transformed as
(
     select 
       grp.acct_key::number(10,0) as acct_id,
       ipa.disp_name::varchar(256) as acct_display_name,
       ipat.acct_type_desc::varchar(50) as acct_type_desc,
       ipa.acct_street1::varchar(256) as acct_street_1,
       ipa.acct_street2::varchar(256) as acct_street_2,
       ipa.acct_street3::varchar(256) as acct_street_3,
       ipa.acct_suburb::varchar(255) as acct_suburb,
       ipa.acct_postcode::varchar(255) as acct_postcode,
       ipa.acct_phone::varchar(255) as acct_phone_number,
       ipa.acct_fax::varchar(50) as acct_fax_number,
       ipa.acct_email::varchar(256) as acct_email,
       coalesce(grp.acct_country,'Not Assigned')::varchar(256) as acct_country,
       coalesce(grp.acct_region,'Not Assigned')::varchar(256) as acct_region,
       coalesce(grp.acct_state,'Not Assigned')::varchar(256) as acct_state,
       coalesce(grp.acct_banner_country,'Not Assigned')::varchar(256) as acct_banner_country,
       coalesce(grp.acct_banner_division,'Not Assigned')::varchar(256) as acct_banner_division,
       coalesce(grp.acct_banner_type,'Not Assigned')::varchar(256) as acct_banner_type,
       coalesce(grp.acct_banner,'Not Assigned')::varchar(256) as acct_banner,
       coalesce(grp.acct_type,'Not Assigned')::varchar(256) as acct_type,
       coalesce(grp.acct_sub_type,'Not Assigned')::varchar(256) as acct_sub_type,
       coalesce(grp.acct_grade,'Not Assigned')::varchar(256) as acct_grade,
       coalesce(grp.acct_nz_pharma_country,'Not Assigned')::varchar(256) as acct_nz_pharma_country,
       coalesce(grp.acct_nz_pharma_state,'Not Assigned')::varchar(256) as acct_nz_pharma_state,
       coalesce(grp.acct_nz_pharma_territory,'Not Assigned')::varchar(256) as acct_nz_pharma_territory,
       coalesce(grp.acct_nz_groc_country,'Not Assigned')::varchar(256) as acct_nz_groc_country,
       coalesce(grp.acct_nz_groc_state,'Not Assigned')::varchar(256) as acct_nz_groc_state,
       coalesce(grp.acct_nz_groc_territory,'Not Assigned')::varchar(256) as acct_nz_groc_territory,
       coalesce(grp.acct_ssr_country,'Not Assigned')::varchar(256) as acct_ssr_country,
       coalesce(grp.acct_ssr_state,'Not Assigned')::varchar(256) as acct_ssr_state,
       coalesce(grp.acct_ssr_team_leader,'Not Assigned')::varchar(256) as acct_ssr_team_leader,
       coalesce(grp.acct_ssr_territory,'Not Assigned')::varchar(256) as acct_ssr_territory,
       coalesce(grp.acct_ssr_sub_territory,'Not Assigned')::varchar(256) as acct_ssr_sub_territory,
       coalesce(grp.acct_ind_groc_country,'Not Assigned')::varchar(256) as acct_ind_groc_country,
       coalesce(grp.acct_ind_groc_state,'Not Assigned')::varchar(256) as acct_ind_groc_state,
       coalesce(grp.acct_ind_groc_territory,'Not Assigned')::varchar(256) as acct_ind_groc_territory,
       coalesce(grp.acct_ind_groc_sub_territory,'Not Assigned')::varchar(256) as acct_ind_groc_sub_territory,
       coalesce(grp.acct_au_pharma_country,'Not Assigned')::varchar(256) as acct_au_pharma_country,
       coalesce(grp.acct_au_pharma_state,'Not Assigned')::varchar(256) as acct_au_pharma_state,
       coalesce(grp.acct_au_pharma_territory,'Not Assigned')::varchar(256) as acct_au_pharma_territory,
       coalesce(grp.acct_au_pharma_ssr_country,'Not Assigned')::varchar(256) as acct_au_pharma_ssr_country,
       coalesce(grp.acct_au_pharma_ssr_state,'Not Assigned')::varchar(256) as acct_au_pharma_ssr_state,
       coalesce(grp.acct_au_pharma_ssr_territory,'Not Assigned')::varchar(256) as acct_au_pharma_ssr_territory,
       coalesce(grp.acct_store_code,'Not Assigned')::varchar(256) as acct_store_code,
	   coalesce(grp.acct_fax_opt_out,'Not Assigned')::varchar(256) as acct_fax_opt_out,
	   coalesce(grp.acct_email_opt_out,'Not Assigned')::varchar(256) as acct_email_opt_out,
	   coalesce(grp.acct_contact_method,'Not Assigned')::varchar(256) as acct_contact_method,
	   (extract(year from current_timestamp())||lpad(extract(month from current_timestamp()),2,0))::varchar(30) as snapshot_mnth,
       current_timestamp()::timestamp_ntz(9) as snapshot_dt
    from grp,
        itg_perenso_account ipa,
        itg_perenso_account_type ipat
    where grp.acct_key = ipa.acct_key
    and   ipa.acct_type_key = ipat.acct_type_key
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and snapshot_dt::date > (select max(snapshot_dt)::date from {{ this }}) 
    {% endif %}
)
select * from transformed
