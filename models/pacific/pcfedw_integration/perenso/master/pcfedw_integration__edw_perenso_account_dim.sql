with
itg_perenso_acct_mapping as
(
    select * from snappcfitg_integration.itg_perenso_acct_mapping
), --one time load
wks_perenso_acct_intermideate as
(
    select * from snappcfwks_integration.wks_perenso_acct_intermideate
),
itg_perenso_account  as
(
    select * from snappcfitg_integration.itg_perenso_account
),
itg_perenso_account_type as
(
    select * from snappcfitg_integration.itg_perenso_account_type
),
grp as
(SELECT 
    I.ACCT_KEY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_COUNTRY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_COUNTRY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_REGION' THEN I.GRP_DESC ELSE NULL END) AS ACCT_REGION,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_STATE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_STATE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_BANNER_COUNTRY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_BANNER_COUNTRY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_BANNER_DIVISION' THEN I.GRP_DESC ELSE NULL END) AS ACCT_BANNER_DIVISION,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_BANNER_TYPE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_BANNER_TYPE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_BANNER' THEN I.GRP_DESC ELSE NULL END) AS ACCT_BANNER,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_TYPE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_TYPE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_SUB_TYPE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_SUB_TYPE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_GRADE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_GRADE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_NZ_PHARMA_COUNTRY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_NZ_PHARMA_COUNTRY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_NZ_PHARMA_STATE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_NZ_PHARMA_STATE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_NZ_PHARMA_TERRITORY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_NZ_PHARMA_TERRITORY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_NZ_GROC_COUNTRY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_NZ_GROC_COUNTRY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_NZ_GROC_STATE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_NZ_GROC_STATE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_NZ_GROC_TERRITORY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_NZ_GROC_TERRITORY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_SSR_COUNTRY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_SSR_COUNTRY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_SSR_STATE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_SSR_STATE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_SSR_TEAM_LEADER' THEN I.GRP_DESC ELSE NULL END) AS ACCT_SSR_TEAM_LEADER,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_SSR_TERRITORY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_SSR_TERRITORY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_SSR_SUB_TERRITORY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_SSR_SUB_TERRITORY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_IND_GROC_COUNTRY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_IND_GROC_COUNTRY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_IND_GROC_STATE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_IND_GROC_STATE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_IND_GROC_TERRITORY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_IND_GROC_TERRITORY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_IND_GROC_SUB_TERRITORY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_IND_GROC_SUB_TERRITORY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_AU_PHARMA_COUNTRY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_AU_PHARMA_COUNTRY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_AU_PHARMA_STATE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_AU_PHARMA_STATE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_AU_PHARMA_TERRITORY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_AU_PHARMA_TERRITORY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_AU_PHARMA_SSR_COUNTRY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_AU_PHARMA_SSR_COUNTRY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_AU_PHARMA_SSR_STATE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_AU_PHARMA_SSR_STATE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_AU_PHARMA_SSR_TERRITORY' THEN I.GRP_DESC ELSE NULL END) AS ACCT_AU_PHARMA_SSR_TERRITORY,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_STORE_CODE' THEN I.GRP_DESC ELSE NULL END) AS ACCT_STORE_CODE,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_FAX_OPT_OUT' THEN I.GRP_DESC ELSE NULL END) AS ACCT_FAX_OPT_OUT,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_EMAIL_OPT_OUT' THEN I.GRP_DESC ELSE NULL END) AS ACCT_EMAIL_OPT_OUT,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'ACCT_CONTACT_METHOD' THEN I.GRP_DESC ELSE NULL END) AS ACCT_CONTACT_METHOD,
    MAX(CASE WHEN UPPER(M.COLUMN_NAME) = 'SSR_GRADE' THEN I.GRP_DESC ELSE NULL END) AS SSR_GRADE
    from itg_perenso_acct_mapping m,
        wks_perenso_acct_intermideate i
    where m.field_key = i.field_key
    and   m.acct_grp_lev_key = i.acct_grp_lev_key
    group by i.acct_key
),
trans as 
(select 
       grp.acct_key as acct_id,
       ipa.disp_name as acct_display_name,
       ipat.acct_type_desc as acct_type_desc,
       ipa.acct_street1 as acct_street_1,
       ipa.acct_street2 as acct_street_2,
       ipa.acct_street3 as acct_street_3,
       ipa.acct_suburb as acct_suburb,
       ipa.acct_postcode as acct_postcode,
       ipa.acct_phone as acct_phone_number,
       ipa.acct_fax as acct_fax_number,
       ipa.acct_email as acct_email,
       COALESCE(GRP.ACCT_COUNTRY,'Not Assigned') AS ACCT_COUNTRY,
       COALESCE(GRP.ACCT_REGION,'Not Assigned') AS ACCT_REGION,
       COALESCE(GRP.ACCT_STATE,'Not Assigned') AS ACCT_STATE,
       COALESCE(GRP.ACCT_BANNER_COUNTRY,'Not Assigned') AS ACCT_BANNER_COUNTRY,
       COALESCE(GRP.ACCT_BANNER_DIVISION,'Not Assigned') AS ACCT_BANNER_DIVISION,
       COALESCE(GRP.ACCT_BANNER_TYPE,'Not Assigned') AS ACCT_BANNER_TYPE,
       COALESCE(GRP.ACCT_BANNER,'Not Assigned') AS ACCT_BANNER,
       COALESCE(GRP.ACCT_TYPE,'Not Assigned') AS ACCT_TYPE,
       COALESCE(GRP.ACCT_SUB_TYPE,'Not Assigned') AS ACCT_SUB_TYPE,
       COALESCE(GRP.ACCT_GRADE,'Not Assigned') AS ACCT_GRADE,
       COALESCE(GRP.ACCT_NZ_PHARMA_COUNTRY,'Not Assigned') AS ACCT_NZ_PHARMA_COUNTRY,
       COALESCE(GRP.ACCT_NZ_PHARMA_STATE,'Not Assigned') AS ACCT_NZ_PHARMA_STATE,
       COALESCE(GRP.ACCT_NZ_PHARMA_TERRITORY,'Not Assigned') AS ACCT_NZ_PHARMA_TERRITORY,
       COALESCE(GRP.ACCT_NZ_GROC_COUNTRY,'Not Assigned') AS ACCT_NZ_GROC_COUNTRY,
       COALESCE(GRP.ACCT_NZ_GROC_STATE,'Not Assigned') AS ACCT_NZ_GROC_STATE,
       COALESCE(GRP.ACCT_NZ_GROC_TERRITORY,'Not Assigned') AS ACCT_NZ_GROC_TERRITORY,
       COALESCE(GRP.ACCT_SSR_COUNTRY,'Not Assigned') AS ACCT_SSR_COUNTRY,
       COALESCE(GRP.ACCT_SSR_STATE,'Not Assigned') AS ACCT_SSR_STATE,
       COALESCE(GRP.ACCT_SSR_TEAM_LEADER,'Not Assigned') AS ACCT_SSR_TEAM_LEADER,
       COALESCE(GRP.ACCT_SSR_TERRITORY,'Not Assigned') AS ACCT_SSR_TERRITORY,
       COALESCE(GRP.ACCT_SSR_SUB_TERRITORY,'Not Assigned') AS ACCT_SSR_SUB_TERRITORY,
       COALESCE(GRP.ACCT_IND_GROC_COUNTRY,'Not Assigned') AS ACCT_IND_GROC_COUNTRY,
       COALESCE(GRP.ACCT_IND_GROC_STATE,'Not Assigned') AS ACCT_IND_GROC_STATE,
       COALESCE(GRP.ACCT_IND_GROC_TERRITORY,'Not Assigned') AS ACCT_IND_GROC_TERRITORY,
       COALESCE(GRP.ACCT_IND_GROC_SUB_TERRITORY,'Not Assigned') AS ACCT_IND_GROC_SUB_TERRITORY,
       COALESCE(GRP.ACCT_AU_PHARMA_COUNTRY,'Not Assigned') AS ACCT_AU_PHARMA_COUNTRY,
       COALESCE(GRP.ACCT_AU_PHARMA_STATE,'Not Assigned') AS ACCT_AU_PHARMA_STATE,
       COALESCE(GRP.ACCT_AU_PHARMA_TERRITORY,'Not Assigned') AS ACCT_AU_PHARMA_TERRITORY,
       COALESCE(GRP.ACCT_AU_PHARMA_SSR_COUNTRY,'Not Assigned') AS ACCT_AU_PHARMA_SSR_COUNTRY,
       COALESCE(GRP.ACCT_AU_PHARMA_SSR_STATE,'Not Assigned') AS ACCT_AU_PHARMA_SSR_STATE,
       COALESCE(GRP.ACCT_AU_PHARMA_SSR_TERRITORY,'Not Assigned') AS ACCT_AU_PHARMA_SSR_TERRITORY,
       COALESCE(GRP.ACCT_STORE_CODE,'Not Assigned') AS ACCT_STORE_CODE,
	   COALESCE(GRP.ACCT_FAX_OPT_OUT,'Not Assigned') AS ACCT_FAX_OPT_OUT,
	   COALESCE(GRP.ACCT_EMAIL_OPT_OUT,'Not Assigned') AS ACCT_EMAIL_OPT_OUT,
	   COALESCE(GRP.ACCT_CONTACT_METHOD,'Not Assigned') AS ACCT_CONTACT_METHOD,
	   COALESCE(GRP.SSR_GRADE,'Not Assigned') AS SSR_GRADE
FROM grp,
     itg_perenso_account ipa,
     itg_perenso_account_type ipat
where grp.acct_key = ipa.acct_key
and   ipa.acct_type_key = ipat.acct_type_key
),
final as 
(
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
	ssr_grade::varchar(256) as ssr_grade
from trans
)
select * from final