with itg_perenso_account_group_reln as(
    select * from {{ ref('pcfitg_integration__itg_perenso_account_group_reln') }}
),
itg_perenso_account as(
    select * from {{ ref('pcfitg_integration__itg_perenso_account') }}
), 
itg_perenso_account_fields as(
    select * from {{ ref('pcfitg_integration__itg_perenso_account_fields') }}
), 
itg_perenso_account_type as(
    select * from {{ ref('pcfitg_integration__itg_perenso_account_type') }}
), 
itg_perenso_account_group_lvl as(
    select * from {{ ref('pcfitg_integration__itg_perenso_account_group_lvl') }}
), 
itg_perenso_account_group as(
    select * from {{ ref('pcfitg_integration__itg_perenso_account_group') }}
), 
itg_perenso_account_reln_id as(
    select * from {{ ref('pcfitg_integration__itg_perenso_account_reln_id') }}
), 
itg_perenso_account_custom_list as(
    select * from {{ ref('pcfitg_integration__itg_perenso_account_custom_list') }}
), 
union1 as(
    select
        ipa.acct_key,
        ipagr.acct_grp_key,
        ipag.grp_desc,
        ipag.acct_grp_lev_key,
        ipagl.acct_lev_desc,
        ipagl.field_key,
        ipaf.field_desc
    from itg_perenso_account as ipa, itg_perenso_account_type as ipat, itg_perenso_account_group_reln as ipagr, itg_perenso_account_group as ipag, itg_perenso_account_group_lvl as ipagl, itg_perenso_account_fields as ipaf
    where
    ipa.acct_type_key = ipat.acct_type_key
    and ipa.acct_key = ipagr.acct_key
    and ipagr.acct_grp_key = ipag.acct_grp_key
    and ipag.acct_grp_lev_key = ipagl.acct_grp_lev_key
    and ipagl.field_key = ipaf.field_key
),
union2 as(
    select
        acct_key,
        acct_grp_key,
        grp_desc,
        acct_grp_lev_key,
        acct_lev_desc,
        field_key,
        field_desc
    from (
        select
        ipa.acct_key,
        try_cast('999999' as decimal) as acct_grp_key,
        ipari.id as grp_desc,
        try_cast('999999' as decimal) as acct_grp_lev_key,
        null as acct_lev_desc,
        ipari.field_key,
        ipaf.field_desc,
        count(ipa.acct_key) over (partition by ipa.acct_key) as cnt
        from itg_perenso_account as ipa, itg_perenso_account_type as ipat, itg_perenso_account_fields as ipaf, itg_perenso_account_reln_id as ipari
        where
        ipa.acct_type_key = ipat.acct_type_key
        and ipaf.acct_type_key = ipat.acct_type_key
        and ipari.field_key = ipaf.field_key(+)
        and ipa.acct_key = ipari.acct_key(+)
        and upper(ipaf.field_desc) = 'STORE CODE'
    )
    where
        cnt <> '2'
),
union3 as(
    select
        acct_key,
        acct_grp_key,
        grp_desc,
        acct_grp_lev_key,
        acct_lev_desc,
        field_key,
        field_desc
    from (
        select
        ipa.acct_key,
        try_cast('999999' as decimal) as acct_grp_key,
        ipari.option_desc as grp_desc,
        try_cast('999999' as decimal) as acct_grp_lev_key,
        null as acct_lev_desc,
        ipari.field_key,
        ipaf.field_desc,
        count(ipa.acct_key) over (partition by ipa.acct_key, ipari.field_key) as cnt
        from itg_perenso_account as ipa, itg_perenso_account_type as ipat, itg_perenso_account_fields as ipaf, itg_perenso_account_custom_list as ipari
        where
        ipa.acct_type_key = ipat.acct_type_key
        and ipaf.acct_type_key = ipat.acct_type_key
        and ipari.field_key = ipaf.field_key(+)
        and ipa.acct_key = ipari.acct_key
    )
    where
        cnt <> '2'
),
final as(
    select distinct
        acct_key,
        field_key,
        acct_grp_lev_key,
        acct_grp_key,
        field_desc,
        acct_lev_desc,
        grp_desc
    from (
        select * from union1
        union
        select * from union2
        union
        select * from union3
    )
)
select * from final