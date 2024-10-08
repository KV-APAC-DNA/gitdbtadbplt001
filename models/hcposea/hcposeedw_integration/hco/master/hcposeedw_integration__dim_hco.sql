{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = ["{% if is_incremental() %}
                delete from {{this}}
                where hco_key in (select hco_key from {{ ref('hcposeitg_integration__itg_account_hco') }} itg_hco  where itg_hco.hco_key = hco_key);
                {% endif %}",
                "{% if is_incremental() %}
                delete from {{this}} where hco_key ='Not Applicable';
                {% endif %}"]
    )
}}


WITH 
itg_lookup_eng_data as (
select * from {{ source('hcposeitg_integration', 'itg_lookup_eng_data') }}
),
itg_account_hco as (
select * from {{ ref('hcposeitg_integration__itg_account_hco') }}
),
itg_recordtype as (
select * from {{ ref('hcposeitg_integration__itg_recordtype') }}
),
itg_address as (
select * from {{ ref('hcposeitg_integration__itg_address') }}
),
HCO_TYP_ENG AS
(SELECT COUNTRY_CODE, KEY_VALUE, TARGET_VALUE
FROM itg_lookup_eng_data
WHERE UPPER(TABLE_NAME) ='DIM_HCO'
AND UPPER(COLUMN_NAME) = 'HCO_TYPE'
AND UPPER(TARGET_COLUMN_NAME) = 'HCO_TYPE_ENGLISH'
),
cte1 as (
select distinct
hco_key as hco_key,
itg_hco.country_code as country_code,
itg_hco.account_source_id as hco_source_id,
nvl(itg_hco.last_modified_by_id,'Not Applicable') as modify_id,
itg_hco.last_modified_date as modify_dt,
'Not Applicable' as hco_business_id ,
nvl(itg_hco.inactive,0) as inactive_flag,
'Not Applicable' as inactive_reason,
account_name as hco_name,
hco_type as hco_type,
nvl(target_value,hco_type) as hco_type_english_name,
hco_sector as sector,
phone_number as phone_number,
fax_number as fax_number,
website as website,
territory_name as territory_name,
hco_name as formatted_name,
remarks as remarks,
is_excluded_from_realign as exclude_from_territory_assignment_rules,
beds as beds,
total_mds_dos as total_mds_dos,
departments as departments,
sfe_approved as sfe_approved_flag,
business_description as business_description,
hcc_account_approved as hcc_account_approved,
total_physicians_enrolled as total_physicians_enrolled,
total_pharmacists as total_pharmacists,
is_external_id_number as is_external_id_number,
ot as ot,
kam_paediatric as kam_paediatric,
kam_obgyn as kam_obgyn,
kam_minimally_invasive as kam_minimally_invasive,
kam_total_urologysurgeons as kam_total_urologysurgeons,
kam_total_surgeons as kam_total_surgeons,
kam_total_rheumphysicians as kam_total_rheumphysicians,
kam_total_psychiatryphysicians as kam_total_psychiatryphysicians,
kam_total_orthosurgeons as kam_total_orthosurgeons,
kam_total_opthalsurgeons as kam_total_opthalsurgeons,
kam_total_neurologyphysicians as kam_total_neurologyphysicians,
kam_total_medoncophysicians as kam_total_medoncophysicians,
kam_total_infectiousphysicians as kam_total_infectiousphysicians,
kam_total_haemaphysicians as kam_total_haemaphysicians,
kam_total_generalsurgeons as kam_total_generalsurgeons,
kam_total_gastrophysicians as kam_total_gastrophysicians,
kam_total_endophysicians as kam_total_endophysicians,
kam_total_dermaphysicians as kam_total_dermaphysicians,
kam_total_cardiologyphysicians as kam_total_cardiologyphysicians,
kam_total_cardiosurgeons as kam_total_cardiosurgeons,
kam_total_aestheticsurgeons as kam_total_aestheticsurgeons,
kam_general_differnentiations as kam_general_differnentiations,
kam_clinical_differentiations as kam_clinical_differentiations,
itg_rec.record_type_name as record_type_name,
nvl(itg_hco.external_id,'Not Applicable') as hco_external_id,
parent_hco_key as parent_hco_key,
parent_hco_name as parent_hco_name,
itg_hco.is_deleted as deleted_flag,
address_name as address_line_1,
address_line2_name as address_line_2,
suburb_town as suburb_town,
city as city,
state as state,
zip as postcode,
brick as brick,
map as map,
nvl(address_source_id,'Not Applicable') as hco_address_source_id,
appt_required as appt_required_flag,
itg_add.external_id as address_external_id,
phone as address_phone,
fax as address_fax,
itg_add.is_primary as primary_flag,
itg_add.inactive as address_inactive_flag,
controlling_address as controlling_address,
veeva_autogen_id as veeva_autogen_id,
customer_code as customer_code,
current_timestamp() as inserted_date,
current_timestamp() as updated_date
FROM itg_account_hco ITG_HCO
  JOIN itg_recordtype ITG_REC ON ITG_HCO.RECORD_TYPE_SOURCE_ID = ITG_REC.RECORD_TYPE_SOURCE_ID
  AND ITG_HCO.COUNTRY_CODE = ITG_REC.COUNTRY_CODE
  LEFT OUTER JOIN ( select * from itg_address 
                      where address_source_id in ( select max(address_source_id) from itg_address where is_primary =1 group by account_source_id ) 
                      or address_source_id in ( select max(address_source_id) from itg_address where is_primary =0 
and account_source_id not in ( select account_source_id from itg_address where is_primary =1)  group by account_source_id)
                   )as ITG_ADD ON ITG_ADD.ACCOUNT_SOURCE_ID = decode(ITG_HCO.PRIMARY_PARENT_SOURCE_ID,NULL,ITG_HCO.ACCOUNT_SOURCE_ID,' ',
                   ITG_HCO.ACCOUNT_SOURCE_ID,ITG_HCO.PRIMARY_PARENT_SOURCE_ID )

  AND ITG_HCO.COUNTRY_CODE = ITG_ADD.COUNTRY_CODE

  LEFT OUTER JOIN HCO_TYP_ENG ON ITG_HCO.HCO_TYPE = HCO_TYP_ENG.KEY_VALUE
  AND ITG_HCO.COUNTRY_CODE = HCO_TYP_ENG.COUNTRY_CODE)
,
cte2 as (
SELECT 'Not Applicable',
       'ZZ',
       'Not Applicable',
       'Not Applicable',
       current_timestamp(),
       'Not Applicable',
       0,
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       0,
       0,
       0,
       0,
       'Not Applicable',
       0,
      0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       0,
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       current_timestamp(),
       current_timestamp()
),
transformed as (
select * from cte1 
union all
select * from cte2
),
final as (
select
hco_key::varchar(32) as hco_key,
country_code::varchar(8) as country_code,
hco_source_id::varchar(18) as hco_source_id,
modify_id::varchar(18) as modify_id,
modify_dt::timestamp_ntz(9) as modify_dt,
hco_business_id::varchar(123) as hco_business_id,
inactive_flag::number(38,0) as inactive_flag,
inactive_reason::varchar(255) as inactive_reason,
hco_name::varchar(1300) as hco_name,
hco_type::varchar(255) as hco_type,
hco_type_english_name::varchar(255) as hco_type_english_name,
sector::varchar(255) as sector,
phone_number::varchar(40) as phone_number,
fax_number::varchar(40) as fax_number,
website::varchar(255) as website,
territory_name::varchar(255) as territory_name,
formatted_name::varchar(1300) as formatted_name,
remarks::varchar(255) as remarks,
exclude_from_territory_assignment_rules::number(38,0) as exclude_from_territory_assignment_rules,
beds::number(8,0) as beds,
total_mds_dos::number(18,0) as total_mds_dos,
departments::number(18,0) as departments,
sfe_approved_flag::number(38,0) as sfe_approved_flag,
business_description::varchar(32330) as business_description,
hcc_account_approved::number(38,0) as hcc_account_approved,
total_physicians_enrolled::number(18,0) as total_physicians_enrolled,
total_pharmacists::number(3,0) as total_pharmacists,
is_external_id_number::number(38,0) as is_external_id_number,
ot::number(18,0) as ot,
kam_paediatric::number(18,0) as kam_paediatric,
kam_obgyn::number(18,0) as kam_obgyn,
kam_minimally_invasive::number(18,0) as kam_minimally_invasive,
kam_total_urologysurgeons::number(18,0) as kam_total_urologysurgeons,
kam_total_surgeons::number(18,0) as kam_total_surgeons,
kam_total_rheumphysicians::number(18,0) as kam_total_rheumphysicians,
kam_total_psychiatryphysicians::number(18,0) as kam_total_psychiatryphysicians,
kam_total_orthosurgeons::number(18,0) as kam_total_orthosurgeons,
kam_total_opthalsurgeons::number(18,0) as kam_total_opthalsurgeons,
kam_total_neurologyphysicians::number(18,0) as kam_total_neurologyphysicians,
kam_total_medoncophysicians::number(18,0) as kam_total_medoncophysicians,
kam_total_infectiousphysicians::number(18,0) as kam_total_infectiousphysicians,
kam_total_haemaphysicians::number(18,0) as kam_total_haemaphysicians,
kam_total_generalsurgeons::number(18,0) as kam_total_generalsurgeons,
kam_total_gastrophysicians::number(18,0) as kam_total_gastrophysicians,
kam_total_endophysicians::number(18,0) as kam_total_endophysicians,
kam_total_dermaphysicians::number(18,0) as kam_total_dermaphysicians,
kam_total_cardiologyphysicians::number(18,0) as kam_total_cardiologyphysicians,
kam_total_cardiosurgeons::number(18,0) as kam_total_cardiosurgeons,
kam_total_aestheticsurgeons::number(18,0) as kam_total_aestheticsurgeons,
kam_general_differnentiations::varchar(32768) as kam_general_differnentiations,
kam_clinical_differentiations::varchar(32768) as kam_clinical_differentiations,
record_type_name::varchar(255) as record_type_name,
hco_external_id::varchar(120) as hco_external_id,
parent_hco_key::varchar(32) as parent_hco_key,
parent_hco_name::varchar(255) as parent_hco_name,
deleted_flag::number(38,0) as deleted_flag,
address_line_1::varchar(255) as address_line_1,
address_line_2::varchar(300) as address_line_2,
suburb_town::varchar(50) as suburb_town,
city::varchar(40) as city,
state::varchar(255) as state,
postcode::varchar(20) as postcode,
brick::varchar(250) as brick,
map::varchar(1300) as map,
hco_address_source_id::varchar(18) as hco_address_source_id,
appt_required_flag::number(38,0) as appt_required_flag,
address_external_id::varchar(123) as address_external_id,
address_phone::varchar(43) as address_phone,
address_fax::varchar(43) as address_fax,
primary_flag::number(38,0) as primary_flag,
address_inactive_flag::number(38,0) as address_inactive_flag,
controlling_address::varchar(18) as controlling_address,
veeva_autogen_id::varchar(33) as veeva_autogen_id,
customer_code::varchar(60) as customer_code,
inserted_date::timestamp_ntz(9) as inserted_date,
updated_date::timestamp_ntz(9) as updated_date
from transformed
)
select * from final