{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where country = 'IN' AND source_system = 'SALES_CUBE' AND activity_type = 'SALES_ANALYSIS';
        {% endif %}"
    )
}}
with 
wks_edw_hcp360_sales_data as 
(
    select * from {{ ref('hcpwks_integration__wks_edw_hcp360_sales_data') }}
),
itg_mds_in_hcp_sales_rep_mapping as 
(
    select * from {{ ref('hcpitg_integration__itg_mds_in_hcp_sales_rep_mapping') }}
),
itg_mds_in_hcp_sales_hierarchy_mapping as 
(
    select * from {{ ref('hcpitg_integration__itg_mds_in_hcp_sales_hierarchy_mapping') }}
),
wks_edw_hcp360_sales_cube_details as 
(
    select * from {{ ref('hcpwks_integration__wks_edw_hcp360_sales_cube_details') }}
),
trans_a as 
(
     with BASE AS 
(
SELECT mth_mm AS year_month,
       'IN' AS country ,
       Brand,
	   variant_name,
       channel_name AS sales_channel,
       nvl (region_code ,'Non-Covered Area') AS region,
       nvl (zone_code ,'Non-Covered Area') AS zone,
       nvl (sales_area_code ,'Non-Covered Area') AS sales_area,
	   UDC_AvBabyBodyDocQ42019,
	   udc_babyprofesionalcac2019,
       SUM(achievement_nr) AS sales_value
FROM wks_edw_hcp360_sales_data
group by 1,2,3,4,5,6,7,8,9,10
)
 , SALES_REP as 
(
  SELECT brand_name_code as brand,
         'IN' AS country ,
         region_code as region,
         zone_code as zone,
         hq_sales_area_code as sales_area ,
         count(*) as sales_rep_count
  FROM itg_mds_in_hcp_sales_rep_mapping
  WHERE designation IN ('MSR','HRA','EM')
  group by 1,2,3,4 ,5
)

SELECT 

base.country,
'SALES_CUBE' AS source_system	,
NULL AS channel	,
'SALES_ANALYSIS' AS activity_type	,
NULL AS hcp_master_id	,
NULL AS  employee_id	,
	
NULL AS  brand_category	,
NULL AS speciality	,
NULL AS core_noncore	,
NULL AS classification	,
NULL AS territory	,
	
NULL AS hcp_created_date	,
NULL AS activity_date	,
NULL AS call_source_id	,
NULL AS product_indication_name	,
NULL AS no_of_prescription_units	,
NULL AS first_prescription_date	,
NULL AS planned_call_cnt	,
NULL AS call_duration	,
NULL AS prescription_id	,
NULL AS noofprescritions	,
NULL AS noofprescribers	,
NULL AS email_name	,
NULL AS is_unique	,
NULL AS email_delivered_flag	,
	
NULL AS target_value	,
NULL AS target_kpi	,
NULL AS report_brand_reference	,
NULL AS diagnosis	,
NULL AS region_hq	,
NULL AS email_activity_type	,
NULL AS hcp_id	,
NULL AS transaction_flag	,
NULL AS iqvia_brand	,
NULL AS iqvia_pack_description	,
NULL AS iqvia_product_description	,
NULL AS iqvia_pack_volume	,
NULL AS iqvia_input_brand	,
NULL AS mat_noofprescritions	,
NULL AS mat_noofprescribers	,
NULL AS field_rep_active	,
NULL AS mat_totalprescritions_by_brand	,
NULL AS mat_totalprescribers_by_brand	,
NULL AS mat_totalprescritions_jnj_brand	,
NULL AS totalprescritions_by_brand	,
NULL AS totalprescribers_by_brand	,
NULL AS totalprescritions_jnj_brand	,
NULL AS call_type	,
NULL AS email_subject	,
NULL AS totalprescritions_by_speciality	,
NULL AS totalprescribers_by_speciality	,
NULL AS totalprescritions_jnj_speciality	,
NULL AS totalprescritions_by_indication	,
NULL AS totalprescribers_by_indication	,
NULL AS totalprescritions_jnj_indication	,
	
NULL AS devicecategory	,
NULL AS channelgrouping	,
NULL AS visitor_country	,
NULL AS new_visitors	,
NULL AS repeat_visitors	,
NULL AS all_visitor	,
NULL AS unique_pageviews	,
NULL AS total_downloads	,
NULL AS pages	,
NULL AS page_sessions	,
NULL AS total_page_views	,
NULL AS total_bounces	,
NULL AS total_session_duration	,
NULL AS sessions	,
NULL AS territory_id	,
	
NULL AS sales_unit	,
NULL AS totalsales_unit_by_brand	,
NULL AS totalsales_value_by_brand	,
NULL AS  totalsales_unit_by_jnj_brand	,
NULL AS totalsales_value_by_jnj_brand	,
NULL AS  medical_event_id	,
NULL AS  event_name	,
NULL AS event_type	,
NULL AS event_role	,
NULL AS attendee_status	,
NULL AS event_location	,
NULL AS survey_question	,
NULL AS survey_response	,
NULL AS attendee_name	,
NULL AS key_message	,

current_timestamp() as crt_dttm	,
current_timestamp() as updt_dttm	,
base.year_month AS year_month	,
base.Brand AS brand	,
base.variant_name,
 base.sales_channel AS sales_channel	,
base.region AS region	,
 base.zone AS zone	,
base.sales_area AS sales_area	,
base.sales_value AS sales_value	,
SALES_REP.sales_rep_count AS sales_rep_count,
base.UDC_AvBabyBodyDocQ42019,
  base.udc_babyprofesionalcac2019	


FROM BASE
  LEFT OUTER JOIN SALES_REP
   ON 
   base.country = SALES_REP.country
  and base.Brand = SALES_REP.Brand
  and base.region = SALES_REP.region
  and base.zone = SALES_REP.zone
  and base.sales_area = SALES_REP.sales_area
),
final as 
(
    select 
    country,
    source_system,
    channel,
    activity_type,
    hcp_master_id,
    employee_id,
    brand_category,
    speciality,
    core_noncore,
    classification,
    territory,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    prescription_id,
    noofprescritions,
    noofprescribers,
    email_name,
    is_unique,
    email_delivered_flag,
    target_value,
    target_kpi,
    report_brand_reference,
    diagnosis,
    region_hq,
    email_activity_type,
    hcp_id,
    transaction_flag,
    iqvia_brand,
    iqvia_pack_description,
    iqvia_product_description,
    iqvia_pack_volume,
    iqvia_input_brand,
    mat_noofprescritions,
    mat_noofprescribers,
    field_rep_active,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    call_type,
    email_subject,
    totalprescritions_by_speciality,
    totalprescribers_by_speciality,
    totalprescritions_jnj_speciality,
    totalprescritions_by_indication,
    totalprescribers_by_indication,
    totalprescritions_jnj_indication,
    devicecategory,
    channelgrouping,
    visitor_country,
    new_visitors,
    repeat_visitors,
    all_visitor,
    unique_pageviews,
    total_downloads,
    pages,
    page_sessions,
    total_page_views,
    total_bounces,
    total_session_duration,
    sessions,
    territory_id,
    sales_unit,
    totalsales_unit_by_brand,
    totalsales_value_by_brand,
    totalsales_unit_by_jnj_brand,
    totalsales_value_by_jnj_brand,
    medical_event_id,
    event_name,
    event_type,
    event_role,
    attendee_status,
    event_location,
    survey_question,
    survey_response,
    attendee_name,
    key_message,
    crt_dttm,
    updt_dttm,
    year_month,
    brand,
    variant_name,
    sales_channel,
    region,
    zone,
    sales_area,
    sales_value,
    sales_rep_count,
    UDC_AvBabyBodyDocQ42019,
    udc_babyprofesionalcac2019
    from trans_a
)
select * from final