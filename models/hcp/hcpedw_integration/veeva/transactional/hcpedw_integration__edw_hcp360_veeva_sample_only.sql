with itg_hcp360_veeva_call as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_call') }}
),
itg_hcp360_veeva_call_detail as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_call_detail') }}
),
itg_hcp360_veeva_recordtype as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_recordtype') }}
),
edw_hcp360_veeva_dim_country as
(
    select * from snapindedw_integration.edw_hcp360_veeva_dim_country
),
edw_hcp360_veeva_dim_employee as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_employee') }}
),
edw_hcp360_veeva_dim_organization as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_organization') }}
),
edw_vw_hcp360_date_dim as
(
    select * from {{ ref('hcpedw_integration__edw_vw_hcp360_date_dim') }}
),
edw_hcp360_veeva_dim_product_indication as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_product_indication') }}
),
edw_hcp360_veeva_dim_hcp as
(
    select * from snapindedw_integration.edw_hcp360_veeva_dim_hcp
),
edw_hcp360_veeva_dim_hco as
(
    select * from snapindedw_integration.edw_hcp360_veeva_dim_hco
),
itg_hcp360_veeva_lookup_retention_period as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_lookup_retention_period') }}
),
call as 
(
    select inn.*
       , p.product_indication_key
     from 
     (select distinct ci.*      
      ,d.call_detail_source_id
      , d.call_detail_priority
      , d.product_source_id cd_product_source_id
      , d.country_code cd_country_code
      , d.is_parent_call is_parent_call_dtl
      , d.last_modified_date call_detail_modify_date
      , d.last_modified_by_id call_detail_modify_id
      , rt. record_type_name 
      , nvl(us_pr.employee_key,'Not Applicable') as employee_key
      , nvl(organization_key,'Not Applicable' ) as organization_key
      , dd.cal_date_id
      ,  d.call_detail_name
      from  itg_hcp360_veeva_call ci 
      , (select * from itg_hcp360_veeva_call_detail d where nvl(d.is_deleted,'0') = '0'  )d
      , itg_hcp360_veeva_recordtype rt
      , edw_hcp360_veeva_dim_country ct
      , ( select employee_key,employee_source_id, 
                    us.country_code ,o.organization_key,o.effective_from_date ,o.effective_to_date
              from edw_hcp360_veeva_dim_employee us, 
                   edw_hcp360_veeva_dim_organization o
               where us.my_organization_code = o.my_organization_code (+)
               and us.country_code = o.country_code (+) ) us_pr  
       , edw_vw_hcp360_date_dim dd                  
      where 
         ci.call_source_id = d. call_source_id (+)
        and ci.country_code = d.country_code (+) 
        and ci.record_type_source_id = rt.record_type_source_id
        and ci.country_code = rt.country_code
       and ci.country_code = ct.country_key 
        and ci.owner_source_id = us_pr.employee_source_id(+) 
        and ci.country_code = us_pr.country_code(+) 
        and ci.call_date  = cal_date
        and ci.call_type = 'Sample Only'
        and ci.is_deleted ='0'
		and (ci.msl_interaction_type is null and ci.Interaction_Mode is null)
        and ci.call_date > (select DATE_TRUNC('year', to_date(convert_timezone('UTC',current_timestamp()))-retention_years*365) 
                          from itg_hcp360_veeva_lookup_retention_period 
                         where upper(table_name) = 'FACT_CALL_DETAIL')
        and ci.call_date >= nvl(us_pr.effective_from_date,to_date('1900-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss'))
        and ci.call_date <= nvl(us_pr.effective_to_date,to_date('2099-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))                     
      ) inn
      , edw_hcp360_veeva_dim_product_indication p
	  where  cd_product_source_id = p.product_source_id(+)
),
final as
(
    SELECT distinct c1.country_code
    , nvl(main.hcp_key,'Not Applicable') hcp_key
    , nvl(child_rec.hco_key ,'Not Applicable') hco_key
    , c1.employee_key
    ,c1.organization_key
    , c1.cal_date_id
    , case 
        when c1.country_code = 'IN' then convert_timezone('Asia/Kolkata',C1.CALL_DATE_TIME)  
        end CALL_DATE_TIME
    , case 
        when c1.country_code = 'IN' then convert_timezone('Asia/Kolkata',C1.LAST_MODIFIED_DATE) 
        end LAST_MODIFIED_DATE  
    ,case 
        when c1.country_code = 'IN' then convert_timezone('Asia/Kolkata',C1.MOBILE_LAST_MODIFIED_DATE_TIME) 
        end MOBILE_LAST_MODIFIED_DATE_TIME
    , C1. CALL_DATE_TIME utc_call_date_time
    ,C1.LAST_MODIFIED_DATE utc_call_entry_time
    ,substring( CALL_STATUS_TYPE, 1, length(call_status_type) -4) call_status_type
    ,'F2F' CALL_CHANNEL_TYP
    , C1. CALL_TYPE ,C1. CALL_DURATION ,C1. CALL_CLM
    ,row_number() over (partition by main.parent_call_name, main.country_code  order by main.parent_call_source_id nulls first, nvl(hcp_key,'Not Applicable'), nvl(hco_key,'Not Applicable'), call_detail_priority  ) is_parent_call_base
    ,main.attendees
    ,substring( main.attendee_type, 1, length(main.attendee_type) -4) attendee_type
    ,replace(c1.call_comments, chr(10),'') call_comments
    ,c1.next_call_notes
    ,C1.pre_call_notes
    ,C1. record_type_name, C1.CALL_NAME, main.parent_call_name, C1. SUBMITTED_BY_MOBILE ,C1. call_source_id 
    ,main. PARENT_CALL_SOURCE_ID
    ,nvl(product_indication_key,'Not Applicable') as product_indication_key   
    ,call_detail_priority, case when call_detail_modify_date is NOT NULL Then 'Detail' Else NULL END CALL_DETAIL_TYPE 
    ,call_detail_source_id 
    ,C1.LAST_MODIFIED_DATE  AS  LASTMODIFIEDDATE
    ,C1.LAST_MODIFIED_BY_ID 
    ,call_detail_modify_date , call_detail_modify_id
    ,c1.is_deleted call_is_deleted_flag,
    c1.last_activity_date,
    c1.sample_card,
    c1.account_plan,
    c1.mobile_id,
    c1.significant_event,
    c1.location,
    c1.signature_date,
    c1.signature,
    c1.territory,
    c1.detailed_products,
    c1.user_name,
    c1.is_sampled_call,
    c1.presentations,
    c1.product_priority_1,
    c1.product_priority_2,
    c1.product_priority_3,
    c1.product_priority_4,
    c1.product_priority_5,
    c1.attendee_list,
    c1.sea_call_type,
    c1.interaction_mode,
    c1.hcp_kol_initiated,
    c1.msl_interaction_type,
    c1.calculated_field_2,
    c1.calculated_hours_3,
    c1.call_planned,
    c1.call_submission_day,
    c1.day_of_week,
    c1.call_detail_name,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz crt_dttm,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz updt_dttm
    from 
    (select c1.call_source_id, c1.call_source_id as parent_call_source_id ,c1.call_name parent_call_name, c1.attendees ,c1.attendee_type, C1.CALL_NAME, c1.is_parent_call, hcp.hcp_key, c1.country_code,c1.call_source_id main_call_source_id
            from itg_hcp360_veeva_call c1
                ,edw_hcp360_veeva_dim_hcp hcp 
            where (c1.parent_call_source_id is NULL or c1.parent_call_source_id = '')
            and c1.account_source_id = hcp_source_id
            and c1.country_code = hcp.country_code  
            and c1.call_type = 'Sample Only'
            and c1.is_deleted ='0'
        union all
        select  c2.call_source_id,c2.parent_call_source_id, c1.call_name parent_call_name,c2.attendees, c2.attendee_type, c2.call_name, c2.is_parent_call, hcp.hcp_key, c1.country_code,c1.call_source_id main_call_source_id 
            from  itg_hcp360_veeva_call c1
                , itg_hcp360_veeva_call c2
                , edw_hcp360_veeva_dim_hcp hcp
                , edw_hcp360_veeva_dim_hcp hcp1
        where  c1.call_type = 'Sample Only'
            and c1.call_type = c2.call_type 
            and (c1.parent_call_source_id is NULL or c1.parent_call_source_id = '')
            and c2.parent_call_source_id = c1.call_source_id
            and c2.account_source_id =  hcp.hcp_source_id
            and c2.country_code = hcp.country_code  
            and c1.account_source_id =  hcp1.hcp_source_id
            and c1.country_code = hcp1.country_code 
            and c2.is_deleted ='0'		 
        ) main
        ,( select child.call_name,child.call_source_id, child.parent_call_source_id, nvl(hco_key,'Not Applicable') hco_key, child.country_code
            from   itg_hcp360_veeva_call child
                    ,edw_hcp360_veeva_dim_hco hco
            where child.account_source_id = hco.hco_source_id 
            and  child.country_code = hco.country_code 
            and  child.call_type = 'Sample Only'
            and  parent_call_source_id is not null
            and  child.is_deleted ='0'
            ) child_rec 
        ,call c1
        where 
        main.call_source_id = c1.call_source_id
        and main.country_code = c1.country_code
        and  main.main_call_source_id  = child_rec.parent_call_source_id (+)
        and  main.country_code = child_rec. country_code (+)
    union all   
    SELECT distinct c1.country_code
        , nvl(child_rec.hcp_key ,'Not Applicable') hcp_key
        , nvl(main.hco_key,'Not Applicable') hco_key
        ,c1.employee_key 
        ,c1.organization_key
        ,c1.cal_date_id
    , case 
        when c1.country_code = 'IN' then convert_timezone('Asia/Kolkata',C1.CALL_DATE_TIME) 
        end CALL_DATE_TIME
    , case 
        when c1.country_code = 'IN' then convert_timezone('Asia/Kolkata',C1.LAST_MODIFIED_DATE) 
        end LAST_MODIFIED_DATE  
    ,case 
        when c1.country_code = 'IN' then convert_timezone('Asia/Kolkata',C1.MOBILE_LAST_MODIFIED_DATE_TIME) 
        end MOBILE_LAST_MODIFIED_DATE_TIME
        , C1. CALL_DATE_TIME utc_call_date_time
        ,C1.LAST_MODIFIED_DATE utc_call_entry_time
    ,substring( CALL_STATUS_TYPE, 1, length(call_status_type) -4) call_status_type
    ,'F2F' CALL_CHANNEL_TYP, C1. CALL_TYPE ,C1. CALL_DURATION ,C1. CALL_CLM
    ,row_number() over (partition by main.parent_call_name, main.country_code  order by main.parent_call_source_id nulls first, nvl(hcp_key,'Not Applicable'), nvl(hco_key,'Not Applicable'), call_detail_priority  ) is_parent_call_base
    ,main.attendees
    ,substring( main.attendee_type, 1, length(main.attendee_type) -4) attendee_type
    ,replace(c1.call_comments, chr(10),'') call_comments
    ,c1.next_call_notes
    ,C1.pre_call_notes
    ,C1.record_type_name
    ,case when (C1.CALL_NAME = main.parent_call_name and child_rec.call_name is not null )  then child_rec.call_name else  c1.call_name  end
    ,main.parent_call_name, C1. SUBMITTED_BY_MOBILE 
    ,case when (C1.call_source_id = main.parent_call_source_id and child_rec.call_source_id is not null )  then child_rec.call_source_id else  c1.call_source_id  end  
    ,main. PARENT_CALL_SOURCE_ID
    , nvl(product_indication_key,'Not Applicable') as product_indication_key   
    , call_detail_priority, case when call_detail_modify_date is NOT NULL Then 'Detail' Else NULL END CALL_DETAIL_TYPE
    , call_detail_source_id
    , C1.LAST_MODIFIED_DATE  AS  LASTMODIFIEDDATE
    , C1.LAST_MODIFIED_BY_ID 
    ,call_detail_modify_date , call_detail_modify_id
    ,c1.is_deleted,
    c1.last_activity_date,
    c1.sample_card,
    c1.account_plan,
    c1.mobile_id,
    c1.significant_event,
    c1.location,
    c1.signature_date,
    c1.signature,
    c1.territory,
    c1.detailed_products,
    c1.user_name,
    c1.is_sampled_call,
    c1.presentations,
    c1.product_priority_1,
    c1.product_priority_2,
    c1.product_priority_3,
    c1.product_priority_4,
    c1.product_priority_5,
    c1.attendee_list,
    c1.sea_call_type,
    c1.interaction_mode,
    c1.hcp_kol_initiated,
    c1.msl_interaction_type,
    c1.calculated_field_2,
    c1.calculated_hours_3,
    c1.call_planned,
    c1.call_submission_day,
    c1.day_of_week,
    c1.call_detail_name,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz 
    from 
        (select c1.call_source_id, c1.call_source_id as parent_call_source_id , c1.call_name parent_call_name,c1.attendees,c1.attendee_type, C1.CALL_NAME, c1.is_parent_call, hco.hco_key, c1.country_code, c1.call_source_id main_call_source_id
            from itg_hcp360_veeva_call c1
                ,edw_hcp360_veeva_dim_hco hco 
            where (c1.parent_call_source_id is NULL or c1.parent_call_source_id = '')
            and c1.account_source_id = hco_source_id
            and c1.country_code = hco.country_code  
            and c1.call_type = 'Sample Only'
            and c1.is_deleted ='0'
        union all
        select  c2.call_source_id,c2.parent_call_source_id, c1.call_name parent_call_name,c2.attendees,  c2.attendee_type, c2.call_name, c2.is_parent_call, hco.hco_key, c1.country_code,c1.call_source_id main_call_source_id 
            from itg_hcp360_veeva_call c1
                , itg_hcp360_veeva_call c2
                , edw_hcp360_veeva_dim_hco hco
                , edw_hcp360_veeva_dim_hco hco1
        where  c1.call_type = 'Sample Only'
            and c1.call_type = c2.call_type 
            and (c1.parent_call_source_id is NULL or c1.parent_call_source_id = '')
            and c2.parent_call_source_id = c1.call_source_id
            and c2.account_source_id =  hco.hco_source_id
            and c2.country_code = hco.country_code  
            and c1.account_source_id =  hco1.hco_source_id
            and c1.country_code = hco1.country_code  
            and c2.is_deleted ='0'
        ) main
            ,( select child.call_name,child.call_source_id, child.parent_call_source_id, nvl(hcp_key,'Not Applicable') hcp_key, child.country_code
            from  itg_hcp360_veeva_call child
                    ,edw_hcp360_veeva_dim_hcp hcp
            where child.account_source_id = hcp.hcp_source_id 
            and  child.country_code = hcp.country_code 
            and  child.call_type = 'Sample Only'
            and  parent_call_source_id is not null 
            and child.is_deleted ='0'
            ) child_rec 
        ,call c1
        where 
        main.call_source_id = c1.call_source_id
        and main.country_code = c1.country_code
        and  main.main_call_source_id  = child_rec.parent_call_source_id (+)
        and  main.country_code = child_rec. country_code (+)
)
select * from final