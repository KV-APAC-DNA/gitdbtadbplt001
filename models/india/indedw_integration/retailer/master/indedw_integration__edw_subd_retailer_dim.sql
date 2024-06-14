with itg_rrl_retailermaster as
(
    select * from inditg_integration.itg_rrl_retailermaster
),
itg_retailervalueclass as
(
    select * from inditg_integration.itg_retailervalueclass
),
itg_retailercategory as
(
    select * from inditg_integration.itg_retailercategory
),
itg_distributoractivation as
(
    select * from inditg_integration.itg_distributoractivation
),
itg_customer as
(
    select * from inditg_integration.itg_customer
),
itg_region as
(
    select * from inditg_integration.itg_region
),
itg_zone as
(
    select * from inditg_integration.itg_zone
),
itg_territory as
(
    select * from inditg_integration.itg_territory
),
itg_rsdmaster as
(
    select * from inditg_integration.itg_rsdmaster
),
itg_routemaster as
(
    select * from inditg_integration.itg_routemaster
),
itg_townmaster as
(
    select * from inditg_integration.itg_townmaster
),
itg_rrl_usermaster as
(
    select * from inditg_integration.itg_rrl_usermaster
),
itg_retailermaster as
(
    select * from inditg_integration.itg_retailermaster
),
itg_salesmanmaster as
(
    select * from inditg_integration.itg_salesmanmaster
),
final as 
(
    SELECT 
      rm.RetailerCode as subd_ret_code,
      rm.crt_dttm as Start_Date,
      case when rm.actv_flg = 'Y' Then 
        to_timestamp('99991231', 'YYYYMMDD')
      else rm.updt_dttm end as End_Date,  
      case when cslrm.rtrcode is null or trim(cslrm.rtrcode) = '' then 'Unknown' else cslrm.rtrcode End as SubD_RTR_Code,
      case when rm.RetailerName is null or trim(rm.RetailerName) = '' then 'Unknown' else rm.RetailerName End as SubD_Ret_Name,
      case when rm.Address1 is null or trim(rm.Address1) = '' then 'Unknown' else rm.Address1 End as SubD_Ret_Address1,
      case when rm.Address2 is null or trim(rm.Address2) = '' then 'Unknown' else rm.Address2 End as SubD_Ret_Address2,
      case when rm.Address is null or trim(rm.Address) = '' then 'Unknown' else rm.Address End as SubD_Ret_Address3,
      coalesce(rm.RetailerClassId, -1) as SubD_Ret_Class_Code,
      case when rvc.valueclassname is null or trim(rvc.valueclassname) = '' then 'Unknown' else rvc.valueclassname End as SubD_Ret_Class_Name,
      case when rm.RetailerChannelCode is null or trim(rm.RetailerChannelCode) = '' then '-1' else rm.RetailerChannelCode End as SubD_Ret_Channel_Code,
      case when rc3.channelname is null or trim(rc3.channelname) = '' then 'Unknown' else rc3.channelname End as SubD_Ret_Channel_Name,
      case when rm.RetailerClassCode is null or trim(rm.RetailerClassCode) = '' then '-1' else rm.RetailerClassCode End as subd_ret_category_code,
      case when rc3.categoryname is null or trim(rc3.categoryname) = '' then 'Unknown' else rc3.categoryname End as subd_ret_category_name,
      case when rm.IsActive=1 then 'Active' Else 'Inactive' End  as SubD_Ret_Status,
      case when rm.OWNERNAME is null or trim(rm.OWNERNAME) = '' then 'Unknown' else rm.OWNERNAME End as SubD_Ret_Owner,
      case when rsd.RouteCode is null or trim(rsd.RouteCode) = '' then 'Unknown' else rsd.RouteCode End as SubD_Route_Code,
      case when rtm.RouteEName is null or trim(rtm.RouteEName) = '' then 'Unknown' else rtm.RouteEName End as SubD_Route_Name,
      case when rm.VillageCode is null or trim(rm.VillageCode) = '' then 'Unknown' else rm.VillageCode End as SubD_Ret_Village_Code,
      case when tm.VillageName is null or trim(tm.VillageName) = '' then 'Unknown' else tm.VillageName End as SubD_Ret_Village_Name,
      case when rm.RSDCode is null or trim(rm.RSDCode) = '' then 'Unknown' else rm.RSDCode End as SubD_Code,
      case when rsd.RSDName is null or trim(rsd.RSDName) = '' then 'Unknown' else rsd.RSDName End as SubD_Name,
      case when cslrm.status=1 then 'Active' else 'Inactive' End as SubD_Status,
      case when rm.DistributorCode is null or trim(rm.DistributorCode) = '' then 'Unknown' else rm.DistributorCode End as Customer_Code,
      case when um2.EUserName is null or trim(um2.EUserName) = '' then 'Unknown' else um2.EUserName End as Customer_Name,
      case when da.activestatus=1 then 'Active' else 'Inactive' End  as Distributor_Status,
      case when rsd.RSRCode is null or trim(rsd.RSRCode) = '' then 'Unknown' else rsd.RSRCode End as SubD_Salesman_Code,
      case when um.EUserName is null or trim(um.EUserName) = '' then 'Unknown' else um.EUserName End as SubD_Salesman_Name,
      case when vc.Region_Name is null or trim(vc.Region_Name) = '' then 'Unknown' else vc.Region_Name End as Region,
      case when vc.Zone_Name is null or trim(vc.Zone_Name) = '' then 'Unknown' else vc.Zone_Name End as Zone ,
      case when vc.Territory_Name is null or trim(vc.Territory_Name) = '' then 'Unknown' else vc.Territory_Name End as Territory,
      case when sm.status is null or trim(sm.status) = '' then 'Unknown' else sm.status end as Salesman_Status,
      case when rm.Mobile is null or trim(rm.Mobile) = '' then 'Unknown' else rm.Mobile End as Mobile,
      case when rm.DisplayStatus is null or trim(rm.DisplayStatus) = '' then 'Unknown' else rm.DisplayStatus End as Display_Status,
      convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm,
      convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm,
      rm.actv_flg as actv_flg ,
	  rm.createddate as createddate
        FROM  
        ( Select * from
            (Select *,
            row_number() over( partition by upper(RSDCode), RetailerCode, DistributorCode order by createddate desc) rn
            from itg_rrl_retailermaster) a
            where rn=1 ) rm 
        LEFT OUTER JOIN (select Distcode, ClassId,valueclassname, row_number() over( partition by  Distcode, ClassId order by valueclassname) rnk from itg_RetailerValueClass group by Distcode, ClassId, valueclassname ) rvc 
        on rm.RetailerClassId = rvc.ClassId and  rm.DistributorCode = rvc.Distcode
        and rvc.rnk=1
        LEFT OUTER JOIN (select distinct rc.RetailerCategoryName as channelname,rc2.RetailerCategoryName as categoryname , rc2.RetailerCategoryCode from 
        (select RetailerCategoryCode, RetailerCategoryName,ctglinkid from itg_RetailerCategory  where ctglevelid='2') rc2 
        LEFT OUTER JOIN (select RetailerCategoryCode, RetailerCategoryName from itg_RetailerCategory where ctglevelid='1') rc 
        on  rc2.ctglinkid = rc.RetailerCategoryCode) rc3 on rm.RetailerChannelCode = rc3.RetailerCategoryCode
        LEFT OUTER JOIN ( select distcode, activestatus from itg_distributoractivation ) da on rm.DistributorCode=da.distcode
        LEFT OUTER JOIN 
            ( Select c.sapid customer_code, r.regionname region_name, z.zonename zone_name, t.territoryname territory_name
            FROM itg_customer c
            left join itg_region r on c.regioncode=r.regioncode
            left join itg_zone z on c.zonecode=z.zonecode
            left join itg_territory t on c.territorycode=t.territorycode
            WHERE c.isactive='Y' ) vc on rm.DistributorCode=vc.customer_Code
        LEFT OUTER JOIN (Select * from
            (Select *,
            row_number() over( partition by DistributorCode, upper(RSDCode), RSRCode order by createddate desc) rn
            from itg_RSDMaster where DistributorCode= right(trim(RSRCode),6)) 
            where rn=1) rsd on rm.DistributorCode=rsd.DistributorCode and upper(rm.RSDCode)=upper(rsd.RSDCode)
        LEFT OUTER JOIN ( select distinct DistributorCode, RouteCode, RouteEName From itg_routemaster ) rtm on rm.DistributorCode=rtm.DistributorCode and rsd.RouteCode=rtm.RouteCode
        LEFT OUTER JOIN ( select villagecode,routecode,rsdcode,rsrcode, VillageName From itg_townmaster ) tm 
        ON rm.villagecode=tm.villagecode and rsd.RouteCode=tm.routecode and rm.rsdcode=tm.rsdcode and rsd.rsrcode=tm.rsrcode
        LEFT OUTER JOIN ( select usercode, EUserName from itg_rrl_usermaster ) um on rsd.rsrcode=um.usercode
        LEFT OUTER JOIN ( select usercode, EUserName from itg_rrl_usermaster ) um2 on rm.distributorcode=um2.usercode
        LEFT OUTER JOIN 
            ( Select distcode, csrtrcode, rtrcode, status from
            (Select distcode, csrtrcode, status, rtrcode, createddate,
            row_number() over( partition by distcode, csrtrcode order by createddate desc, status desc, rtrcode desc) rn
            from itg_retailermaster) a
            where rn=1 ) cslrm 
        on rm.distributorcode=cslrm.distcode and rm.rsdcode=cslrm.csrtrcode
        LEFT OUTER JOIN ( Select distcode, smcode, smname, rmcode, rmname, status from itg_salesmanmaster ) sm 
        ON rm.distributorcode=sm.distcode and rsd.RSRCode=sm.smcode||'-'||sm.distcode and um.EUserName=sm.smname and rsd.RouteCode=sm.rmcode and rtm.RouteEName=sm.rmname

        UNION ALL

        select '-1', convert_timezone('UTC',current_timestamp())::timestamp_ntz, to_timestamp('99991231', 'YYYYMMDD'), 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 
        -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
        'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 
        'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 
        'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', convert_timezone('UTC',current_timestamp())::timestamp_ntz, convert_timezone('UTC',current_timestamp())::timestamp_ntz, 'Y',convert_timezone('UTC',current_timestamp())::timestamp_ntz from dual
)
select subd_ret_code::varchar(50) as subd_ret_code,
    start_date::timestamp_ntz(9) as start_date,
    end_date::timestamp_ntz(9) as end_date,
    subd_rtr_code::varchar(50) as subd_rtr_code,
    subd_ret_name::varchar(150) as subd_ret_name,
    subd_ret_address1::varchar(150) as subd_ret_address1,
    subd_ret_address2::varchar(150) as subd_ret_address2,
    subd_ret_address3::varchar(150) as subd_ret_address3,
    subd_ret_class_code::number(18,0) as subd_ret_class_code,
    subd_ret_class_name::varchar(150) as subd_ret_class_name,
    subd_ret_category_code::varchar(50) as subd_ret_category_code,
    subd_ret_category_name::varchar(50) as subd_ret_category_name,
    subd_ret_status::varchar(10) as subd_ret_status,
    subd_ret_owner::varchar(150) as subd_ret_owner,
    subd_ret_channel_code::varchar(50) as subd_ret_channel_code,
    subd_ret_channel_name::varchar(50) as subd_ret_channel_name,
    subd_route_code::varchar(150) as subd_route_code,
    subd_route_name::varchar(150) as subd_route_name,
    subd_ret_village_code::varchar(50) as subd_ret_village_code,
    subd_ret_village_name::varchar(250) as subd_ret_village_name,
    subd_code::varchar(50) as subd_code,
    subd_name::varchar(50) as subd_name,
    subd_status::varchar(10) as subd_status,
    customer_code::varchar(50) as customer_code,
    customer_name::varchar(150) as customer_name,
    distributor_status::varchar(10) as distributor_status,
    subd_salesman_code::varchar(150) as subd_salesman_code,
    subd_salesman_name::varchar(150) as subd_salesman_name,
    region::varchar(50) as region,
    zone::varchar(50) as zone,
    territory::varchar(50) as territory,
    trim(salesman_status)::varchar(50) as salesman_status,
    mobile::varchar(15) as mobile,
    trim(display_status)::varchar(10) as display_status,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    trim(actv_flg)::varchar(1) as actv_flg,
    createddate::timestamp_ntz(9) as createddate
from final