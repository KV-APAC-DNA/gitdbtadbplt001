with 
edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
itg_jnjreport_muser as 
(
    select * from {{ ref('inditg_integration__itg_jnjreport_muser') }}
), 
itg_customer_retailer as 
(
    select * from {{ ref('inditg_integration__itg_customer_retailer') }}
), 
itg_plant as 
(
    select * from {{ source('inditg_integration', 'itg_plant') }}
), 
trans as 
(
    Select 
            Case When cr.KA_Flag='D' Then cr.sapid 
                When cr.KA_Flag='W' Then cr.Wholesalercode
                Else 'Unknown' End as KA_Code,
            cr.CustomerName as KA_Name,
            cr.KA_Flag,
            Case 
            When cr.KA_Flag='D'
                Then cr.sapid
            When cr.DistributorSAPID is null or trim(cr.DistributorSAPID) = '' 
                Then 'Unknown'
            Else
                cr.DistributorSAPID
            End as Distributor_Code,
            Case When cr.KA_Flag='D' Then coalesce(vc.Customer_name, 'Unknown') When cr.KA_Flag='W' Then coalesce(vc2.Customer_name, 'Unknown') Else 'Unknown' End Distributor_Name,
            case when cr.parentcustomercode is null then 'Unknown' else cast(cr.parentcustomercode as varchar) End as Parent_Code,
            case when pcr.CustomerName is null or trim(pcr.CustomerName) = '' then 'Unknown' else pcr.CustomerName End as Parent_Name,
            case when cr.CustomerAddress1 is null or trim(cr.CustomerAddress1) = '' then 'Unknown' else cr.CustomerAddress1 End as KA_Address1,
            case when cr.CustomerAddress2 is null or trim(cr.CustomerAddress2) = '' then 'Unknown' else cr.CustomerAddress2 End as KA_Address2,
            case when cr.CustomerAddress3 is null or trim(cr.CustomerAddress3) = '' then 'Unknown' else cr.CustomerAddress3 End as KA_Address3,
            case when cr.RegionCode is null then '-1' else cr.RegionCode End as Region_Code,
            Case When cr.KA_Flag='D' Then vc.Region_Name When cr.KA_Flag='W' Then vc2.Region_Name Else 'Unknown' End as Region_Name,
            case when cr.ZoneCode is null then '-1' else cr.ZoneCode End as Zone_Code,
            Case When cr.KA_Flag='D' Then vc.Zone_Name When cr.KA_Flag='W' Then vc2.Zone_Name Else 'Unknown' End as Zone_Name,
            case when cr.TerritoryCode is null then '-1' else cr.TerritoryCode End as Territory_Code,
            Case When cr.KA_Flag='D' Then vc.Territory_Name When cr.KA_Flag='W' Then vc2.Territory_Name Else 'Unknown' End as Territory_Name,
            case when cr.StateCode is null then '-1' else cr.StateCode  End as State_Code,
            Case When cr.KA_Flag='D' Then vc.State_Name When cr.KA_Flag='W' Then vc2.State_Name Else 'Unknown' End as State_Name,
            case when cr.TownCode is null then '-1' else cr.TownCode End as Town_Code,
            Case When cr.KA_Flag='D' Then vc.Town_Name When cr.KA_Flag='W' Then vc2.Town_Name Else 'Unknown' End as Town_Name,
            case when cr.IsActive is null or trim(cr.IsActive) = '' then 'U' else cr.IsActive End as Active_Flag,
            case when cr.ABICode is null then '-1' else cr.ABICode End as ABI_Code,  
            case 
            when mu.mfirstname is not null and trim(mu.mfirstname) <> '' and mu.mlastname is not null and trim(mu.mlastname) <> '' 
                then mu.mfirstname || ' ' || mu.mlastname
            when mu.mfirstname is not null and trim(mu.mfirstname) <> ''
                then mu.mfirstname
            when mu.mlastname is not null and trim(mu.mlastname) <> '' 
                then mu.mlastname
            else
                'Unknown'
            End as ABI_Name,  
            case when pl.ShortName is null or trim(pl.ShortName) = '' then 'Unknown' else pl.ShortName End as Plant,
            current_timestamp()::timestamp_ntz(9) as CRT_DTTM,
            current_timestamp()::timestamp_ntz(9) as UPDT_DTTM
            FROM ( Select icr.*, 
                  Case
                  When upper(icr.IsDirectAcct)='Y' 
                        Then 'D'
                  When upper(icr.IsParent)<>'Y' and upper(icr.IsDirectAcct)in ('N','W') and icr.wholesalercode is not null and icr.wholesalercode <> ''  
                        Then 'W'
                  Else 'U' End as KA_Flag
                  FROM (Select * from (
                    Select 
                    row_number() over(partition by 
                    case when wholesalercode='' then null else wholesalercode end, 
                    case when distributorsapid=''then null else distributorsapid end,
                    case when sapid =''then null else sapid end
                    order by ActiveDt desc) rn,
                    i.*
                    from itg_customer_retailer i where activedt is not null)
                    Where rn=1) icr) cr 
            LEFT OUTER JOIN edw_customer_dim vc on cr.SAPID=vc.customer_Code 
            LEFT OUTER JOIN edw_customer_dim vc2 on cr.DistributorSAPID=vc2.customer_Code    
            LEFT OUTER JOIN itg_jnjreport_muser mu ON lower(cr.ABICode)= lower(mu.musername)  
            LEFT OUTER JOIN itg_customer_retailer pcr on cr.parentcustomercode=pcr.customercode
            LEFT OUTER JOIN itg_customer_retailer dcr on cr.distributorcode::varchar(50)=dcr.customercode
            LEFT OUTER JOIN itg_plant pl ON dcr.suppliedBy=pl.PlantCode                  
            Where (upper(cr.IsDirectAcct)='Y' and cr.sapid is not null and cr.sapid <> '' )
            Or (upper(cr.IsParent)<>'Y' and upper(cr.IsDirectAcct)in ('N','W')  and cr.wholesalercode is not null and cr.wholesalercode <> '')
),
final as 
(
    select 
    ka_code::varchar(50) as ka_code,
	ka_name::varchar(50) as ka_name,
	ka_flag::varchar(1) as ka_flag,
	distributor_code::varchar(50) as distributor_code,
	distributor_name::varchar(50) as distributor_name,
	parent_code::varchar(50) as parent_code,
	parent_name::varchar(50) as parent_name,
	ka_address1::varchar(250) as ka_address1,
	ka_address2::varchar(250) as ka_address2,
	ka_address3::varchar(250) as ka_address3,
	region_code::varchar(50) as region_code,
	region_name::varchar(50) as region_name,
	zone_code::varchar(50) as zone_code,
	zone_name::varchar(50) as zone_name,
	territory_code::varchar(50) as territory_code,
	territory_name::varchar(50) as territory_name,
	state_code::varchar(50) as state_code,
	state_name::varchar(50) as state_name,
	town_code::varchar(50) as town_code,
	town_name::varchar(50) as town_name,
	active_flag::varchar(1) as active_flag,
	abi_code::varchar(50) as abi_code,
	abi_name::varchar(50) as abi_name,
	plant::varchar(50) as plant,
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final