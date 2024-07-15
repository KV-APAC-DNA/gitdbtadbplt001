with 
itg_retailermaster as 
(
    select * from {{ ref('inditg_integration__itg_retailermaster') }}
),
edw_customer_dim  as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
itg_udcdetails as 
(
    select * from {{ ref('inditg_integration__itg_udcdetails') }}
),
itg_csl_retailerhierarchy as 
(
    select * from {{ ref('inditg_integration__itg_csl_retailerhierarchy') }}
),
trans as 
(
    SELECT 
       RetailerMaster.Rtrcode AS Retailer_Code,
       RetailerMaster.crt_dttm AS Start_Date,
       CASE
         WHEN RetailerMaster.actv_flg = 'Y' THEN TO_TIMESTAMP('99991231','YYYYMMDD')
         ELSE RetailerMaster.updt_dttm
       END AS End_Date,
       COALESCE(RetailerMaster.DistCode,'-1') AS Customer_Code,
       COALESCE(Dim_Customer.customer_name,'Unknown') Customer_Name,
       COALESCE(RetailerMaster.RtrName,'Unknown') AS Retailer_Name,
       COALESCE(RetailerMaster.rtraddress1,'Unknown') AS Retailer_Address1,
       CASE
         WHEN RetailerMaster.rtraddress2 IS NULL OR TRIM(RetailerMaster.rtraddress2) = '' THEN 'Unknown'
         ELSE RetailerMaster.rtraddress2
       END Retailer_Address2,
       CASE
         WHEN RetailerMaster.rtraddress3 IS NULL OR TRIM(RetailerMaster.rtraddress3) = '' THEN 'Unknown'
         ELSE RetailerMaster.rtraddress3
       END Retailer_Address3,
       COALESCE(Dim_Customer.Region_Code,-1) AS Region_Code,
       COALESCE(Dim_Customer.Region_Name,'Unknown') AS Region_Name,
       COALESCE(Dim_Customer.Zone_Code,-1) AS Zone_Code,
       COALESCE(Dim_Customer.Zone_Name,'Unknown') AS Zone_Name,
       COALESCE(Dim_Customer.Zone_Classification,'Unknown') AS Zone_Classification,
       COALESCE(Dim_Customer.Territory_Code,-1) AS Territory_Code,
       COALESCE(Dim_Customer.Territory_Name,'Unknown') AS Territory_Name,
       COALESCE(Dim_Customer.Territory_Classification,'Unknown') AS Territory_Classification,
       COALESCE(Dim_Customer.State_Code,-1) AS State_Code,
       COALESCE(Dim_Customer.State_Name,'Unknown') AS State_Name,
       COALESCE(Dim_Customer.Town_Code,'-1') AS Town_Code,
       COALESCE(Dim_Customer.Town_Name,'Unknown') AS Town_Name,
       COALESCE(Dim_Customer.Town_Classification,'Unknown') AS Town_Classification,
       RetailerMaster.rtrcatlevelid,
       RetailerMaster.rtrcategorycode,
       COALESCE(RetailerMaster.ClassCode,'Unknown') AS Class_Code,
       COALESCE(ClassMaster.ClassDesc,'Unknown') AS Class_Desc,
       CASE
         WHEN ClassMaster.ClassDesc = 'C-Unnati' THEN 'SMALL OUTLET'
         WHEN ClassMaster.ClassDesc = 'SA' THEN 'TOP OUTLET'
         WHEN ClassMaster.ClassDesc = 'A' THEN 'TOP OUTLET'
         WHEN ClassMaster.ClassDesc = 'B' THEN 'TOP OUTLET'
         WHEN ClassMaster.ClassDesc = 'D-Unnati' THEN 'SMALL OUTLET'
         WHEN ClassMaster.ClassDesc = 'C-Unnati' THEN 'SMALL OUTLET'
         WHEN ClassMaster.ClassDesc = 'C' THEN 'SMALL OUTLET' /*included category C and D */
         WHEN ClassMaster.ClassDesc = 'D' THEN 'SMALL OUTLET'
         ELSE 'Unknown'
       END AS OUTLET_TYPE,
       COALESCE(Ret_Cat.RtrHierDfn_Code,'Unknown') AS Channel_Code,
       COALESCE(Ret_Cat.RtrHierDfn_Name,'Unknown') AS Channel_Name,
       CASE
         WHEN Ret_Cat.RtrHierDfn_Name = 'APNA STORE' THEN 'GT'
         WHEN Ret_Cat.RtrHierDfn_Name = 'National Key Accounts' THEN 'MT'
         WHEN Ret_Cat.RtrHierDfn_Name = 'CSD' THEN 'INSITUTIONAL'
         WHEN Ret_Cat.RtrHierDfn_Name = 'E-Commerce' THEN 'MT'
         WHEN Ret_Cat.RtrHierDfn_Name = 'GT' THEN 'GT'
         WHEN Ret_Cat.RtrHierDfn_Name = 'Institutional Sale' THEN 'INSITUTIONAL'
         WHEN Ret_Cat.RtrHierDfn_Name = 'Local Modern Trade Chains' THEN 'MT'
         WHEN Ret_Cat.RtrHierDfn_Name = 'Pharmacy Chains' THEN 'MT'
         WHEN Ret_Cat.RtrHierDfn_Name = 'Self Service Store' THEN 'GT'
         WHEN Ret_Cat.RtrHierDfn_Name = 'Sub-Stockiest' THEN 'SS'
         WHEN Ret_Cat.RtrHierDfn_Name = 'Van-Retail' THEN 'SS'
         WHEN Ret_Cat.RtrHierDfn_Name = 'Van-Wholesale' THEN 'SS'
         WHEN Ret_Cat.RtrHierDfn_Name = 'Wholesale' THEN 'WS'
         ELSE 'Unknown'
       END AS Business_Channel,
       CASE
         WHEN Ret_Cat.RtrHierDfn_Name = 'GT' THEN
           CASE
             WHEN UPPER(UDCDetails.columnvalue) = 'YES' THEN 'LOYALTY'
             ELSE 'NON-LOYALTY'
           END
         ELSE 'REJECT'
       END AS Loyalty_Desc,
       CAST(TO_CHAR(RetailerMaster.RegDate,'YYYYMMDD') AS INTEGER) AS Registration_Date,
       COALESCE(RetailerMaster.Status,-1) AS Status_cd,
       CASE
         WHEN RetailerMaster.Status = 1 THEN 'Active'
         ELSE 'Inactive'
       END Status_Desc,
       COALESCE(RetailerMaster.CSRTRCODE,'Unknown') AS CSRTRCode,
       convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS CRT_DTTM,
       convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS UPDT_DTTM,
       RetailerMaster.actv_flg AS actv_flg,
       Ret_Cat.retlrgroupcode retailer_Category_cd,
       Ret_Cat.retlrgroupname Retailer_Category_Name,
       RetailerMaster.RtrLatitude,
       RetailerMaster.RtrLongitude,
       RetailerMaster.RtrUniquecode,
       RetailerMaster.createddate,
       RetailerMaster.file_rec_dt,
       COALESCE(Dim_Customer.type_name,'Unknown') AS Type_Name
FROM itg_retailermaster RetailerMaster
  LEFT OUTER JOIN edw_customer_dim Dim_Customer ON RetailerMaster.distcode = Dim_Customer.Customer_Code
  LEFT OUTER JOIN (SELECT classcode,
                          classdesc
                   FROM (SELECT classcode,
                                classdesc,
                                ROW_NUMBER() OVER (PARTITION BY classcode order by null) rn
                         FROM itg_csl_retailerhierarchy)
                   WHERE rn = 1) ClassMaster ON RetailerMaster.classcode = ClassMaster.classcode
  LEFT OUTER JOIN (SELECT rtrhierdfn_code,
                          rtrhierdfn_name,
                          retlrgroupcode,
                          retlrgroupname
                   FROM (SELECT rtrhierdfn_code,
                                rtrhierdfn_name,
                                retlrgroupcode,
                                retlrgroupname,
                                ROW_NUMBER() OVER (PARTITION BY rtrhierdfn_code,retlrgroupcode order by null) RN
                         FROM itg_csl_retailerhierarchy)
                   WHERE RN = 1) Ret_Cat
               ON RetailerMaster.rtrcategorycode = Ret_Cat.rtrHierDfn_code
              AND RetailerMaster.rtrcatlevelid = Ret_Cat.retlrgroupcode
  LEFT OUTER JOIN (SELECT *
                   FROM (SELECT distcode,
                                mastervaluecode,
                                columnvalue,
                                ROW_NUMBER() OVER (PARTITION BY distcode,mastervaluecode ORDER BY createddate DESC) rn
                         FROM itg_udcdetails UDCDetails
                         WHERE UPPER(columnname) = 'PLATINUMCLUB2015') udc
                   WHERE rn = 1) UDCDetails
               ON RetailerMaster.distcode = UDCDetails.distcode
              AND RetailerMaster.Rtrcode = UDCDetails.mastervaluecode
),
trans2 as 
(
    select 
        retailer_code,
        start_date,
        end_date,
        customer_code,
        customer_name,
        retailer_name,
        retailer_address1,
        retailer_address2,
        retailer_address3,
        region_code,
        region_name,
        zone_code,
        zone_name,
        zone_classification,
        territory_code,
        territory_name,
        territory_classification,
        state_code,
        state_name,
        town_name,
        town_classification,
        rtrcatlevelid,
        rtrcategorycode,
        class_code,
        class_desc,
        outlet_type,
        channel_code,
        channel_name,
        business_channel,
        loyalty_desc,
        registration_date,
        status_cd,
        status_desc,
        csrtrcode,
        crt_dttm,
        updt_dttm,
        actv_flg,
        retailer_category_cd,
        retailer_category_name,
        rtrlatitude,
        rtrlongitude,
        rtruniquecode,
        createddate,
        file_rec_dt,
        type_name,
        town_code
from trans
),
wks as 
(
    (SELECT DISTINCT RM.rtrcategorycode,
             RM.class_code,
             CASE
               WHEN RH.RtrHierDfn_Name = 'APNA STORE' THEN 'GT'
               WHEN RH.RtrHierDfn_Name = 'National Key Accounts' THEN 'MT'
               WHEN RH.RtrHierDfn_Name = 'CSD' THEN 'INSITUTIONAL'
               WHEN RH.RtrHierDfn_Name = 'E-Commerce' THEN 'MT'
               WHEN RH.RtrHierDfn_Name = 'GT' THEN 'GT'
               WHEN RH.RtrHierDfn_Name = 'Institutional Sale' THEN 'INSITUTIONAL'
               WHEN RH.RtrHierDfn_Name = 'Local Modern Trade Chains' THEN 'MT'
               WHEN RH.RtrHierDfn_Name = 'Pharmacy Chains' THEN 'MT'
               WHEN RH.RtrHierDfn_Name = 'Self Service Store' THEN 'GT'
               WHEN RH.RtrHierDfn_Name = 'Sub-Stockiest' THEN 'SS'
               WHEN RH.RtrHierDfn_Name = 'Van-Retail' THEN 'SS'
               WHEN RH.RtrHierDfn_Name = 'Van-Wholesale' THEN 'SS'
               WHEN RH.RtrHierDfn_Name = 'Wholesale' THEN 'WS'
               ELSE 'Unknown'
             END AS Business_Channel,
             CASE
               WHEN UPPER(RH.RtrHierDfn_Name) = 'GT' THEN
                 CASE
                   WHEN UPPER(UDCDetails.columnvalue) = 'YES' THEN 'LOYALTY'
                   ELSE 'NON-LOYALTY'
                 END
               ELSE 'REJECT'
             END AS Loyalty_Desc,
             RH.rtrhierdfn_code,
             RH.rtrhierdfn_name,
             RH.retlrgroupcode,
             RH.retlrgroupname
      FROM trans2 RM
        JOIN itg_csl_retailerhierarchy RH
          ON UPPER (RM.rtrcategorycode) = UPPER (RH.retlrgroupcode)
         AND UPPER (RM.class_code) = UPPER (RH.classcode)
        LEFT JOIN (SELECT *
              FROM (SELECT distcode,
                           mastervaluecode,
                           columnvalue,
                           ROW_NUMBER() OVER (PARTITION BY distcode,mastervaluecode ORDER BY createddate DESC) rn
                    FROM itg_udcdetails UDCDetails
                    WHERE UPPER(columnname) = 'PLATINUMCLUB2015') udc
              WHERE rn = 1) UDCDetails
          ON RM.customer_code = UDCDetails.distcode
         AND RM.retailer_code = UDCDetails.mastervaluecode
      WHERE UPPER(RM.channel_code) = 'UNKNOWN')
),
updt as 
(
    select 
        upd.retailer_code,
        upd.start_date,
        upd.end_date,
        upd.customer_code,
        upd.customer_name,
        upd.retailer_name,
        upd.retailer_address1,
        upd.retailer_address2,
        upd.retailer_address3,
        upd.region_code,
        upd.region_name,
        upd.zone_code,
        upd.zone_name,
        upd.zone_classification,
        upd.territory_code,
        upd.territory_name,
        upd.territory_classification,
        upd.state_code,
        upd.state_name,
        upd.town_name,
        upd.town_classification,
        upd.rtrcatlevelid,
        upd.rtrcategorycode,
        upd.class_code,
        upd.class_desc,
        upd.outlet_type,
        -- case when upper(channel_code) is null then "UNKNOWN" else upd.channel_code
        -- end as channel_code,
        nvl(upd.channel_code,wks.rtrhierdfn_code) as channel_code,
        nvl(upd.channel_name,wks.rtrhierdfn_name) as channel_name,
        nvl(upd.business_channel,wks.business_channel) as business_channel,
        nvl(upd.loyalty_desc,wks.Loyalty_Desc) as loyalty_desc,
        upd.registration_date,
        upd.status_cd,
        upd.status_desc,
        upd.csrtrcode,
        upd.crt_dttm,
        upd.updt_dttm,
        upd.actv_flg,
        nvl(upd.retailer_category_cd,wks.retlrgroupcode) as retailer_category_cd,
        nvl(upd.retailer_category_name,wks.retlrgroupname) as retailer_category_name,
        upd.rtrlatitude,
        upd.rtrlongitude,
        upd.rtruniquecode,
        upd.createddate,
        upd.file_rec_dt,
        upd.type_name,
        upd.town_code 
        from trans as upd
    left join wks
    ON UPPER (upd.rtrcategorycode) = upper (wks.retlrgroupcode)
   and upper (upd.class_code) = UPPER (wks.class_code)
),
final as 
(
    select 
    retailer_code::varchar(50) as retailer_code,
	start_date::timestamp_ntz(9) as start_date,
	end_date::timestamp_ntz(9) as end_date,
	customer_code::varchar(50) as customer_code,
	customer_name::varchar(150) as customer_name,
	retailer_name::varchar(150) as retailer_name,
	retailer_address1::varchar(250) as retailer_address1,
	retailer_address2::varchar(250) as retailer_address2,
	retailer_address3::varchar(250) as retailer_address3,
	region_code::number(38,0) as region_code,
	region_name::varchar(50) as region_name,
	zone_code::number(38,0) as zone_code,
	zone_name::varchar(50) as zone_name,
	zone_classification::varchar(50) as zone_classification,
	territory_code::number(38,0) as territory_code,
	territory_name::varchar(50) as territory_name,
	territory_classification::varchar(50) as territory_classification,
	state_code::number(38,0) as state_code,
	state_name::varchar(50) as state_name,
	town_name::varchar(50) as town_name,
	town_classification::varchar(50) as town_classification,
	rtrcatlevelid::varchar(30) as rtrcatlevelid,
	rtrcategorycode::varchar(50) as rtrcategorycode,
	class_code::varchar(50) as class_code,
	class_desc::varchar(50) as class_desc,
	outlet_type::varchar(50) as outlet_type,
	channel_code::varchar(50) as channel_code,
	channel_name::varchar(150) as channel_name,
	business_channel::varchar(50) as business_channel,
	loyalty_desc::varchar(50) as loyalty_desc,
	registration_date::number(18,0) as registration_date,
	status_cd::varchar(50) as status_cd,
	status_desc::varchar(10) as status_desc,
	csrtrcode::varchar(50) as csrtrcode,
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm,
	actv_flg::varchar(1) as actv_flg,
	retailer_category_cd::varchar(25) as retailer_category_cd,
	retailer_category_name::varchar(50) as retailer_category_name,
	rtrlatitude::varchar(40) as rtrlatitude,
	rtrlongitude::varchar(40) as rtrlongitude,
	rtruniquecode::varchar(100) as rtruniquecode,
	createddate::timestamp_ntz(9) as createddate,
	file_rec_dt::date as file_rec_dt,
	type_name::varchar(50) as type_name,
	town_code::varchar(50) as town_code
    from updt
)
select * from final