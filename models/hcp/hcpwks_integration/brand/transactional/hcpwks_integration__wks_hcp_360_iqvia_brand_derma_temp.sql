with itg_hcp360_in_iqvia_brand as(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_in_iqvia_brand') }}
),
itg_mds_hcp360_product_mapping as(
    select * from {{ ref('hcpitg_integration__itg_mds_hcp360_product_mapping') }}
),
final as(
SELECT 

     'IN' AS COUNTRY												

    ,'IQVIA' AS SOURCE_SYSTEM					

    ,'DERMA' AS BRAND									

    ,brand_category AS BRAND_CATEGORY		

    ,Report_Brand As  REPORT_BRAND_REFERENCE		

    --,case when Report_Brand like'%COMP' then NULL else 'DERMA Total' end  AS IQVIA_BRAND 

    ,'AVEENO BODY' AS IQVIA_BRAND 	

    ,Product_Description

    ,Pack_Description  													

    ,ZONE AS REGION		

    ,pack_form

    ,pack_volume											

    ,cast(Year_month as Date) AS ACTIVTY_DATE 										

    ,no_of_prescriptions AS NoofPrescritions    

    ,no_of_prescribers AS NoofPrescribers   

FROM (SELECT 'DERMA' as Brand, 

       nvl(M.BRAND,case when position( ' ' in I.Product_description ) > 0 then 

                                 split_part(I.Product_description,' ',1) 

                           when position( '-' in I.Product_description ) > 0 then 

                                 split_part(I.Product_description,'-',1) 

                           else 

                                 I.Product_description 

                          end||'_COMP') as Report_Brand, 

       product_description,  

       pack_description,

       pack_form, 

       brand_category, 

       zone, 

       year_month, 

       no_of_prescriptions, 

       no_of_prescribers, 

       pack_volume

       FROM ITG_HCP360_IN_IQVIA_BRAND I

           ,(select  case 

                      when split_part(iqvia,' ',1)='AVEENO' then 'AVEENO BODY' 

                      else brand 

                    end as brand  ,

                    iqvia from ITG_MDS_HCP360_PRODUCT_MAPPING where brand = 'DERMA') M      

      WHERE I.PACK_DESCRIPTION = M.IQVIA(+)

        and I.data_source = 'Aveeno_body'

     ) iqvia
)
select * from final




