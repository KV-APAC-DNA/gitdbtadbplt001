--import cte
with wks_china_personal_care_regional_sellout_allmonths
as ( select * from {{ ref('chnwks_integration__wks_china_personal_care_regional_sellout_allmonths') }}),

edw_vw_cal_retail_excellence_dim 
as (select * from {{ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim')}}),

base_dim 
as
(select distinct cntry_cd :: varchar(2) as cntry_cd,
                   sellout_dim_key :: varchar(32) as sellout_dim_key,
                    month::varchar(27) as month
            from wks_china_personal_care_regional_sellout_allmonths
            where month >= (select last_17mnths from edw_vw_cal_retail_excellence_dim)
            and month <= (select last_2mnths from edw_vw_cal_retail_excellence_dim)
            )


select * from base_dim            