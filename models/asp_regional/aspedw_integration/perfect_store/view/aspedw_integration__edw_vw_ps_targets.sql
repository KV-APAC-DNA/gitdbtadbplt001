 with 
-- itg_mds_cn_ps_targets as (
--     select * from ntaitg_integration.itg_mds_cn_ps_targets -- China out of scope
-- ),
itg_mds_pacific_ps_targets as (
    select * from {{ ref('pcfitg_integration__itg_mds_pacific_ps_targets') }}
),
itg_mds_jp_ps_targets as (
    select * from jpnitg_integration.itg_mds_jp_ps_targets
),
itg_mds_hk_ps_targets as (
    select * from {{ ref('ntaitg_integration__itg_mds_hk_ps_targets') }}
),
itg_mds_tw_ps_targets as (
    select * from {{ ref('ntaitg_integration__itg_mds_tw_ps_targets') }}
),
itg_mds_kr_ps_targets as (
    select * from {{ ref('ntaitg_integration__itg_mds_kr_ps_targets') }}
),
itg_mds_in_ps_targets as (
    select * from {{ ref('inditg_integration__itg_mds_in_ps_targets') }}
),
itg_id_ps_targets as (
    select * from  {{ ref('idnitg_integration__itg_id_ps_targets') }}
),
itg_mds_sg_ps_targets as (
    select * from {{ ref('sgpitg_integration__itg_mds_sg_ps_targets') }}
),
itg_mds_th_ps_targets as (
    select * from {{ ref('thaitg_integration__itg_mds_th_ps_targets') }}
),
itg_mds_vn_ps_targets as (
    select * from {{ ref('vnmitg_integration__itg_mds_vn_ps_targets') }}
),
itg_mds_ph_ps_targets as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_ps_targets') }}
),
itg_mds_my_ps_targets as (
    select * from {{ ref('mysitg_integration__itg_mds_my_ps_targets') }}
),
-- china as
-- (       
--         SELECT 
--                 'China' AS market
--                 ,itg_mds_cn_ps_targets.kpi
--                 ,itg_mds_cn_ps_targets.channel
--                 ,itg_mds_cn_ps_targets.retail_env AS retail_environment
--                 ,itg_mds_cn_ps_targets.attribute_1
--                 ,itg_mds_cn_ps_targets.attribute_2
--                 ,itg_mds_cn_ps_targets.target AS value
--         FROM itg_mds_cn_ps_targets
-- ),
pacific as
(       
        SELECT itg_mds_pacific_ps_targets.market
                ,itg_mds_pacific_ps_targets.kpi
                ,itg_mds_pacific_ps_targets.channel
                ,itg_mds_pacific_ps_targets.retail_env AS retail_environment
                ,itg_mds_pacific_ps_targets.attribute_1
                ,itg_mds_pacific_ps_targets.attribute_2
                ,itg_mds_pacific_ps_targets.target AS value
        FROM itg_mds_pacific_ps_targets
),
japan as
(       
        SELECT 'Japan' AS market
                ,itg_mds_jp_ps_targets.kpi
                ,itg_mds_jp_ps_targets.channel
                ,itg_mds_jp_ps_targets.retail_env AS retail_environment
                ,itg_mds_jp_ps_targets.attribute_1
                ,itg_mds_jp_ps_targets.attribute_2
                ,itg_mds_jp_ps_targets.target AS value
        FROM itg_mds_jp_ps_targets
),
hongkong as
(       
        SELECT 'Hong Kong' AS market
                ,itg_mds_hk_ps_targets.kpi
                ,itg_mds_hk_ps_targets.channel
                ,itg_mds_hk_ps_targets.retail_env AS retail_environment
                ,itg_mds_hk_ps_targets.attribute_1
                ,itg_mds_hk_ps_targets.attribute_2
                ,itg_mds_hk_ps_targets.target AS value
        FROM itg_mds_hk_ps_targets
),
taiwan as
(       
        SELECT 'Taiwan' AS market
                ,itg_mds_tw_ps_targets.kpi
                ,itg_mds_tw_ps_targets.channel
                ,itg_mds_tw_ps_targets.retail_env AS retail_environment
                ,itg_mds_tw_ps_targets.attribute_1
                ,itg_mds_tw_ps_targets.attribute_2
                ,itg_mds_tw_ps_targets.target AS value
        FROM itg_mds_tw_ps_targets
),
korea as
(       
    SELECT 'Korea' AS market
            ,itg_mds_kr_ps_targets.kpi
            ,itg_mds_kr_ps_targets.channel
            ,itg_mds_kr_ps_targets.retail_env AS retail_environment
            ,itg_mds_kr_ps_targets.attribute_1
            ,itg_mds_kr_ps_targets.attribute_2
            ,itg_mds_kr_ps_targets.target AS value
    FROM itg_mds_kr_ps_targets
),
india as
(       
        SELECT 'India' AS market
                ,itg_mds_in_ps_targets.kpi
                ,itg_mds_in_ps_targets.channel
                ,itg_mds_in_ps_targets.retail_env AS retail_environment
                ,itg_mds_in_ps_targets.attribute_1
                ,itg_mds_in_ps_targets.attribute_2
                ,itg_mds_in_ps_targets.target AS value
        FROM itg_mds_in_ps_targets
),
indonesia as
(       
        SELECT 'Indonesia' AS market
                ,itg_id_ps_targets.kpi
                ,itg_id_ps_targets.channel
                ,itg_id_ps_targets.retail_env AS retail_environment
                ,itg_id_ps_targets.attribute_1
                ,itg_id_ps_targets.attribute_2
                ,itg_id_ps_targets.target AS value
        FROM itg_id_ps_targets
),
singapore as
(       
    SELECT 'Singapore' AS market
            ,itg_mds_sg_ps_targets.kpi
            ,itg_mds_sg_ps_targets.channel
            ,itg_mds_sg_ps_targets.retail_env AS retail_environment
            ,itg_mds_sg_ps_targets.attribute_1
            ,itg_mds_sg_ps_targets.attribute_2
            ,itg_mds_sg_ps_targets.target AS value
    FROM itg_mds_sg_ps_targets
),
thailand as
(       
        SELECT 'Thailand' AS market
                ,itg_mds_th_ps_targets.kpi
                ,itg_mds_th_ps_targets.channel
                ,itg_mds_th_ps_targets.retail_env AS retail_environment
                ,itg_mds_th_ps_targets.attribute_1
                ,itg_mds_th_ps_targets.attribute_2
                ,itg_mds_th_ps_targets.target AS value
        FROM itg_mds_th_ps_targets
),
vietnam as
(       
        SELECT 'Vietnam' AS market
                ,itg_mds_vn_ps_targets.kpi
                ,itg_mds_vn_ps_targets.channel
                ,itg_mds_vn_ps_targets.retail_env AS retail_environment
                ,itg_mds_vn_ps_targets.attribute_1
                ,itg_mds_vn_ps_targets.attribute_2
                ,itg_mds_vn_ps_targets.target AS value
        FROM itg_mds_vn_ps_targets
),
philippines as
(        SELECT 'Philippines' AS market
                ,itg_mds_ph_ps_targets.kpi
                ,itg_mds_ph_ps_targets.channel
                ,itg_mds_ph_ps_targets.retail_env AS retail_environment
                ,itg_mds_ph_ps_targets.attribute_1
                ,itg_mds_ph_ps_targets.attribute_2
                ,itg_mds_ph_ps_targets.target AS value
        FROM itg_mds_ph_ps_targets
),
malaysia as
(        SELECT 'Malaysia' AS market
                ,itg_mds_my_ps_targets.kpi
                ,itg_mds_my_ps_targets.channel
                ,itg_mds_my_ps_targets.retail_env AS retail_environment
                ,itg_mds_my_ps_targets.attribute_1
                ,itg_mds_my_ps_targets.attribute_2
                ,itg_mds_my_ps_targets.target AS value
        FROM itg_mds_my_ps_targets
),
final as 
(
        select * from pacific
        union all
        select * from japan
        union all
        select * from hongkong
        union all
        select * from taiwan
        union all
        select * from korea
        union all
        select * from india
        union all
        select * from indonesia
        union all
        select * from singapore
        union all
        select * from thailand
        union all
        select * from vietnam
        union all
        select * from philippines  
        union all
        select * from malaysia       
)
select * from final