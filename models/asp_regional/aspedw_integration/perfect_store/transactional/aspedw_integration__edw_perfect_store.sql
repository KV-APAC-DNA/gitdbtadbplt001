{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}

{{
    config(
        post_hook="delete from {{this}} where country='Malaysia' and channel='GT' and trim(storetype)='SUPERMARKET' and customername not in (select distinct customername from  {{ source('mysedw_integration','my_perfect_store_supermarket_adjust') }});"
    )
}}
with source as (
    select * from {{ ref('aspedw_integration__v_rpt_perfect_store') }}
),
final as (
    select
        dataset::varchar(128) as dataset,
        merchandisingresponseid::varchar(128) as merchandisingresponseid,
        surveyresponseid::varchar(128) as surveyresponseid,
        customerid::varchar(255) as customerid,
        salespersonid::varchar(255) as salespersonid,
        visitid::varchar(255) as visitid,
        mrch_resp_startdt::timestamp_ntz(9) as mrch_resp_startdt,
        mrch_resp_enddt::timestamp_ntz(9) as mrch_resp_enddt,
        mrch_resp_status::varchar(256) as mrch_resp_status,
        mastersurveyid::varchar(128) as mastersurveyid,
        survey_status::varchar(256) as survey_status,
        survey_enddate::varchar(256) as survey_enddate,
        questionkey::varchar(256) as questionkey,
        questiontext::varchar(513) as questiontext,
        valuekey::varchar(256) as valuekey,
        value::varchar(65535) as value,
        productid::varchar(255) as productid,
        mustcarryitem::varchar(256) as mustcarryitem,
        answerscore::varchar(256) as answerscore,
        presence::varchar(256) as presence,
        outofstock::varchar(65535) as outofstock,
        mastersurveyname::varchar(256) as mastersurveyname,
        kpi::varchar(573) as kpi,
        category::varchar(510) as category,
        segment::varchar(510) as segment,
        vst_visitid::varchar(256) as vst_visitid,
        scheduleddate::date as scheduleddate,
        scheduledtime::varchar(52) as scheduledtime,
        duration::varchar(256) as duration,
        vst_status::varchar(382) as vst_status,
        fisc_yr::number(18,0) as fisc_yr,
        fisc_per::number(18,0) as fisc_per,
        firstname::varchar(513) as firstname,
        lastname::varchar(256) as lastname,
        cust_remotekey::varchar(255) as cust_remotekey,
        customername::varchar(771) as customername,
        country::varchar(200) as country,
        state::varchar(256) as state,
        county::varchar(256) as county,
        district::varchar(256) as district,
        city::varchar(256) as city,
        storereference::varchar(382) as storereference,
        storetype::varchar(382) as storetype,
        channel::varchar(382) as channel,
        salesgroup::varchar(382) as salesgroup,
        bu::varchar(200) as bu,
        soldtoparty::varchar(256) as soldtoparty,
        brand::varchar(384) as brand,
        productname::varchar(2258) as productname,
        eannumber::varchar(255) as eannumber,
        matl_num::varchar(100) as matl_num,
        prod_hier_l1::varchar(500) as prod_hier_l1,
        prod_hier_l2::varchar(500) as prod_hier_l2,
        prod_hier_l3::varchar(510) as prod_hier_l3,
        prod_hier_l4::varchar(510) as prod_hier_l4,
        prod_hier_l5::varchar(2000) as prod_hier_l5,
        prod_hier_l6::varchar(500) as prod_hier_l6,
        prod_hier_l7::varchar(500) as prod_hier_l7,
        prod_hier_l8::varchar(500) as prod_hier_l8,
        prod_hier_l9::varchar(2258) as prod_hier_l9,
        kpi_chnl_wt::float as kpi_chnl_wt,
        mkt_share::float as mkt_share,
        ques_desc::varchar(382) as ques_desc,
        "y/n_flag"::varchar(150) as "y/n_flag",
        posm_execution_flag::varchar(7) as posm_execution_flag,
        rej_reason::varchar(65535) as rej_reason,
        response::varchar(65535) as response,
        response_score::varchar(65535) as response_score,
        acc_rej_reason::varchar(65535) as acc_rej_reason,
        actual::varchar(40) as actual,
        "target"::varchar(40) as target,
        priority_store_flag::varchar(256) as priority_store_flag,
        photo_url::varchar(500) as photo_url,
        website_url::varchar(1) as website_url,
        store_grade::varchar(50) as store_grade
    from source
)
select * from final