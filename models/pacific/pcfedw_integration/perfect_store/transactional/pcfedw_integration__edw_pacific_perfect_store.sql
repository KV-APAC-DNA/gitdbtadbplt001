with source as 
(
    select * from {{ ref('pcfedw_integration__v_rpt_pacific_perfect_store') }}
),
final as 
(
    select
        dataset::varchar(128) as dataset,
        merchandisingresponseid::varchar(128) as merchandisingresponseid,
        surveyresponseid::varchar(128) as surveyresponseid,
        customerid::varchar(64) as customerid,
        salespersonid::varchar(128) as salespersonid,
        visitid::varchar(128) as visitid,
        mrch_resp_startdt::timestamp_ntz(9) as mrch_resp_startdt,
        mrch_resp_enddt::timestamp_ntz(9) as mrch_resp_enddt,
        mrch_resp_status::varchar(256) as mrch_resp_status,
        mastersurveyid::varchar(128) as mastersurveyid,
        survey_status::varchar(256) as survey_status,
        survey_enddate::varchar(256) as survey_enddate,
        questionkey::varchar(256) as questionkey,
        questiontext::varchar(256) as questiontext,
        valuekey::varchar(256) as valuekey,
        value::varchar(24) as value,
        productid::varchar(40) as productid,
        mustcarryitem::varchar(256) as mustcarryitem,
        answerscore::varchar(256) as answerscore,
        presence::varchar(256) as presence,
        outofstock::varchar(256) as outofstock,
        mastersurveyname::varchar(256) as mastersurveyname,
        kpi::varchar(256) as kpi,
        category::varchar(384) as category,
        segment::varchar(384) as segment,
        vst_visitid::varchar(256) as vst_visitid,
        scheduleddate::date as scheduleddate,
        scheduledtime::varchar(1) as scheduledtime,
        duration::varchar(256) as duration,
        vst_status::varchar(256) as vst_status,
        fisc_yr::float as fisc_yr,
        fisc_per::varchar(21) as fisc_per,
        firstname::varchar(256) as firstname,
        lastname::varchar(256) as lastname,
        cust_remotekey::varchar(64) as cust_remotekey,
        customername::varchar(256) as customername,
        country::varchar(510) as country,
        state::varchar(256) as state,
        county::varchar(256) as county,
        district::varchar(256) as district,
        city::varchar(256) as city,
        storereference::varchar(384) as storereference,
        storetype::varchar(256) as storetype,
        channel::varchar(256) as channel,
        salesgroup::varchar(384) as salesgroup,
        soldtoparty::varchar(256) as soldtoparty,
        brand::varchar(384) as brand,
        productname::varchar(256) as productname,
        eannumber::varchar(64) as eannumber,
        matl_num::varchar(50) as matl_num,
        prod_hier_l1::varchar(256) as prod_hier_l1,
        prod_hier_l2::varchar(256) as prod_hier_l2,
        prod_hier_l3::varchar(384) as prod_hier_l3,
        prod_hier_l4::varchar(384) as prod_hier_l4,
        prod_hier_l5::varchar(384) as prod_hier_l5,
        prod_hier_l6::varchar(256) as prod_hier_l6,
        prod_hier_l7::varchar(256) as prod_hier_l7,
        prod_hier_l8::varchar(256) as prod_hier_l8,
        prod_hier_l9::varchar(323) as prod_hier_l9,
        kpi_chnl_wt::number(31,2) as kpi_chnl_wt,
        mkt_share::number(31,3) as mkt_share,
        ques_desc::varchar(256) as ques_desc,
        "y/n_flag"::varchar(3) as "y/n_flag",
        rej_reason::varchar(256) as rej_reason,
        response::varchar(256) as response,
        response_score::varchar(256) as response_score,
        acc_rej_reason::varchar(256) as acc_rej_reason,
        actual::float as actual,
        target::float as target,
        gcph_category::varchar(256) as gcph_category,
        gcph_subcategory::varchar(256) as gcph_subcategory,
        store_grade::varchar(50) as store_grade
    from source 
)
select * from final