with source as
(
    select * from dev_dna_core.snapindedw_integration.v_rpt_in_perfect_store
),

final as
(
    select 
        dataset::varchar(22) as dataset,
        customerid::varchar(255) as customerid,
        salespersonid::varchar(255) as salespersonid,
        mustcarryitem::varchar(4) as mustcarryitem,
        answerscore::varchar(1) as answerscore,
        presence::varchar(5) as presence,
        outofstock::varchar(4) as outofstock,
        kpi::varchar(18) as kpi,
        scheduleddate::date as scheduleddate,
        vst_status::varchar(9) as vst_status,
        fisc_yr::varchar(20) as fisc_yr,
        fisc_per::number(18,0) as fisc_per,
        firstname::varchar(255) as firstname,
        lastname::varchar(1) as lastname,
        customername::varchar(511) as customername,
        country::varchar(5) as country,
        state::varchar(255) as state,
        storereference::varchar(255) as storereference,
        storetype::varchar(255) as storetype,
        channel::varchar(50) as channel,
        salesgroup::varchar(255) as salesgroup,
        prod_hier_l1::varchar(5) as prod_hier_l1,
        prod_hier_l2::varchar(1) as prod_hier_l2,
        prod_hier_l3::varchar(255) as prod_hier_l3,
        prod_hier_l4::varchar(255) as prod_hier_l4,
        prod_hier_l5::varchar(255) as prod_hier_l5,
        prod_hier_l6::varchar(100) as prod_hier_l6,
        prod_hier_l7::varchar(1) as prod_hier_l7,
        prod_hier_l8::varchar(1) as prod_hier_l8,
        prod_hier_l9::varchar(255) as prod_hier_l9,
        kpi_chnl_wt::float as kpi_chnl_wt,
        ms_flag::number(18,0) as ms_flag,
        hit_ms_flag::number(18,0) as hit_ms_flag,
        "y/n_flag"::varchar(3) as "y/n_flag",
        priority_store_flag::varchar(1) as priority_store_flag,
        questiontext::varchar(255) as questiontext,
        ques_desc::varchar(11) as ques_desc,
        value::number(14,0) as value,
        mkt_share::number(20,4) as mkt_share,
        rej_reason::varchar(511) as rej_reason,
        photo_url::varchar(500) as photo_url
    from source
)

select * from final