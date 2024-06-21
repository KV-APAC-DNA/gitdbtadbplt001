with source as (
    select * from snapaspwks_integration.wks_edw_perfect_store_data_reformat
),
final as (
    select md5(
            dataset || customerid || nvl (salespersonid, 'a') || nvl (visitid, 'b') || nvl (questiontext, 'c') || nvl (productid, 'd') || nvl (kpi, 'e') || nvl (scheduleddate, '2999-12-12') || nvl (latestdate, '2999-12-12') || nvl (fisc_yr, 2099) || nvl (fisc_per, 12) || nvl (merchandiser_name, 'f') || nvl (customername, 'g') || nvl (country, 'h') || nvl (state, 'i') || nvl (parent_customer, 'j') || nvl (retail_environment, 'k') || nvl (channel, 'l') || nvl (retailer, 'm') || nvl (business_unit, 'n') || nvl (eannumber, 'o') || nvl (matl_num, 'p') || nvl (prod_hier_l1, 'q') || nvl (prod_hier_l2, 'r') || nvl (prod_hier_l3, 's') || nvl (prod_hier_l4, 't') || nvl (prod_hier_l5, 'u') || nvl (prod_hier_l6, 'v') || nvl (prod_hier_l7, 'w') || nvl (prod_hier_l8, 'x') || nvl (prod_hier_l9, 'y') || nvl (ques_type, 'z') || nvl ("y/n_flag", 'a1') || nvl (priority_store_flag, 'b1') || nvl (add_info, 'c1') || nvl (response, 'd1') || nvl (response_score, 'e1') || nvl (kpi_chnl_wt, 99) || nvl (mkt_share, 99) || nvl (salience_val, 99) || nvl(channel_weightage, 99) || nvl (actual_value, '99999') || nvl (ref_value, '99999') || nvl(kpi_actual_wt_val, '99999') || nvl(kpi_ref_val, '99999') || nvl(kpi_ref_wt_val, '99999') || nvl(photo_url, 'p')
        ) hashkey,
        row_number() over (
            partition by md5(
                dataset || customerid || nvl (salespersonid, 'a') || nvl (visitid, 'b') || nvl (questiontext, 'c') || nvl (productid, 'd') || nvl (kpi, 'e') || nvl (scheduleddate, '2999-12-12') || nvl (latestdate, '2999-12-12') || nvl (fisc_yr, 2099) || nvl (fisc_per, 12) || nvl (merchandiser_name, 'f') || nvl (customername, 'g') || nvl (country, 'h') || nvl (state, 'i') || nvl (parent_customer, 'j') || nvl (retail_environment, 'k') || nvl (channel, 'l') || nvl (retailer, 'm') || nvl (business_unit, 'n') || nvl (eannumber, 'o') || nvl (matl_num, 'p') || nvl (prod_hier_l1, 'q') || nvl (prod_hier_l2, 'r') || nvl (prod_hier_l3, 's') || nvl (prod_hier_l4, 't') || nvl (prod_hier_l5, 'u') || nvl (prod_hier_l6, 'v') || nvl (prod_hier_l7, 'w') || nvl (prod_hier_l8, 'x') || nvl (prod_hier_l9, 'y') || nvl (ques_type, 'z') || nvl ("y/n_flag", 'a1') || nvl (priority_store_flag, 'b1') || nvl (add_info, 'c1') || nvl (response, 'd1') || nvl (response_score, 'e1') || nvl (kpi_chnl_wt, 99) || nvl (mkt_share, 99) || nvl (salience_val, 99) || nvl(channel_weightage, 99) || nvl (actual_value, '99999') || nvl (ref_value, '99999') || nvl(kpi_actual_wt_val, '99999') || nvl(kpi_ref_val, '99999') || nvl(kpi_ref_wt_val, '99999') || nvl(photo_url, 'p')
            )
        ) hash_row,
        dataset,
        customerid,
        salespersonid,
        visitid,
        questiontext,
        productid,
        kpi,
        scheduleddate,
        latestdate,
        fisc_yr,
        fisc_per,
        merchandiser_name,
        customername,
        country,
        state,
        parent_customer,
        retail_environment,
        channel,
        retailer,
        business_unit,
        eannumber,
        matl_num,
        prod_hier_l1,
        prod_hier_l2,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        prod_hier_l7,
        prod_hier_l8,
        prod_hier_l9,
        ques_type,
        "y/n_flag",
        priority_store_flag,
        add_info,
        response,
        response_score,
        kpi_chnl_wt,
        mkt_share,
        salience_val,
        channel_weightage,
        actual_value,
        ref_value,
        kpi_actual_wt_val,
        --- new column addition as part of india gt
        kpi_ref_val,
        --- new column addition as part of india gt
        kpi_ref_wt_val,
        --- new column addition as part of india gt
        photo_url,
        store_grade --gcph_category as gcph_category,
        --gcph_subcategory as gcph_subcategory
    from source
)
select * from final