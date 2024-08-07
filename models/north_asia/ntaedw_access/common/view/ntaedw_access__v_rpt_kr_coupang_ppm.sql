with source as (
    select * from {{ ref('ntaedw_integration__v_rpt_kr_coupang_ppm') }}
),
final as (
    select
        ctry_cd as "ctry_cd",
        ctry_nm as "ctry_nm",
        crncy_cd as "crncy_cd",
        transaction_date as "transaction_date",
        sku_id as "sku_id",
        barcode as "barcode",
        brand as "brand",
        sku_people as "sku_people",
        product_id as "product_id",
        ppp as "ppp",
        sales_gmv as "sales_gmv",
        revenue as "revenue",
        ppm_percent as "ppm_percent",
        cost_of_purchase as "cost_of_purchase",
        units_sold as "units_sold",
        price as "price",
        fisc_year as "fisc_year",
        fisc_qrtr as "fisc_qrtr",
        fisc_month as "fisc_month",
        fisc_month_num as "fisc_month_num",
        fisc_month_name as "fisc_month_name",
        fisc_wk_num as "fisc_wk_num",
        fisc_month_wk_num as "fisc_month_wk_num",
        fisc_month_day as "fisc_month_day",
        cal_year as "cal_year",
        cal_qrtr as "cal_qrtr",
        cal_month as "cal_month",
        cal_month_num as "cal_month_num",
        cal_month_name as "cal_month_name",
        cal_wk_num as "cal_wk_num",
        cal_mnth_wk_num as "cal_mnth_wk_num",
        cal_mnth_day as "cal_mnth_day",
        cal_day as "cal_day",
        ex_rt_typ as "ex_rt_typ",
        to_crncy as "to_crncy",
        ex_rt as "ex_rt",
        sap_matl_num as "sap_matl_num",
        prod_hier_l1 as "prod_hier_l1",
        prod_hier_l2 as "prod_hier_l2",
        prod_hier_l3 as "prod_hier_l3",
        prod_hier_l4 as "prod_hier_l4",
        prod_hier_l5 as "prod_hier_l5",
        prod_hier_l6 as "prod_hier_l6",
        prod_hier_l7 as "prod_hier_l7",
        prod_hier_l8 as "prod_hier_l8",
        prod_hier_l9 as "prod_hier_l9",
        lcl_prod_nm as "lcl_prod_nm"
    from source
)
select * from final