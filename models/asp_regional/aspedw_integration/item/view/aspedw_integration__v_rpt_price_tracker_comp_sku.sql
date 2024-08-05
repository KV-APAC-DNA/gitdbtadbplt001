with edw_price_tracker as (
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_PRICE_TRACKER
),
itg_mds_rg_sku_benchmarks as (
    select * from DEV_DNA_CORE.SNAPASPITG_INTEGRATION.ITG_MDS_RG_SKU_BENCHMARKS
),
trans as
(
    SELECT jj_base.data_source,
    jj_base.kpi,
    jj_base."cluster",
    jj_base.market,
    jj_base.channel,
    jj_base.retail_environment,
    jj_base.parent_customer,
    jj_base.manufacturer,
    jj_base.competitor,
    jj_base.platform,
    jj_base.store,
    jj_base.sub_store_1,
    jj_base.sub_store_2,
    jj_base.report_date,
    benchmarks.jj_sku_description AS jj_sku_desc,
    jj_base.jj_packsize,
    jj_base.jj_upc,
    benchmarks.comp_upc,
    comp_base.comp_rpc,
    benchmarks.comp_sku_description AS comp_sku_desc,
    CASE 
        WHEN ((comp_base.data_source)::TEXT = ('Yimian Digital Shelf'::CHARACTER VARYING)::TEXT)
            THEN upper((comp_base.source_prod_hier_l5)::TEXT)
        ELSE upper((comp_base.source_prod_hier_l3)::TEXT)
        END AS comp_brand,
    comp_base.comp_packsize,
    comp_base.msrp_lcy,
    comp_base.msrp_usd,
    comp_base.mrp_lcy,
    comp_base.mrp_usd,
    comp_base.mrp_type,
    comp_base.asp_lcy,
    comp_base.asp_usd,
    comp_base.observed_price_lcy,
    comp_base.observed_price_usd,
    comp_base.bcp_lcy,
    comp_base.bcp_usd,
    jj_base.jj_promo_flag,
    comp_base.cust_comp_promo_flag,
    comp_base.price_comp_promo_flag
FROM (
    (
        (
            SELECT DISTINCT edw_price_tracker.data_source,
                'PRICE TRACKER COMP SKU DATA'::CHARACTER VARYING AS kpi,
                edw_price_tracker.cluster as "cluster",
                edw_price_tracker.market,
                edw_price_tracker.channel,
                edw_price_tracker.retail_environment,
                edw_price_tracker.parent_customer,
                edw_price_tracker.manufacturer,
                edw_price_tracker.competitor,
                edw_price_tracker.platform,
                edw_price_tracker.store,
                edw_price_tracker.sub_store_1,
                edw_price_tracker.sub_store_2,
                edw_price_tracker.report_date,
                edw_price_tracker.final_packsize AS jj_packsize,
                edw_price_tracker.ean_upc AS jj_upc,
                "max" (edw_price_tracker.cust_promo_flag) AS jj_promo_flag
            FROM edw_price_tracker
            WHERE (
                    ((edw_price_tracker.kpi)::TEXT = ('PRICE TRACKER SKU DATA'::CHARACTER VARYING)::TEXT)
                    AND ((edw_price_tracker.competitor)::TEXT = ('FALSE'::CHARACTER VARYING)::TEXT)
                    )
            GROUP BY edw_price_tracker.data_source,
                edw_price_tracker.cluster,
                edw_price_tracker.market,
                edw_price_tracker.channel,
                edw_price_tracker.retail_environment,
                edw_price_tracker.parent_customer,
                edw_price_tracker.manufacturer,
                edw_price_tracker.competitor,
                edw_price_tracker.platform,
                edw_price_tracker.store,
                edw_price_tracker.sub_store_1,
                edw_price_tracker.sub_store_2,
                edw_price_tracker.report_date,
                edw_price_tracker.final_packsize,
                edw_price_tracker.ean_upc
            ) jj_base JOIN (
            SELECT itg_mds_rg_sku_benchmarks.market,
                ltrim((itg_mds_rg_sku_benchmarks.jj_upc)::TEXT, ('0'::CHARACTER VARYING)::TEXT) AS jj_upc,
                ltrim((itg_mds_rg_sku_benchmarks.comp_upc)::TEXT, ('0'::CHARACTER VARYING)::TEXT) AS comp_upc,
                itg_mds_rg_sku_benchmarks.valid_from,
                itg_mds_rg_sku_benchmarks.valid_to,
                "max" ((itg_mds_rg_sku_benchmarks.jj_sku_description)::TEXT) AS jj_sku_description,
                "max" ((itg_mds_rg_sku_benchmarks.comp_sku_description)::TEXT) AS comp_sku_description
            FROM itg_mds_rg_sku_benchmarks
            WHERE (
                    (itg_mds_rg_sku_benchmarks.comp_upc IS NOT NULL)
                    AND ((itg_mds_rg_sku_benchmarks.comp_upc)::TEXT <> (''::CHARACTER VARYING)::TEXT)
                    )
            GROUP BY itg_mds_rg_sku_benchmarks.market,
                ltrim((itg_mds_rg_sku_benchmarks.jj_upc)::TEXT, ('0'::CHARACTER VARYING)::TEXT),
                ltrim((itg_mds_rg_sku_benchmarks.comp_upc)::TEXT, ('0'::CHARACTER VARYING)::TEXT),
                itg_mds_rg_sku_benchmarks.valid_from,
                itg_mds_rg_sku_benchmarks.valid_to
            ) benchmarks ON (
                (
                    (
                        (
                            ((jj_base.jj_upc)::TEXT = benchmarks.jj_upc)
                            AND (upper((jj_base.market)::TEXT) = upper((benchmarks.market)::TEXT))
                            )
                        AND (jj_base.report_date >= benchmarks.valid_from)
                        )
                    AND (jj_base.report_date <= benchmarks.valid_to)
                    )
                )
        ) JOIN (
        SELECT DISTINCT edw_price_tracker.data_source,
            edw_price_tracker.market,
            edw_price_tracker.channel,
            edw_price_tracker.retail_environment,
            edw_price_tracker.parent_customer,
            edw_price_tracker.platform,
            edw_price_tracker.report_date,
            edw_price_tracker.final_packsize AS comp_packsize,
            edw_price_tracker.source_prod_hier_l3,
            edw_price_tracker.source_prod_hier_l5,
            edw_price_tracker.ean_upc AS comp_upc,
            edw_price_tracker.rpc AS comp_rpc,
            edw_price_tracker.cust_promo_flag AS cust_comp_promo_flag,
            edw_price_tracker.price_promo_flag AS price_comp_promo_flag,
            edw_price_tracker.msrp_lcy,
            edw_price_tracker.msrp_usd,
            edw_price_tracker.mrp_lcy,
            edw_price_tracker.mrp_usd,
            edw_price_tracker.mrp_type,
            edw_price_tracker.asp_lcy,
            edw_price_tracker.asp_usd,
            edw_price_tracker.observed_price_lcy,
            edw_price_tracker.observed_price_usd,
            edw_price_tracker.bcp_lcy,
            edw_price_tracker.bcp_usd
        FROM edw_price_tracker
        WHERE (
                ((edw_price_tracker.kpi)::TEXT = ('PRICE TRACKER SKU DATA'::CHARACTER VARYING)::TEXT)
                AND ((edw_price_tracker.competitor)::TEXT = ('TRUE'::CHARACTER VARYING)::TEXT)
                )
        ) comp_base ON (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (benchmarks.comp_upc = (comp_base.comp_upc)::TEXT)
                                        AND (jj_base.report_date = comp_base.report_date)
                                        )
                                    AND ((jj_base.data_source)::TEXT = (comp_base.data_source)::TEXT)
                                    )
                                AND ((jj_base.market)::TEXT = (comp_base.market)::TEXT)
                                )
                            AND ((COALESCE(jj_base.platform, 'NA'::CHARACTER VARYING))::TEXT = (COALESCE(comp_base.platform, 'NA'::CHARACTER VARYING))::TEXT)
                            )
                        AND ((COALESCE(jj_base.channel, 'NA'::CHARACTER VARYING))::TEXT = (COALESCE(comp_base.channel, 'NA'::CHARACTER VARYING))::TEXT)
                        )
                    AND ((COALESCE(jj_base.retail_environment, 'NA'::CHARACTER VARYING))::TEXT = (COALESCE(comp_base.retail_environment, 'NA'::CHARACTER VARYING))::TEXT)
                    )
                AND ((COALESCE(jj_base.parent_customer, 'NA'::CHARACTER VARYING))::TEXT = (COALESCE(comp_base.parent_customer, 'NA'::CHARACTER VARYING))::TEXT)
                )
            )
    )
),
final as
(
    select
    data_source::varchar(100) as data_source,
	kpi::varchar(27) as kpi,
	"cluster"::varchar(200) as cluster,
	market::varchar(200) as market,
	channel::varchar(200) as channel,
	retail_environment::varchar(200) as retail_environment,
	parent_customer::varchar(200) as parent_customer,
	manufacturer::varchar(200) as manufacturer,
	competitor::varchar(75) as competitor,
	platform::varchar(200) as platform,
	store::varchar(200) as store,
	sub_store_1::varchar(100) as sub_store_1,
	sub_store_2::varchar(100) as sub_store_2,
	report_date::date as report_date,
	jj_sku_desc::varchar(16777216) as jj_sku_desc,
	jj_packsize::float as jj_packsize,
	jj_upc::varchar(510) as jj_upc,
	comp_upc::varchar(16777216) as comp_upc,
	comp_rpc::varchar(510) as comp_rpc,
	comp_sku_desc::varchar(16777216) as comp_sku_desc,
	comp_brand::varchar(16777216) as comp_brand,
	comp_packsize::float as comp_packsize,
	msrp_lcy::number(15,2) as msrp_lcy,
	msrp_usd::number(25,7) as msrp_usd,
	mrp_lcy::number(15,2) as mrp_lcy,
	mrp_usd::number(25,7) as mrp_usd,
	mrp_type::varchar(100) as mrp_type,
	asp_lcy::number(38,2) as asp_lcy,
	asp_usd::number(38,7) as asp_usd,
	observed_price_lcy::number(15,2) as observed_price_lcy,
	observed_price_usd::number(25,7) as observed_price_usd,
	bcp_lcy::number(25,7) as bcp_lcy,
	bcp_usd::number(25,7) as bcp_usd,
	jj_promo_flag::number(38,0) as jj_promo_flag,
	cust_comp_promo_flag::number(38,0) as cust_comp_promo_flag,
	price_comp_promo_flag::number(38,0) as price_comp_promo_flag
    from trans
)
select *  from final