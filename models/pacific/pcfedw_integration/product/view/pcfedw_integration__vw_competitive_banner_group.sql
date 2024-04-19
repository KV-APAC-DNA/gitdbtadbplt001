with itg_competitive_banner_group as(
    select * from dev_dna_core.snappcfitg_integration.itg_competitive_banner_group
),
edw_time_dim as(
    select * from dev_dna_core.snappcfedw_integration.edw_time_dim
),
transformed as(
select coalesce(itg_cbg.market, '#'::character varying) as market
	,coalesce(itg_cbg.banner, '#'::character varying) as banner
	,coalesce(itg_cbg.banner_classification, '#'::character varying) as banner_classification
	,coalesce(itg_cbg.manufacturer, '#'::character varying) as manufacturer
	,coalesce(itg_cbg.brand, '#'::character varying) as brand
	,coalesce(itg_cbg.sku_name, '#'::character varying) as sku_name
	,coalesce(itg_cbg.ean_number, '#'::character varying) as ean_number
	,itg_cbg.unit
	,itg_cbg.dollar
	,year(itg_cbg.transaction_date::timestamp) as "year"
	,month(itg_cbg.transaction_date::timestamp) as "month"
	,quarter(itg_cbg.transaction_date::timestamp) as quarter
	,time_dim.jj_mnth
	,time_dim.jj_qrtr
	,time_dim.jj_year
	,itg_cbg.country
	,itg_cbg.currency
	,current_timestamp()::timestamp_ntz(9) as crt_dttm
from (
	itg_competitive_banner_group itg_cbg left join edw_time_dim time_dim on ((itg_cbg.transaction_date::date = time_dim.cal_date::date))
	)
)
select * from transformed
