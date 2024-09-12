with v_rpt_cn_perfect_store as
(
    select * from {{ ref('chnedw_integration__v_rpt_cn_perfect_store') }}
),
final as
(
    select
    dataset::varchar(22) as dataset,
	customerid::varchar(200) as customerid,
	salespersonid::varchar(1) as salespersonid,
	mrch_resp_startdt::varchar(1) as mrch_resp_startdt,
	mrch_resp_enddt::varchar(1) as mrch_resp_enddt,
	survey_enddate::varchar(1) as survey_enddate,
	questiontext::varchar(28) as questiontext,
	value::varchar(40) as value,
	mustcarryitem::varchar(4) as mustcarryitem,
	presence::varchar(5) as presence,
	outofstock::varchar(4) as outofstock,
	kpi::varchar(20) as kpi,
	scheduleddate::date as scheduleddate,
	vst_status::varchar(9) as vst_status,
	fisc_yr::number(38,0) as fisc_yr,
	fisc_per::number(38,0) as fisc_per,
	firstname::varchar(100) as firstname,
	lastname::varchar(1) as lastname,
	customername::varchar(500) as customername,
	country::varchar(5) as country,
	state::varchar(100) as state,
	storereference::varchar(100) as storereference,
	storetype::varchar(100) as storetype,
	channel::varchar(2) as channel,
	salesgroup::varchar(100) as salesgroup,
	bu::varchar(100) as bu,
	prod_hier_l1::varchar(5) as prod_hier_l1,
	prod_hier_l4::varchar(30) as prod_hier_l4,
	prod_hier_l5::varchar(300) as prod_hier_l5,
	prod_hier_l6::varchar(200) as prod_hier_l6,
	prod_hier_l9::varchar(100) as prod_hier_l9,
	productname::varchar(100) as productname,
	eannumber::varchar(100) as eannumber,
	category::varchar(300) as category,
	segment::varchar(1) as segment,
	kpi_chnl_wt::number(38,5) as kpi_chnl_wt,
	actual::varchar(40) as actual,
	target::varchar(40) as target,
	mkt_share::number(20,4) as mkt_share,
	ques_desc::varchar(11) as ques_desc,
	"y/n_flag"::VARCHAR(3) as "y/n_flag"
    from v_rpt_cn_perfect_store
)
select * from final