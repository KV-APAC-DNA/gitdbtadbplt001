with bw_ohjapan_pan as(
    select * from SNAPJPNITG_INTEGRATION.bw_ohjapan_pan
),
final as(
    select 
    	account::varchar(30) as account,
		calday::varchar(24) as calday,
		chrt_accts::varchar(12) as chrt_accts,
		comp_code::varchar(12) as comp_code,
		curkey_tc::varchar(15) as curkey_tc,
		currency::varchar(15) as currency,
		customer::varchar(30) as customer,
		cust_sales::varchar(30) as cust_sales,
		distr_chan::varchar(6) as distr_chan,
		fiscper::varchar(21) as fiscper,
		fiscvarnt::varchar(6) as fiscvarnt,
        material::varchar(54) as material,
        obj_curr::varchar(15) as obj_curr,
    	recordtp::varchar(3) as recordtp,
    	sales_grp::varchar(9) as sales_grp,
		vtype::varchar(9) as vtype,
		amocac::number(17,2) as amocac,
		amoccc::number(17,2) as amoccc,
		s016_0cust_sales::varchar(9) as s016_0cust_sales,
		s017_0cust_sales::varchar(12) as s017_0cust_sales,
		s001_0cust_sales::varchar(24) as s001_0cust_sales,
		grossamttc::number(17,2) as grossamttc,
		s023_0material::varchar(54) as s023_0material,
		s024_0material::varchar(54) as s024_0material,
		s025_0material::varchar(54) as s025_0material,
		s026_0material::varchar(54) as s026_0material,
		s003_0account::varchar(54) as s003_0account,
		s004_0account::varchar(54) as s004_0account,
		s005_0account::varchar(54) as s005_0account,
		s006_0account::varchar(54) as s006_0account,
		s007_0account::varchar(54) as s007_0account,
		plnt::varchar(10) as plnt
    from bw_ohjapan_pan
)
select * from final