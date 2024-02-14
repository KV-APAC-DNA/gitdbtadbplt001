with

bwa_copa10 as (
    select * from {{ source('bwa_access', 'bwa_copa10') }}
),
itg_crncy_mult as (
    select * from {{ source('aspitg_integration', 'itg_crncy_mult') }}
),
final as (
     select
            recordmode as recordmode,
            bill_type as bill_type,
            null as data_packet,
            null as data_record,
            prod_hier as prod_hier,
            null as request_number,
            salesdeal as salesdeal,
			fiscper  as fiscper,
			fiscvarnt as fiscvarnt,
			fiscyear as fiscyear,
			calday as calday,
			fiscper3 as fiscper3,
			calmonth as calmonth,
			version  as version,
			vtype as vtype,
			comp_code as comp_code,
			co_area  as co_area,
			profit_ctr as profit_ctr,
			salesemply as salesemply,
			salesorg as salesorg,
			sales_grp as sales_grp,
			sales_off as sales_off,
			cust_group as cust_group,
			distr_chan as distr_chan,
			sales_dist as sales_dist,
			customer as customer,
			material as material,
			cust_sales as cust_sales,
			division as division,
			plant as plant,
			bic_zmercref as zmercref,
			bic_zz_mvgr1 as zz_mvgr1,
			bic_zz_mvgr2 as zz_mvgr2,
			bic_zz_mvgr3 as zz_mvgr3,
			bic_zz_mvgr4 as zz_mvgr4,
			bic_zz_mvgr5 as zz_mvgr5,
			region	as region,
			country  as country,
			prodh6	as prodh6,
			prodh5	as prodh5,
			prodh4	as prodh4,
			prodh3	as prodh3,
			prodh2	as prodh2,
			prodh1	as prodh1,
			-- bic_zfis_quar as zfis_quar,
			mat_sales as mat_sales,
			-- bill_type as bill_type,
			-- bic_zjjfiscwe as zjjfiscwe,
			case when curr2.currkey is null 
			then iff(right(amocac,1)='-',concat('-',replace(amocac,'-','')),amocac) 
			else iff(right(amocac,1)='-',concat('-',replace(amocac,'-','')),amocac) * pow(10,(2-curr2.currdec)) 
			end as amocac,
			case when curr1.currkey is null 
			then iff(right(amoccc,1)='-',concat('-',replace(amoccc,'-','')),amoccc) 
			else iff(right(amoccc,1)='-',concat('-',replace(amoccc,'-','')),amoccc) * pow(10,(2-curr1.currdec)) 
			end as amoccc,
			currency as currency,
			obj_curr as obj_curr,
			account  as account,
			chrt_accts as chrt_accts,
			bic_zz_wwme as zz_wwme,
			bic_zsalesper as zsalesper,
			bus_area as bus_area,
			case when curr3.currkey is null 
			then iff(right(grossamttc,1)='-',concat('-',replace(grossamttc,'-','')),grossamttc) 
			else iff(right(grossamttc,1)='-',concat('-',replace(grossamttc,'-','')),grossamttc) * pow(10,(2-curr3.currdec)) 
			end as grossamttc,
			curkey_tc as curkey_tc,
			-- mat_plant as mat_plant,
			iff(right(quantity,1)='-',concat('-',replace(quantity,'-','')),quantity) as quantity,
			unit as unit,
			iff(right(bic_zqtyieu,1)='-',concat('-',replace(bic_zqtyieu,'-','')),bic_zqtyieu) as zqtyieu,
			bic_zunitieu as zunitieu,
			-- bic_zbpt_dc as zbpt_dc,
			case when curr2.currkey is null 
			then iff(right(bic_zfamocac,1)='-',concat('-',replace(bic_zfamocac,'-','')),bic_zfamocac) 
			else iff(right(bic_zfamocac,1)='-',concat('-',replace(bic_zfamocac,'-','')),bic_zfamocac) * pow(10,(2-curr2.currdec)) 
			end as zfamocac,
            current_timestamp()::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
        from bwa_copa10 as src
		left join itg_crncy_mult curr1
				on src.obj_curr = curr1.currkey
		left join itg_crncy_mult curr2
				on src.currency = curr2.currkey
		left join itg_crncy_mult curr3
				on src.curkey_tc = curr3.currkey
)
select * from final