with wrk_vn_sellthrough_target_p1 as
(
    select * from {{ ref('vnmwks_integration__wrk_vn_sellthrough_target_p1') }}
),
wrk_vn_sellthrough_target_p2 as
(
    select * from {{ ref('vnmwks_integration__wrk_vn_sellthrough_target_p2') }}
),
inn as 
(
        select
            p1.dstrbtr_id,
            nvl(p1.mapped_spk, p2.mapped_spk) as mapped_spk,
            p1.target_cyc,
            p1.target_wk,
            p1.target_value tgt_by_month,
            p1.sales_mnth prev_sales_mnth1,
            p1.sales_wk prev_sales_mnth_wk1,
            p1.amt_by_wk prev_sales_amt_wk1,
            p2.sales_mnth prev_sales_mnth2,
            p2.sales_wk prev_sales_mnth_wk2,
            p2.amt_by_wk prev_sales_amt_wk2,
            nvl(p1.amt_by_wk, 0) + nvl(p2.amt_by_wk, 0) sum_by_wk,
            sum(nvl(p1.amt_by_wk, 0) + nvl(p2.amt_by_wk, 0)) over (
                partition by nvl(p1.mapped_spk, p2.mapped_spk),
                p1.dstrbtr_id,
                p1.target_cyc
            ) sum_for_both_months
        from wrk_vn_sellthrough_target_p1 p1,
            wrk_vn_sellthrough_target_p2 p2
        where p1.dstrbtr_id = p2.dstrbtr_id 
            and p1.target_cyc = p2.target_cyc
            and p1.target_wk = p2.target_wk
        order by p1.dstrbtr_id,
            p1.mapped_spk,
            p1.target_cyc,
            p1.target_wk
),
trans as 
(
select 
    inn.*,
    case
        when (
            nvl(sum_for_both_months, 0) > 0
            and nvl(sum_by_wk, 0) > 0
        ) then (
            (
                round(
                    (round(sum_by_wk, 7) / round(sum_for_both_months, 7)),
                    7
                )
            ) * tgt_by_month
        )
        else case
            when (
                nvl(tgt_by_month, 0) > 0
                and nvl(
                    sum(sum_for_both_months) over (partition by dstrbtr_id, target_cyc),
                    0
                ) = 0
            ) then round(
                tgt_by_month / max(target_wk) over(partition by mapped_spk, dstrbtr_id, target_cyc),
                2
            )
        end
    end tgt_by_week
from inn
),
final as
(
select
	dstrbtr_id::varchar(30) as dstrbtr_id,
	mapped_spk::varchar(30) as mapped_spk,
	target_cyc::number(18,0) as target_cyc,
	target_wk::number(18,0) as target_wk,
	tgt_by_month::number(15,4) as tgt_by_month,
	prev_sales_mnth1::varchar(23) as prev_sales_mnth1,
	prev_sales_mnth_wk1::number(18,0) as prev_sales_mnth_wk1,
	prev_sales_amt_wk1::number(20,4) as prev_sales_amt_wk1,
	prev_sales_mnth2::varchar(23) as prev_sales_mnth2,
	prev_sales_mnth_wk2::number(18,0) as prev_sales_mnth_wk2,
	prev_sales_amt_wk2::number(20,4) as prev_sales_amt_wk2,
	sum_by_wk::number(20,4) as sum_by_wk,
	sum_for_both_months::number(20,4) as sum_for_both_months,
	tgt_by_week::number(20,4) as tgt_by_week,
from trans
)
select * from final