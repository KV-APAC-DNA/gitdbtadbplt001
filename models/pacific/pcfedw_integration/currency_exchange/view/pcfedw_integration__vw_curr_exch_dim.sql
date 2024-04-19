with edw_crncy_exch as
(
    select * from snapaspedw_integration.edw_crncy_exch
),
final as
(
    SELECT 
        edw_crncy_exch.ex_rt_typ AS rate_type,
        edw_crncy_exch.from_crncy AS from_ccy,
        edw_crncy_exch.to_crncy AS to_ccy,
        (
            ((99999999)::numeric)::numeric(18, 0) - ((edw_crncy_exch.vld_from)::numeric)::numeric(18, 0)
        ) AS valid_date,
        edw_crncy_exch.ex_rt AS exch_rate
    FROM edw_crncy_exch
)
select * from final