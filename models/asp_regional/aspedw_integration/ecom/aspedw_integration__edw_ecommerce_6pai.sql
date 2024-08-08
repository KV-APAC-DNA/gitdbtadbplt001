with itg_ecommerce_6pai as
(
    select * from {{ ref('aspitg_integration__itg_ecommerce_6pai') }}
),
itg_ecom_digital_salesweight as
(
    select * from {{ ref('aspitg_integration__itg_ecom_digital_salesweight') }}
),
final as
(
    select 
    a.Source,
    a.Year,
    a.Month,
    a.Cluster,
    a.Market,
    a.KPI,
    a.Detail,
    a.Plan,
    a.Franchise,
    b.salesweight,
    (cast(a.Score_NON_Weighted as float(4)))*(cast(b.salesweight as float(4))) as Score_Weighted,
    a.Score_NON_Weighted,
    a.Gap_vs_PM,
    a.Gap_vs_P3M,
    a.Gap_vs_Plan,
    a.Filename,
    a.crt_dttm
    from 
    itg_ecommerce_6pai a
    left join
    itg_ecom_digital_salesweight b
    on  a.market=b.market
    and a.detail=b.eretailer
)
select source::varchar(25) as source,
    year::varchar(7) as year,
    month::varchar(5) as month,
    cluster::varchar(30) as cluster,
    market::varchar(30) as market,
    kpi::varchar(100) as kpi,
    detail::varchar(100) as detail,
    plan::number(34,4) as plan,
    franchise::varchar(50) as franchise,
    salesweight::number(34,4) as salesweight,
    score_weighted::number(34,4) as score_weighted,
    score_non_weighted::number(34,4) as score_non_weighted,
    gap_vs_pm::number(34,4) as gap_vs_pm,
    gap_vs_p3m::number(34,4) as gap_vs_p3m,
    gap_vs_plan::number(34,4) as gap_vs_plan,
    filename::varchar(100) as filename,
    crt_dttm::varchar(50) as crt_dttm
 from final