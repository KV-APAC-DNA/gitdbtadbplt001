with itg_ecommerce_nts_regional as
(
    select * from {{ ref('aspitg_integration__itg_ecommerce_nts_regional') }}
),
v_intrm_reg_crncy_exch_fiscper as
(
    select * from aspedw_integration.v_intrm_reg_crncy_exch_fiscper
    --{{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
final as
(
    select
    itg_regional.load_date,
    itg_regional.year,
    itg_regional.month,
    case when itg_regional.country is null or itg_regional.country = '' then '#N/A' else itg_regional.country end as country,
    case when itg_regional.gfo is null or itg_regional.gfo = '' then '#N/A' else itg_regional.gfo end as gfo,
    case when itg_regional.need_state is null or itg_regional.need_state = '' then '#N/A' else itg_regional.need_state end as need_state,
    case when itg_regional.brand is null or itg_regional.brand = '' then '#N/A' else itg_regional.brand end as brand,
    case when itg_regional.customer_name is null or itg_regional.customer_name = '' then '#N/A' else itg_regional.customer_name end as customer_name,
    itg_regional.nts_lcy*crncy.ex_rt as nts_usd,
    itg_regional.subject_area,
    itg_regional.from_crncy,
    crncy.ex_rt as ex_rt_to_usd
    from
    (
    select
    load_date,
    cast (year as integer) as year,
    cast (month as integer) as month,
    from_crncy,
    case when country is null or country = '' then '#N/A' else country end as country,
    case when gfo is null or gfo = '' then '#N/A' else gfo end as gfo,
    case when need_state is null or need_state = '' then '#N/A' else need_state end as need_state,
    case when brand is null or brand = '' then '#N/A' else brand end as brand,
    case when customer_name is null or customer_name = '' then '#N/A' else customer_name end as customer_name,
    nts_lcy,
    subject_area
    from itg_ecommerce_nts_regional
    ) itg_regional
    left outer join
    (
    SELECT * FROM(
                SELECT CASE WHEN from_crncy = 'AUD' THEN 'Australia'
                            WHEN from_crncy = 'RMB' THEN 'China Personal Care'
                            WHEN from_crncy = 'SGD' THEN 'Singapore'
                            WHEN from_crncy = 'MYR' THEN 'Malaysia'
                            WHEN from_crncy = 'IDR' THEN 'Indonesia'
                            WHEN from_crncy = 'INR' THEN 'India'
                            WHEN from_crncy = 'JPY' THEN 'Japan'
                            WHEN from_crncy = 'KRW' THEN 'Korea'
                            WHEN from_crncy = 'THB' THEN 'Thailand'
                            WHEN from_crncy = 'PHP' THEN 'Philippines'
                            WHEN from_crncy = 'HKD' THEN 'Hong Kong'
                            WHEN from_crncy = 'TWD' THEN 'Taiwan'
                            WHEN from_crncy = 'VND' THEN 'Vietnam'
                            WHEN from_crncy = 'NZD' THEN 'New Zealand'
                    ELSE 'Exclude' END AS market,
                    from_crncy,
                    ex_rt, ROW_NUMBER () OVER (PARTITION BY from_crncy ORDER BY vld_from DESC) AS row_number
                FROM v_intrm_reg_crncy_exch_fiscper WHERE ex_rt_typ = 'BWAR' AND to_crncy = 'USD'
                GROUP BY from_crncy,ex_rt,vld_from) WHERE row_number = 1 AND market <> 'Exclude'
    ) crncy
    on
    itg_regional.from_crncy = crncy.from_crncy
)
select load_date::timestamp_ntz(9) as load_date,
    year::varchar(20) as year,
    month::varchar(20) as month,
    country::varchar(20) as country,
    gfo::varchar(255) as gfo,
    need_state::varchar(255) as need_state,
    brand::varchar(255) as brand,
    customer_name::varchar(255) as customer_name,
    nts_usd::float as nts_usd,
    subject_area::varchar(20) as subject_area,
    from_crncy::varchar(5) as from_crncy,
    ex_rt_to_usd::float as ex_rt_to_usd
 from final