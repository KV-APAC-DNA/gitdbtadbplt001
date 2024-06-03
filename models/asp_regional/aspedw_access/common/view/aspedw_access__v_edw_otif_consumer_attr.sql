with edw_otif_consumer_attr as (
    select * from {{ ref('aspedw_integration__edw_otif_consumer_attr') }}
),
final as (
    select
			 rgn_mkt_cd  as "rgn_mkt_cd",
			fiscal_yr_mo  as "fiscal_yr_mo",
			numerator  as "numerator",
			denominator  as "denominator",
    from edw_otif_consumer_attr
)
select * from final