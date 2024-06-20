with edw_otif_consumer_attr as (
    select * from {{ ref('aspedw_integration__edw_otif_consumer_attr') }}
),
final as (
    select
			region as "region",
            rgn_mkt_cd  as "rgn_mkt_cd",
			fiscal_yr_mo  as "fiscal_yr_mo",
            segment_information as "segment_information",
			numerator  as "numerator",
			denominator  as "denominator",
            otif as "otif"
    from edw_otif_consumer_attr
)
select * from final