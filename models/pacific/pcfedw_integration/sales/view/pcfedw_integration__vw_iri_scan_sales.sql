with
itg_iri_scan_sales as
(
    select * from {{ ref('pcfitg_integration__itg_iri_scan_sales') }}
),
sdl_mds_pacific_acct_nielsencode_mapping as
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_acct_nielsencode_mapping') }}
),
vw_customer_dim as
(
    select * from {{ ref('pcfedw_integration__vw_customer_dim') }}
),
final as
(
SELECT iisc.iri_market,
    iisc.wk_end_dt,
    iisc.iri_prod_desc,
    iisc.iri_ean,
    iisc.ac_nielsencode,
    ianm.ac_code,
    ianm.ac_longname,
    vcd.sales_grp_cd,
    vcd.sales_grp_desc,
    iisc.scan_sales,
    iisc.scan_units,
    iisc.numeric_distribution,
    iisc.weighted_distribution,
    iisc.store_count_where_scanned
FROM (
        itg_iri_scan_sales iisc
        LEFT JOIN (
            (
                SELECT DISTINCT sdl_mds_pacific_acct_nielsencode_mapping.ac_nielsencode,
                    sdl_mds_pacific_acct_nielsencode_mapping.ac_code,
                    sdl_mds_pacific_acct_nielsencode_mapping.ac_longname
                FROM sdl_mds_pacific_acct_nielsencode_mapping
                WHERE (
                        (
                            sdl_mds_pacific_acct_nielsencode_mapping.ac_nielsencode IS NOT NULL
                        )
                        OR (
                            (
                                sdl_mds_pacific_acct_nielsencode_mapping.ac_nielsencode
                            )::text <> (''::character varying)::text
                        )
                    )
            ) ianm
            LEFT JOIN vw_customer_dim vcd ON (
                (
                    ltrim(
                        (vcd.cust_no)::text,
                        ('0'::character varying)::text
                    ) = ltrim(
                        (ianm.ac_code)::text,
                        ('0'::character varying)::text
                    )
                )
            )
        ) ON (
            (
                upper((ianm.ac_nielsencode)::text) = upper((iisc.ac_nielsencode)::text)
            )
        )
    )
)
select * from final