{{
    config(
        materialized='view'
    )
}}
with edw_hcp360_hcp_master_key_by_brand as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_hcp_master_key_by_brand') }}
),
edw_hcp360_in_ventasys_hcp_dim as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_hcp_dim') }}
),
edw_hcp360_veeva_dim_hcp as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_hcp') }}
),
edw_hcp360_sfmc_hcp_dim as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_sfmc_hcp_dim') }}
),
final as(
    SELECT derived_table1.brand,
    derived_table1.ventasys_id,
    derived_table1.ventasys_name,
    derived_table1.ventasys_mobile,
    derived_table1.ventasys_email,
    derived_table1.veeva_id,
    derived_table1.veeva_name,
    derived_table1.veeva_mobile,
    derived_table1.veeva_email,
    derived_table1.sfmc_id,
    derived_table1.sfmc_name,
    derived_table1.sfmc_mobile,
    derived_table1.sfmc_email,
    derived_table1.master_hcp_key
FROM (
    SELECT CASE 
            WHEN ((a.brand)::TEXT = 'ORSL'::TEXT)
                THEN 'ORSL'::TEXT
            WHEN ((a.brand)::TEXT = 'JB'::TEXT)
                THEN 'JBABY'::TEXT
            WHEN ((a.brand)::TEXT = 'DERMA'::TEXT)
                THEN 'DERMA'::TEXT
            WHEN (
                    (
                        ((a.brand)::TEXT <> 'ORSL'::TEXT)
                        AND ((a.brand)::TEXT <> 'JB'::TEXT)
                        )
                    AND ((a.brand)::TEXT <> 'DERMA'::TEXT)
                    )
                THEN NULL::TEXT
            ELSE NULL::TEXT
            END AS brand,
        a.ventasys_custid AS ventasys_id,
        b.customer_name AS ventasys_name,
        b.cell_phone AS ventasys_mobile,
        b.email AS ventasys_email,
        a.account_source_id AS veeva_id,
        c.hcp_name AS veeva_name,
        c.mobile_nbr AS veeva_mobile,
        c.email_id AS veeva_email,
        a.subscriber_key AS sfmc_id,
        d.customer_name AS sfmc_name,
        d.mobile_number AS sfmc_mobile,
        d.email AS sfmc_email,
        a.master_hcp_key
    FROM (
        (
            (
                edw_hcp360_hcp_master_key_by_brand a LEFT JOIN (
                    SELECT ventasys.hcp_id,
                        ventasys.customer_name,
                        ventasys.cell_phone,
                        ventasys.email
                    FROM (
                        SELECT edw_hcp360_in_ventasys_hcp_dim.hcp_id,
                            edw_hcp360_in_ventasys_hcp_dim.customer_name,
                            edw_hcp360_in_ventasys_hcp_dim.cell_phone,
                            edw_hcp360_in_ventasys_hcp_dim.email,
                            edw_hcp360_in_ventasys_hcp_dim.valid_to,
                            edw_hcp360_in_ventasys_hcp_dim.valid_from,
                            row_number() OVER (
                                PARTITION BY edw_hcp360_in_ventasys_hcp_dim.hcp_id ORDER BY edw_hcp360_in_ventasys_hcp_dim.valid_to DESC,
                                    edw_hcp360_in_ventasys_hcp_dim.valid_from DESC
                                ) AS rnw
                        FROM edw_hcp360_in_ventasys_hcp_dim
                        ) ventasys
                    WHERE (ventasys.rnw = 1)
                    ) b ON ((trim((b.hcp_id)::TEXT) = trim((a.ventasys_custid)::TEXT)))
                ) LEFT JOIN edw_hcp360_veeva_dim_hcp c ON ((trim((c.hcp_source_id)::TEXT) = trim((a.account_source_id)::TEXT)))
            ) LEFT JOIN (
            SELECT DISTINCT edw_hcp360_sfmc_hcp_dim.subscriber_key,
                (((edw_hcp360_sfmc_hcp_dim.first_name)::TEXT || ' '::TEXT) || (edw_hcp360_sfmc_hcp_dim.last_name)::TEXT) AS customer_name,
                edw_hcp360_sfmc_hcp_dim.mobile_number,
                edw_hcp360_sfmc_hcp_dim.email
            FROM edw_hcp360_sfmc_hcp_dim
            ) d ON ((trim((d.subscriber_key)::TEXT) = (a.subscriber_key)::TEXT))
        )
    ) derived_table1
)
select * from final