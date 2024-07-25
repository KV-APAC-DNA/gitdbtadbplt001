{{
    config(
        materialized='view'
    )
}}
with edw_hcp360_in_ventasys_hcp_dim as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_hcp_dim') }}
),
edw_hcp360_in_ventasys_territory_dim as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_territory_dim') }}
),
edw_hcp360_in_ventasys_brand_map as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_brand_map') }}
),
edw_hcp360_hcp_master_key_by_brand as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_hcp_master_key_by_brand') }}
),
edw_hcp360_veeva_fact_call_detail as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_fact_call_detail') }}
),
itg_hcp360_veeva_territory_fields as(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_territory_fields') }}
),
edw_hcp360_veeva_dim_hcp as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_hcp') }}
),
edw_hcp360_veeva_dim_organization as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_organization') }}
),
final as(
    SELECT DISTINCT COALESCE(hcp.brand, vhcp.brand) AS brand,
    COALESCE(hcp.hcp_master_key, vhcp.hcp_master_id) AS hcp_master_id,
    COALESCE(hcp.country_code, vhcp.country_code) AS country_code,
    COALESCE(hcp.hcp_name, vhcp.customer_name) AS hcp_name,
    hcp.hcp_source_id AS hcp_id_veeva,
    vhcp.hcp_id AS hcp_id_ventasys,
    vhcp.cust_entered_date AS hcp_created_date,
    vhcp.core_noncore,
    vhcp.classification,
    CASE 
        WHEN ((hcp.brand)::TEXT = ('ORSL'::CHARACTER VARYING)::TEXT)
            THEN CASE 
                    WHEN (
                            ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('GP - Non-MBBS'::CHARACTER VARYING)::TEXT)
                            OR ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('GP - Non MBBS'::CHARACTER VARYING)::TEXT)
                            )
                        THEN 'GP - Non-MBBS'::CHARACTER VARYING
                    WHEN ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('GP - MBBS'::CHARACTER VARYING)::TEXT)
                        THEN 'GP - MBBS'::CHARACTER VARYING
                    WHEN (
                            (
                                ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('Pediatrics'::CHARACTER VARYING)::TEXT)
                                OR ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('pedia'::CHARACTER VARYING)::TEXT)
                                )
                            OR ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('Pedia'::CHARACTER VARYING)::TEXT)
                            )
                        THEN 'Pediatrics'::CHARACTER VARYING
                    WHEN (
                            ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('Gynecology'::CHARACTER VARYING)::TEXT)
                            OR ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('Gyne'::CHARACTER VARYING)::TEXT)
                            )
                        THEN 'Gynecology'::CHARACTER VARYING
                    WHEN (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT <> ('GP - Non-MBBS'::CHARACTER VARYING)::TEXT)
                                                    AND ((hcp.speciality_1_type)::TEXT <> ('GP - MBBS'::CHARACTER VARYING)::TEXT)
                                                    )
                                                AND ((hcp.speciality_1_type)::TEXT <> ('Pediatrics'::CHARACTER VARYING)::TEXT)
                                                )
                                            AND ((hcp.speciality_1_type)::TEXT <> ('Gynecology'::CHARACTER VARYING)::TEXT)
                                            )
                                        AND ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT <> ('GP - Non MBBS'::CHARACTER VARYING)::TEXT)
                                        )
                                    AND ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT <> ('Gyne'::CHARACTER VARYING)::TEXT)
                                    )
                                AND ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT <> ('pedia'::CHARACTER VARYING)::TEXT)
                                )
                            AND ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT <> ('Pedia'::CHARACTER VARYING)::TEXT)
                            )
                        THEN 'Others'::CHARACTER VARYING
                    ELSE 'Others'::CHARACTER VARYING
                    END
        WHEN (
                ((COALESCE(hcp.brand, vhcp.brand))::TEXT = ('JB'::CHARACTER VARYING)::TEXT)
                OR ((COALESCE(hcp.brand, vhcp.brand))::TEXT = ('JBABY'::CHARACTER VARYING)::TEXT)
                )
            THEN CASE 
                    WHEN (
                            ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('Gyne'::CHARACTER VARYING)::TEXT)
                            OR ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('Gynecology'::CHARACTER VARYING)::TEXT)
                            )
                        THEN 'Gynecology'::CHARACTER VARYING
                    WHEN (
                            (
                                ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('pedia'::CHARACTER VARYING)::TEXT)
                                OR ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('Pedia'::CHARACTER VARYING)::TEXT)
                                )
                            OR ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT = ('Pediatrics'::CHARACTER VARYING)::TEXT)
                            )
                        THEN 'Pediatrics'::CHARACTER VARYING
                    WHEN (
                            (
                                (
                                    (
                                        ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT <> ('Gyne'::CHARACTER VARYING)::TEXT)
                                        AND ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT <> ('pedia'::CHARACTER VARYING)::TEXT)
                                        )
                                    AND ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT <> ('Pedia'::CHARACTER VARYING)::TEXT)
                                    )
                                AND ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT <> ('Pediatrics'::CHARACTER VARYING)::TEXT)
                                )
                            AND ((COALESCE(hcp.speciality_1_type, vhcp.speciality))::TEXT <> ('Gynecology'::CHARACTER VARYING)::TEXT)
                            )
                        THEN 'Others'::CHARACTER VARYING
                    ELSE 'Others'::CHARACTER VARYING
                    END
        WHEN ((COALESCE(hcp.brand, vhcp.brand))::TEXT = ('DERMA'::CHARACTER VARYING)::TEXT)
            THEN CASE 
                    WHEN ((vhcp.speciality)::TEXT = ('Derma'::CHARACTER VARYING)::TEXT)
                        THEN 'Dermatologist'::CHARACTER VARYING
                    WHEN (
                            ((vhcp.speciality)::TEXT = ('pedia'::CHARACTER VARYING)::TEXT)
                            OR ((vhcp.speciality)::TEXT = ('Pedia'::CHARACTER VARYING)::TEXT)
                            )
                        THEN 'Pediatrics'::CHARACTER VARYING
                    WHEN ((vhcp.speciality)::TEXT = ('Cosmo'::CHARACTER VARYING)::TEXT)
                        THEN 'Cosmo'::CHARACTER VARYING
                    WHEN (
                            (
                                (
                                    ((vhcp.speciality)::TEXT <> ('Derma'::CHARACTER VARYING)::TEXT)
                                    AND ((vhcp.speciality)::TEXT <> ('pedia'::CHARACTER VARYING)::TEXT)
                                    )
                                AND ((vhcp.speciality)::TEXT <> ('Pedia'::CHARACTER VARYING)::TEXT)
                                )
                            AND ((vhcp.speciality)::TEXT <> ('Cosmo'::CHARACTER VARYING)::TEXT)
                            )
                        THEN 'Others'::CHARACTER VARYING
                    ELSE 'Others'::CHARACTER VARYING
                    END
        ELSE NULL::CHARACTER VARYING
        END AS speciality,
    "substring" (
        (vhcp.territory)::TEXT,
        6,
        length((vhcp.territory)::TEXT)
        ) AS vhcp_territory,
    "substring" (
        (vhcp.region_hq)::TEXT,
        6,
        length((vhcp.region_hq)::TEXT)
        ) AS vhcp_region,
    "substring" (
        (vhcp.zone)::TEXT,
        6,
        length((vhcp.zone)::TEXT)
        ) AS vhcp_zone,
    call_hier.organization_l5_name,
    call_hier.organization_l4_name,
    call_hier.organization_l3_name,
    call_hier.organization_l2_name
FROM (
    (
        SELECT 'IN'::CHARACTER VARYING AS country_code,
            h.hcp_id,
            h.hcp_master_id,
            h.territory_id,
            h.customer_name,
            h.customer_type,
            h.speciality,
            h.core_noncore,
            h.classification,
            h.planned_visits_per_month,
            h.phone,
            h.email,
            h.is_active,
            h.first_rx_date,
            h.cust_entered_date,
            h.valid_from,
            h.valid_to,
            m.team_brand_name AS brand,
            t.territory,
            t.region_hq,
            t.zone
        FROM (
            SELECT edw_hcp360_in_ventasys_hcp_dim.hcp_id,
                edw_hcp360_in_ventasys_hcp_dim.hcp_master_id,
                edw_hcp360_in_ventasys_hcp_dim.territory_id,
                edw_hcp360_in_ventasys_hcp_dim.customer_name,
                edw_hcp360_in_ventasys_hcp_dim.customer_type,
                edw_hcp360_in_ventasys_hcp_dim.qualification,
                edw_hcp360_in_ventasys_hcp_dim.speciality,
                edw_hcp360_in_ventasys_hcp_dim.core_noncore,
                edw_hcp360_in_ventasys_hcp_dim.classification,
                edw_hcp360_in_ventasys_hcp_dim.is_fbm_adopted,
                edw_hcp360_in_ventasys_hcp_dim.planned_visits_per_month,
                edw_hcp360_in_ventasys_hcp_dim.cell_phone,
                edw_hcp360_in_ventasys_hcp_dim.phone,
                edw_hcp360_in_ventasys_hcp_dim.email,
                edw_hcp360_in_ventasys_hcp_dim.city,
                edw_hcp360_in_ventasys_hcp_dim.STATE,
                edw_hcp360_in_ventasys_hcp_dim.is_active,
                edw_hcp360_in_ventasys_hcp_dim.first_rx_date,
                edw_hcp360_in_ventasys_hcp_dim.cust_entered_date,
                edw_hcp360_in_ventasys_hcp_dim.valid_from,
                edw_hcp360_in_ventasys_hcp_dim.valid_to,
                edw_hcp360_in_ventasys_hcp_dim.crt_dttm,
                edw_hcp360_in_ventasys_hcp_dim.updt_dttm,
                row_number() OVER (
                    PARTITION BY edw_hcp360_in_ventasys_hcp_dim.hcp_master_id ORDER BY edw_hcp360_in_ventasys_hcp_dim.cust_entered_date DESC,
                        edw_hcp360_in_ventasys_hcp_dim.valid_to DESC
                    ) AS rn
            FROM edw_hcp360_in_ventasys_hcp_dim
            ) h,
            edw_hcp360_in_ventasys_territory_dim t,
            edw_hcp360_in_ventasys_brand_map m
        WHERE (
                (
                    (
                        (h.rn = 1)
                        AND ((h.territory_id)::TEXT = (t.territry_id)::TEXT)
                        )
                    AND ((h.hcp_id)::TEXT = (m.hcp_id)::TEXT)
                    )
                AND ((h.is_active)::TEXT = ('Y'::CHARACTER VARYING)::TEXT)
                )
        ) vhcp LEFT JOIN (
        (
            SELECT master.master_hcp_key AS hcp_master_key,
                hcp.country_code,
                hcp.hcp_name,
                CASE 
                    WHEN ((master.brand)::TEXT = ('ORSL'::CHARACTER VARYING)::TEXT)
                        THEN 'ORSL'::CHARACTER VARYING
                    WHEN ((master.brand)::TEXT = ('JB'::CHARACTER VARYING)::TEXT)
                        THEN 'JBABY'::CHARACTER VARYING
                    WHEN ((master.brand)::TEXT = ('DERMA'::CHARACTER VARYING)::TEXT)
                        THEN 'DERMA'::CHARACTER VARYING
                    ELSE NULL::CHARACTER VARYING
                    END AS brand,
                hcp.speciality_1_type,
                hcp.hcp_key,
                hcp.hcp_source_id
            FROM (
                edw_hcp360_hcp_master_key_by_brand master LEFT JOIN edw_hcp360_veeva_dim_hcp hcp ON (((hcp.hcp_source_id)::TEXT = (master.account_source_id)::TEXT))
                )
            WHERE ((hcp.professional_type)::TEXT = ('Doctor'::CHARACTER VARYING)::TEXT)
            ) hcp LEFT JOIN (
            SELECT DISTINCT "call".hcp_key,
                "call".country_key,
                heir.organization_l5_name,
                heir.organization_l4_name,
                heir.organization_l3_name,
                heir.organization_l2_name
            FROM (
                (
                    SELECT edw_hcp360_veeva_fact_call_detail.country_key,
                        edw_hcp360_veeva_fact_call_detail.hcp_key,
                        edw_hcp360_veeva_fact_call_detail.organization_key,
                        row_number() OVER (
                            PARTITION BY edw_hcp360_veeva_fact_call_detail.hcp_key ORDER BY edw_hcp360_veeva_fact_call_detail.call_modify_dt DESC
                            ) AS rn
                    FROM edw_hcp360_veeva_fact_call_detail
                    ) "call" LEFT JOIN edw_hcp360_veeva_dim_organization heir ON (
                        (
                            (("call".country_key)::TEXT = (heir.country_code)::TEXT)
                            AND (("call".organization_key)::TEXT = (heir.organization_key)::TEXT)
                            )
                        )
                )
            WHERE (
                    (
                        ((heir.organization_l5_name)::TEXT like ('FBM%'::CHARACTER VARYING)::TEXT)
                        AND ("call".rn = 1)
                        )
                    AND ((heir.flag)::TEXT = ('NW'::CHARACTER VARYING)::TEXT)
                    )
            
            UNION
            
            SELECT h.hcp_key,
                h.country_code,
                heir.organization_l5_name,
                heir.organization_l4_name,
                heir.organization_l3_name,
                heir.organization_l2_name
            FROM (
                SELECT itg_hcp360_veeva_territory_fields.tf_account_source_id,
                    itg_hcp360_veeva_territory_fields.territory,
                    row_number() OVER (
                        PARTITION BY itg_hcp360_veeva_territory_fields.tf_account_source_id ORDER BY itg_hcp360_veeva_territory_fields.last_modified_date DESC
                        ) AS rn
                FROM itg_hcp360_veeva_territory_fields
                ) tf,
                edw_hcp360_veeva_dim_hcp h,
                edw_hcp360_veeva_dim_organization heir
            WHERE (
                    (
                        (
                            (
                                (
                                    ((tf.territory)::TEXT like ('FBM%'::CHARACTER VARYING)::TEXT)
                                    AND ((h.hcp_source_id)::TEXT = (tf.tf_account_source_id)::TEXT)
                                    )
                                AND ((tf.territory)::TEXT = (heir.my_organization_name)::TEXT)
                                )
                            AND ((h.country_code)::TEXT = (heir.country_code)::TEXT)
                            )
                        AND (tf.rn = 1)
                        )
                    AND ((heir.flag)::TEXT = ('NW'::CHARACTER VARYING)::TEXT)
                    )
            ) call_hier ON (((hcp.hcp_key)::TEXT = (call_hier.hcp_key)::TEXT))
        ) ON (
            (
                ((hcp.hcp_master_key)::TEXT = (vhcp.hcp_master_id)::TEXT)
                AND ((hcp.brand)::TEXT = (vhcp.brand)::TEXT)
                )
            )
    )
)
select * from final
