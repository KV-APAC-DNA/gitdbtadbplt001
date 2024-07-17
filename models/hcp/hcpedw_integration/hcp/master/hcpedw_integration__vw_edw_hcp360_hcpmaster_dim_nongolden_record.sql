with edw_hcp360_in_ventasys_hcp_dim as(
    select * from snapindedw_integration.edw_hcp360_in_ventasys_hcp_dim
),
edw_hcp360_veeva_dim_hcp as(
    select * from snapindedw_integration.edw_hcp360_veeva_dim_hcp
),
edw_hcp360_veeva_dim_organization_hcp as(
    select * from snapindedw_integration.edw_hcp360_veeva_dim_organization_hcp
),
itg_hcp360_veeva_object_territory_association as(
    select * from snapinditg_integration.itg_hcp360_veeva_object_territory_association
),
edw_hcp360_hcp_master_key_by_brand as(
    select * from snapindedw_integration.edw_hcp360_hcp_master_key_by_brand
),
edw_hcp360_sfmc_hcp_dim as(
    select * from snapindedw_integration.edw_hcp360_sfmc_hcp_dim
),
itg_hcp360_in_ventasys_hcp_master as(
    select * from snapinditg_integration.itg_hcp360_in_ventasys_hcp_master
),
final as(
    SELECT derived_table1.source_system,
    derived_table1.team_name,
    derived_table1.hcp_id,
    derived_table1.customer_name,
    derived_table1.cell_phone,
    derived_table1.email
FROM (
    (
        SELECT 'Ventasys'::CHARACTER VARYING AS source_system,
            (
                CASE 
                    WHEN ((b.team_name)::TEXT = 'ORSL'::TEXT)
                        THEN 'ORSL'::TEXT
                    WHEN ((b.team_name)::TEXT = 'JB'::TEXT)
                        THEN 'JBABY'::TEXT
                    WHEN ((b.team_name)::TEXT = 'DERMA'::TEXT)
                        THEN 'DERMA'::TEXT
                    WHEN (
                            (
                                ((b.team_name)::TEXT <> 'ORSL'::TEXT)
                                AND ((b.team_name)::TEXT <> 'JB'::TEXT)
                                )
                            AND ((b.team_name)::TEXT <> 'DERMA'::TEXT)
                            )
                        THEN NULL::TEXT
                    ELSE NULL::TEXT
                    END
                )::CHARACTER VARYING AS team_name,
            a.hcp_id,
            a.customer_name,
            a.cell_phone,
            a.email
        FROM (
            (
                SELECT ventasys.hcp_id,
                    ventasys.customer_name,
                    ventasys.cell_phone,
                    ventasys.email,
                    ventasys.is_active
                FROM (
                    SELECT edw_hcp360_in_ventasys_hcp_dim.hcp_id,
                        edw_hcp360_in_ventasys_hcp_dim.customer_name,
                        edw_hcp360_in_ventasys_hcp_dim.cell_phone,
                        edw_hcp360_in_ventasys_hcp_dim.email,
                        edw_hcp360_in_ventasys_hcp_dim.valid_to,
                        edw_hcp360_in_ventasys_hcp_dim.valid_from,
                        edw_hcp360_in_ventasys_hcp_dim.is_active,
                        row_number() OVER (
                            PARTITION BY edw_hcp360_in_ventasys_hcp_dim.hcp_id ORDER BY edw_hcp360_in_ventasys_hcp_dim.valid_to DESC,
                                edw_hcp360_in_ventasys_hcp_dim.valid_from DESC
                            ) AS rnw
                    FROM edw_hcp360_in_ventasys_hcp_dim
                    ) ventasys
                WHERE (ventasys.rnw = 1)
                ) a LEFT JOIN itg_hcp360_in_ventasys_hcp_master b ON (((a.hcp_id)::TEXT = (b.v_custid)::TEXT))
            )
        WHERE (
                (
                    NOT (
                        EXISTS (
                            SELECT x.ventasys_custid
                            FROM edw_hcp360_hcp_master_key_by_brand x
                            WHERE (
                                    ((a.hcp_id)::TEXT = (x.ventasys_custid)::TEXT)
                                    AND (x.ventasys_custid IS NOT NULL)
                                    )
                            )
                        )
                    )
                AND ((a.is_active)::TEXT = 'Y'::TEXT)
                )
        
        UNION ALL
        
        SELECT 'Veeva'::CHARACTER VARYING AS source_system,
            (
                CASE 
                    WHEN ((o.organization_l2_name)::TEXT = 'ORSL'::TEXT)
                        THEN 'ORSL'::TEXT
                    WHEN ((o.organization_l2_name)::TEXT = 'Johnson Baby Professional'::TEXT)
                        THEN 'JBABY'::TEXT
                    WHEN ((o.organization_l2_name)::TEXT = 'DERMA'::TEXT)
                        THEN 'DERMA'::TEXT
                    WHEN (
                            (
                                ((o.organization_l2_name)::TEXT <> 'ORSL'::TEXT)
                                AND ((o.organization_l2_name)::TEXT <> 'Johnson Baby Professional'::TEXT)
                                )
                            AND ((o.organization_l2_name)::TEXT <> 'DERMA'::TEXT)
                            )
                        THEN NULL::TEXT
                    ELSE NULL::TEXT
                    END
                )::CHARACTER VARYING AS team_name,
            a.hcp_source_id,
            a.hcp_name AS customer_name,
            a.mobile_nbr,
            a.email_id
        FROM edw_hcp360_veeva_dim_hcp a,
            (
                SELECT org_map.object_id,
                    hcp_org.organization_l2_name
                FROM edw_hcp360_veeva_dim_organization_hcp hcp_org,
                    itg_hcp360_veeva_object_territory_association org_map
                WHERE ((org_map.territory2id)::TEXT = (hcp_org.territory_source_id)::TEXT)
                ) o
        WHERE (
                (
                    (
                        NOT (
                            EXISTS (
                                SELECT b.account_source_id
                                FROM edw_hcp360_hcp_master_key_by_brand b
                                WHERE (
                                        ((a.hcp_source_id)::TEXT = (b.account_source_id)::TEXT)
                                        AND (b.account_source_id IS NOT NULL)
                                        )
                                )
                            )
                        )
                    AND ((a.hcp_type)::TEXT = 'Doctor'::TEXT)
                    )
                AND ((a.hcp_source_id)::TEXT = (o.object_id)::TEXT)
                )
        )
    
    UNION ALL
    
    SELECT 'SFMC'::CHARACTER VARYING AS source_system,
        (
            CASE 
                WHEN (split_part((a.subscriber_key)::TEXT, '_'::TEXT, 1) = 'ORSL'::TEXT)
                    THEN 'ORSL'::TEXT
                WHEN (split_part((a.subscriber_key)::TEXT, '_'::TEXT, 1) = 'JB'::TEXT)
                    THEN 'JBABY'::TEXT
                WHEN (split_part((a.subscriber_key)::TEXT, '_'::TEXT, 1) = 'Aveeno'::TEXT)
                    THEN 'DERMA'::TEXT
                WHEN (
                        (
                            (split_part((a.subscriber_key)::TEXT, '_'::TEXT, 1) <> 'ORSL'::TEXT)
                            AND (split_part((a.subscriber_key)::TEXT, '_'::TEXT, 1) <> 'JB'::TEXT)
                            )
                        AND (split_part((a.subscriber_key)::TEXT, '_'::TEXT, 1) <> 'Aveeno'::TEXT)
                        )
                    THEN NULL::TEXT
                ELSE NULL::TEXT
                END
            )::CHARACTER VARYING AS team_name,
        a.subscriber_key,
        ((((a.first_name)::TEXT || ' '::TEXT) || (a.last_name)::TEXT))::CHARACTER VARYING AS customer_name,
        a.mobile_number,
        a.email
    FROM (
        SELECT DISTINCT edw_hcp360_sfmc_hcp_dim.subscriber_key,
            edw_hcp360_sfmc_hcp_dim.first_name,
            edw_hcp360_sfmc_hcp_dim.last_name,
            edw_hcp360_sfmc_hcp_dim.mobile_number,
            edw_hcp360_sfmc_hcp_dim.email
        FROM edw_hcp360_sfmc_hcp_dim
        WHERE (
                ((COALESCE(edw_hcp360_sfmc_hcp_dim.specialty, edw_hcp360_sfmc_hcp_dim.profession))::TEXT <> 'Pharmacist'::TEXT)
                AND ((COALESCE(edw_hcp360_sfmc_hcp_dim.specialty, edw_hcp360_sfmc_hcp_dim.profession))::TEXT <> 'Pharmacy Assistant'::TEXT)
                )
        ) a
    WHERE (
            NOT (
                EXISTS (
                    SELECT b.subscriber_key
                    FROM edw_hcp360_hcp_master_key_by_brand b
                    WHERE (
                            (b.subscriber_key IS NOT NULL)
                            AND ((a.subscriber_key)::TEXT = (b.subscriber_key)::TEXT)
                            )
                    )
                )
            )
    ) derived_table1
)
select * from final