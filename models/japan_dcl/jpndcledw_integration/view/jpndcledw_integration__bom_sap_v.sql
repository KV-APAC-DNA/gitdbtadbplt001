with edw_sap_bw_dna_material_bomlist as (
select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_SAP_BW_DNA_MATERIAL_BOMLIST
),
edw_material_dim as (
select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_MATERIAL_DIM
),
final as (
SELECT base1.oya_hin_cd,
    base1.kos_hin_cd,
    "max" (base1.kos_qut) AS kos_qut
FROM (
    SELECT DISTINCT matl1.old_matl_num AS oya_hin_cd,
        matl2.old_matl_num AS kos_hin_cd,
        ((bom.quantity / bom.basequantity))::NUMERIC(38, 8) AS kos_qut
    FROM (
        (
            edw_sap_bw_dna_material_bomlist bom JOIN edw_material_dim matl1 ON (((bom.matl_num)::TEXT = (matl1.matl_num)::TEXT))
            ) JOIN edw_material_dim matl2 ON (((bom.component)::TEXT = (matl2.matl_num)::TEXT))
        )
    WHERE (
            (
                (
                    (
                        (
                            (
                                (
                                    ((bom.plant)::TEXT = ('3118'::CHARACTER VARYING)::TEXT)
                                    OR ((bom.plant)::TEXT = ('3119'::CHARACTER VARYING)::TEXT)
                                    )
                                AND ((bom.alternativebom)::TEXT = '01'::TEXT)
                                )
                            AND (bom.bomstatus = 1)
                            )
                        AND (bom.validfrom <= to_date(convert_timezone('Asia/Singapore'::TEXT, '2024-07-01'::date::TIMESTAMP without TIME zone)))
                        )
                    AND (bom.validto >= to_date(convert_timezone('Asia/Singapore'::TEXT, '2024-07-01'::date::TIMESTAMP without TIME zone)))
                    )
                AND (bom.validfrom_zvlfromc <= to_date(convert_timezone('Asia/Singapore'::TEXT, '2024-07-01'::date::TIMESTAMP without TIME zone)))
                )
            AND (bom.validto_zvltoi >= to_date(convert_timezone('Asia/Singapore'::TEXT, '2024-07-01'::date::TIMESTAMP without TIME zone)))
            )
    ) base1
WHERE (
        (
            (base1.oya_hin_cd IS NOT NULL)
            AND ((base1.oya_hin_cd)::TEXT <> (''::CHARACTER VARYING)::TEXT)
            )
        AND (
            NOT (
                base1.oya_hin_cd IN (
                    SELECT base2.oya_hin_cd
                    FROM (
                        SELECT DISTINCT matl1.old_matl_num AS oya_hin_cd,
                            matl2.old_matl_num AS kos_hin_cd,
                            ((bom.quantity / bom.basequantity))::NUMERIC(38, 8) AS kos_qut
                        FROM (
                            (
                                edw_sap_bw_dna_material_bomlist bom JOIN edw_material_dim matl1 ON (((bom.matl_num)::TEXT = (matl1.matl_num)::TEXT))
                                ) JOIN edw_material_dim matl2 ON (((bom.component)::TEXT = (matl2.matl_num)::TEXT))
                            )
                        WHERE (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        ((bom.plant)::TEXT = ('3118'::CHARACTER VARYING)::TEXT)
                                                        OR ((bom.plant)::TEXT = ('3119'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    AND ((bom.alternativebom)::TEXT = '01'::TEXT)
                                                    )
                                                AND (bom.bomstatus = 1)
                                                )
                                            AND (bom.validfrom <= to_date(convert_timezone('Asia/Singapore'::TEXT, '2024-07-01'::date::TIMESTAMP without TIME zone)))
                                            )
                                        AND (bom.validto >= to_date(convert_timezone('Asia/Singapore'::TEXT, '2024-07-01'::date::TIMESTAMP without TIME zone)))
                                        )
                                    AND (bom.validfrom_zvlfromc <= to_date(convert_timezone('Asia/Singapore'::TEXT, '2024-07-01'::date::TIMESTAMP without TIME zone)))
                                    )
                                AND (bom.validto_zvltoi >= to_date(convert_timezone('Asia/Singapore'::TEXT, '2024-07-01'::date::TIMESTAMP without TIME zone)))
                                )
                        ) base2
                    WHERE (
                            (base2.kos_hin_cd IS NULL)
                            OR ((base2.kos_hin_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
                            )
                    )
                )
            )
        )
GROUP BY base1.oya_hin_cd,
    base1.kos_hin_cd
)
select * from final
