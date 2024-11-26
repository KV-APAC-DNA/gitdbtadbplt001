





WITH dcr_aggregated AS (

    SELECT

        dcr.v_custid,
        dcr.dcr_dt,

        COUNT(DISTINCT dcr.dcr_dt) AS actual_visit,

        CASE

            WHEN hcp.core_noncore = 'Core' THEN 2

            ELSE 1

        END AS calls_planned,

        CASE

            WHEN hcp.core_noncore = 'Core' AND COUNT(DISTINCT dcr.dcr_dt) > 2 THEN 2

            WHEN hcp.core_noncore = 'Non-Core' AND COUNT(DISTINCT dcr.dcr_dt) > 1 THEN 1

            ELSE COUNT(DISTINCT dcr.dcr_dt)

        END AS calls_done

    FROM {{ ref(hcpitg_integration__itg_hcp360_in_ventasys_dcrdata) }} dcr

    JOIN {{ source('dev_dna_core', 'hcpitg_integration_itg_hcp360_in_ventasys_hcp_master') }} hcp

        ON dcr.v_custid = hcp.v_custid

    GROUP BY

        dcr.v_custid, dcr.dcr_dt, hcp.core_noncore

)

SELECT * 

FROM dcr_aggregated 