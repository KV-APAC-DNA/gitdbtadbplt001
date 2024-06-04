{{
    config(
        pre_hook= '{{build_itg_ims_invnt_temp()}}'
    )
}}
with sdl_tw_ims_dstr_std_stock as(
    select * from {{ ref('ntaitg_integration__sdl_tw_ims_dstr_std_stock') }}
),
tw_ims_distributor_ingestion_metadata as(
    select * from {{ source('ntawks_integration', 'tw_ims_distributor_ingestion_metadata') }}
),
itg_ims_invnt as(
    select * from {{ source('ntaitg_integration', 'itg_ims_invnt_temp') }}
),
stock as
(
    SELECT 
        SRC.invnt_dt,
       SRC.dstr_cd,
       dstr_nm,
       SRC.prod_cd,
       prod_nm,
       SRC.ean_num,
       cust_nm,
       invnt_qty,
       invnt_amt,
       avg_prc_amt,
       safety_stock,
       bad_invnt_qty,
       book_invnt_qty,
       convs_amt,
       prch_disc_amt,
       end_invnt_qty,
       batch_no,
       uom,
       sls_rep_cd,
       sls_rep_nm,
       SRC.ctry_cd,
       crncy_cd,
       SRC.chn_uom,
       null as storage_name,
       TGT.CRT_DTTM AS TGT_CRT_DTTM,
       current_timestamp()::timestamp_ntz(9) AS UPDT_DTTM,
       CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
       END AS CHNG_FLG
    FROM 
        (
            SELECT 
                inventory_date AS invnt_dt,
                stock.distributor_code AS dstr_cd,
                COALESCE(meta.dstr_nm,'#') AS dstr_nm,
                distributor_product_code AS prod_cd,
                --MAX(chn_desp) AS prod_nm, -- need to confirm
                MAX(distributors_product_name) AS prod_nm,
                -- need to confirm
                ean AS ean_num,
                NULL AS cust_nm,
                SUM(quantity) AS invnt_qty,
                SUM(total_cost) AS invnt_amt,
                NULL AS avg_prc_amt,
                NULL AS safety_stock,
                NULL AS bad_invnt_qty,
                NULL AS book_invnt_qty,
                NULL AS convs_amt,
                NULL AS prch_disc_amt,
                NULL AS end_invnt_qty,
                NULL AS batch_no,
                NULL AS uom,
                NULL AS sls_rep_cd,
                NULL AS sls_rep_nm,
                'TW' AS ctry_cd,
                'TWD' AS crncy_cd,
                MAX(uom) AS chn_uom
            FROM sdl_tw_ims_dstr_std_stock stock
            LEFT JOIN 
            (
                SELECT 
                    DISTINCT ctry_cd,
                    distributor_code,
                    dstr_nm
                FROM tw_ims_distributor_ingestion_metadata
                WHERE subject_area = 'Stock-Data'
                AND   ctry_cd = 'TW'
            ) meta ON stock.distributor_code = meta.distributor_code
            where stock.distributor_code <> '134478'
            GROUP BY 
            inventory_date,
            distributor_product_code,
            stock.distributor_code,
            ean,
            meta.dstr_nm
        ) src
        LEFT JOIN 
        (
            SELECT 
                invnt_dt,
                prod_cd,
                ean_num,
                ctry_cd,
                crt_dttm
            FROM itg_ims_invnt
            WHERE dstr_cd IN 
                (SELECT DISTINCT distributor_code FROM tw_ims_distributor_ingestion_metadata
                            WHERE subject_area = 'Stock-Data' AND   ctry_cd = 'TW' and distributor_code <> '134478')
        ) TGT
        ON COALESCE (src.invnt_dt,'1/1/1900') = COALESCE (tgt.invnt_dt,'1/1/1900')
        AND COALESCE (src.prod_cd,'#') = COALESCE (tgt.prod_cd,'#')
        AND COALESCE (src.ean_num,'#') = COALESCE (tgt.ean_num,'#')
        AND COALESCE (src.ctry_cd,'#') = COALESCE (tgt.ctry_cd,'#')
),
d137488 as 
(
    SELECT 
        SRC.invnt_dt,
        SRC.dstr_cd,
        dstr_nm,
        SRC.prod_cd,
        prod_nm,
        SRC.ean_num,
        cust_nm,
        invnt_qty,
        invnt_amt,
        avg_prc_amt,
        safety_stock,
        bad_invnt_qty,
        book_invnt_qty,
        convs_amt,
        prch_disc_amt,
        end_invnt_qty,
        batch_no,
        uom,
        sls_rep_cd,
        sls_rep_nm,
        SRC.ctry_cd,
        crncy_cd,
        SRC.chn_uom,
        SRC.storage_name,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        current_timestamp()::timestamp_ntz(9) AS UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM 
        (
            SELECT 
                inventory_date AS invnt_dt,
                stock.distributor_code AS dstr_cd,
                COALESCE(meta.dstr_nm, '#') AS dstr_nm,
                distributor_product_code AS prod_cd,
                --MAX(chn_desp) AS prod_nm, -- need to confirm
                MAX(distributors_product_name) AS prod_nm,
                -- need to confirm
                ean AS ean_num,
                NULL AS cust_nm,
                SUM(quantity) AS invnt_qty,
                SUM(total_cost) AS invnt_amt,
                NULL AS avg_prc_amt,
                NULL AS safety_stock,
                NULL AS bad_invnt_qty,
                NULL AS book_invnt_qty,
                NULL AS convs_amt,
                NULL AS prch_disc_amt,
                NULL AS end_invnt_qty,
                NULL AS batch_no,
                NULL AS uom,
                NULL AS sls_rep_cd,
                NULL AS sls_rep_nm,
                'TW' AS ctry_cd,
                'TWD' AS crncy_cd,
                MAX(uom) AS chn_uom,
                storage_name
            FROM sdl_tw_ims_dstr_std_stock stock
                LEFT JOIN 
                (
                    SELECT DISTINCT ctry_cd,
                        distributor_code,
                        dstr_nm
                    FROM tw_ims_distributor_ingestion_metadata
                    WHERE subject_area = 'Stock-Data'
                        AND ctry_cd = 'TW' and distributor_code = '134478'
                ) meta ON stock.distributor_code = meta.distributor_code
            where stock.distributor_code = '134478' and filename not like '%PXStock%' and filename not like '%PXStore%'
            GROUP BY inventory_date,
                distributor_product_code,
                stock.distributor_code,
                ean,
                meta.dstr_nm,
                storage_name,
                filename
        ) src
        LEFT JOIN 
        (
            SELECT invnt_dt,
                prod_cd,
                ean_num,
                ctry_cd,
                crt_dttm
            FROM itg_ims_invnt
            WHERE dstr_cd IN (
                    SELECT DISTINCT distributor_code
                    FROM tw_ims_distributor_ingestion_metadata
                    WHERE subject_area = 'Stock-Data'
                        AND ctry_cd = 'TW'
                )
                and dstr_nm = 'Jia Wang'
        ) TGT ON COALESCE (src.invnt_dt, '1/1/1900') = COALESCE (tgt.invnt_dt, '1/1/1900')
        AND COALESCE (src.prod_cd, '#') = COALESCE (tgt.prod_cd, '#')
        AND COALESCE (src.ean_num, '#') = COALESCE (tgt.ean_num, '#')
        AND COALESCE (src.ctry_cd, '#') = COALESCE (tgt.ctry_cd, '#')
),
pxstore as
(
    SELECT 
        SRC.invnt_dt,
        SRC.dstr_cd,
        dstr_nm,
        SRC.prod_cd,
        prod_nm,
        SRC.ean_num,
        cust_nm,
        invnt_qty,
        invnt_amt,
        avg_prc_amt,
        safety_stock,
        bad_invnt_qty,
        book_invnt_qty,
        convs_amt,
        prch_disc_amt,
        end_invnt_qty,
        batch_no,
        uom,
        sls_rep_cd,
        sls_rep_nm,
        SRC.ctry_cd,
        crncy_cd,
        SRC.chn_uom,
        SRC.storage_name,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        current_timestamp()::timestamp_ntz(9) AS UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM
    (
        SELECT 
            inventory_date AS invnt_dt,
            stock.distributor_code AS dstr_cd,
            case 
            when filename like '%PXStock%' then dstr_nm||'_PXStock'
            when filename like '%PXStore%' then dstr_nm||'_PXStore'
            else
            COALESCE(meta.dstr_nm,'#') end AS dstr_nm,
            distributor_product_code AS prod_cd,
            --MAX(chn_desp) AS prod_nm, -- need to confirm
            MAX(distributors_product_name) AS prod_nm,
            -- need to confirm
            ean AS ean_num,
            NULL AS cust_nm,
            SUM(quantity) AS invnt_qty,
            SUM(total_cost) AS invnt_amt,
            NULL AS avg_prc_amt,
            NULL AS safety_stock,
            NULL AS bad_invnt_qty,
            NULL AS book_invnt_qty,
            NULL AS convs_amt,
            NULL AS prch_disc_amt,
            NULL AS end_invnt_qty,
            NULL AS batch_no,
            NULL AS uom,
            NULL AS sls_rep_cd,
            NULL AS sls_rep_nm,
            'TW' AS ctry_cd,
            'TWD' AS crncy_cd,
            MAX(uom) AS chn_uom,
            storage_name
        FROM sdl_tw_ims_dstr_std_stock stock
        LEFT JOIN 
        (
            SELECT 
                DISTINCT ctry_cd,
                distributor_code,
                dstr_nm
            FROM tw_ims_distributor_ingestion_metadata
            WHERE subject_area = 'Stock-Data'
            AND ctry_cd = 'TW'
            and distributor_code = '134478'
        ) meta ON stock.distributor_code = meta.distributor_code
        where stock.distributor_code = '134478' and filename like '%PXStore%'
        GROUP BY 
            inventory_date,
            distributor_product_code,
            stock.distributor_code,
            ean,
            meta.dstr_nm,storage_name,filename
    ) src
    LEFT JOIN 
    (
        SELECT 
            invnt_dt,
            prod_cd,
            ean_num,
            ctry_cd,
            crt_dttm
        FROM itg_ims_invnt
        WHERE dstr_cd IN 
        (
            SELECT DISTINCT distributor_code FROM tw_ims_distributor_ingestion_metadata
            WHERE subject_area = 'Stock-Data' AND ctry_cd = 'TW'
        ) 
        and dstr_nm ='Jia Wang_PXStore'
    ) TGT
    ON COALESCE (src.invnt_dt,'1/1/1900') = COALESCE (tgt.invnt_dt,'1/1/1900')
    AND COALESCE (src.prod_cd,'#') = COALESCE (tgt.prod_cd,'#')
    AND COALESCE (src.ean_num,'#') = COALESCE (tgt.ean_num,'#')
    AND COALESCE (src.ctry_cd,'#') = COALESCE (tgt.ctry_cd,'#')
),
pxstock as
(
    SELECT 
        SRC.invnt_dt,
        SRC.dstr_cd,
        dstr_nm,
        SRC.prod_cd,
        prod_nm,
        SRC.ean_num,
        cust_nm,
        invnt_qty,
        invnt_amt,
        avg_prc_amt,
        safety_stock,
        bad_invnt_qty,
        book_invnt_qty,
        convs_amt,
        prch_disc_amt,
        end_invnt_qty,
        batch_no,
        uom,
        sls_rep_cd,
        sls_rep_nm,
        SRC.ctry_cd,
        crncy_cd,
        SRC.chn_uom,
        SRC.storage_name,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        current_timestamp()::timestamp_ntz(9) AS UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM 
        (
            SELECT inventory_date AS invnt_dt,
                stock.distributor_code AS dstr_cd,
                case
                    when filename like '%PXStock%' then dstr_nm || '_PXStock'
                    when filename like '%PXStore%' then dstr_nm || '_PXStore'
                    else COALESCE(meta.dstr_nm, '#')
                end AS dstr_nm,
                distributor_product_code AS prod_cd,
                --MAX(chn_desp) AS prod_nm, -- need to confirm
                MAX(distributors_product_name) AS prod_nm,
                -- need to confirm
                ean AS ean_num,
                NULL AS cust_nm,
                SUM(quantity) AS invnt_qty,
                SUM(total_cost) AS invnt_amt,
                NULL AS avg_prc_amt,
                NULL AS safety_stock,
                NULL AS bad_invnt_qty,
                NULL AS book_invnt_qty,
                NULL AS convs_amt,
                NULL AS prch_disc_amt,
                NULL AS end_invnt_qty,
                NULL AS batch_no,
                NULL AS uom,
                NULL AS sls_rep_cd,
                NULL AS sls_rep_nm,
                'TW' AS ctry_cd,
                'TWD' AS crncy_cd,
                MAX(uom) AS chn_uom,
                storage_name
            FROM sdl_tw_ims_dstr_std_stock stock
                LEFT JOIN 
                (
                    SELECT DISTINCT ctry_cd,
                        distributor_code,
                        dstr_nm
                    FROM tw_ims_distributor_ingestion_metadata
                    WHERE subject_area = 'Stock-Data'
                        AND ctry_cd = 'TW' and distributor_code = '134478' 
                ) meta ON stock.distributor_code = meta.distributor_code
            where stock.distributor_code = '134478' and filename like '%PXStock%'
            GROUP BY inventory_date,
                distributor_product_code,
                stock.distributor_code,
                ean,
                meta.dstr_nm,
                storage_name,
                filename
        ) src
        LEFT JOIN 
        (
            SELECT invnt_dt,
                prod_cd,
                ean_num,
                ctry_cd,
                crt_dttm
            FROM itg_ims_invnt
            WHERE dstr_cd IN 
            (
                SELECT DISTINCT distributor_code FROM tw_ims_distributor_ingestion_metadata
                WHERE subject_area = 'Stock-Data' AND ctry_cd = 'TW' and distributor_code = '134478'
            )
            and dstr_nm = 'Jia Wang_PXStock'
        ) TGT 
        ON COALESCE (src.invnt_dt, '1/1/1900') = COALESCE (tgt.invnt_dt, '1/1/1900')
        AND COALESCE (src.prod_cd, '#') = COALESCE (tgt.prod_cd, '#')
        AND COALESCE (src.ean_num, '#') = COALESCE (tgt.ean_num, '#')
        AND COALESCE (src.ctry_cd, '#') = COALESCE (tgt.ctry_cd, '#')
),
transformed as
(
    select * from stock
    union all
    select * from pxstock
    union all
    select * from pxstore
    union all
    select * from d137488
)
select * from transformed