{{
    config(
        pre_hook="{{build_itg_ims_invnt()}}"
    )
}}

with sdl_tw_ims_dstr_std_stock as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_ims_dstr_std_stock') }}
),
tw_ims_distributor_ingestion_metadata as(
    select * from {{ source('ntawks_integration', 'tw_ims_distributor_ingestion_metadata') }}
),
src as(
    select inventory_date AS invnt_dt,
             stock.distributor_code AS dstr_cd,
             coalesce(meta.dstr_nm,'#') AS dstr_nm,
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
      from sdl_tw_ims_dstr_std_stock stock
        left join (select DISTINCT ctry_cd,
                          distributor_code,
                          dstr_nm
                   from tw_ims_distributor_ingestion_metadata
                   where subject_area = 'Stock-Data'
                   AND   ctry_cd = 'TW') meta ON stock.distributor_code = meta.distributor_code
      GROUP BY inventory_date,
               distributor_product_code,
               stock.distributor_code,
               ean,
               meta.dstr_nm
),
tgt as(
    select invnt_dt,
                    prod_cd,
                    ean_num,
                    ctry_cd,
                    crt_dttm
             from ntaitg_integration__itg_ims_invnt
             where dstr_cd IN (select DISTINCT distributor_code
                               from tw_ims_distributor_ingestion_metadata
                               where subject_area = 'Stock-Data'
                               AND   ctry_cd = 'TW')
),
transformed as(
    select * from src
    left join tgt
         ON coalesce (src.invnt_dt,'1/1/1900') = coalesce (tgt.invnt_dt,'1/1/1900')
        and coalesce (src.prod_cd,'#') = coalesce (tgt.prod_cd,'#')
        and coalesce (src.ean_num,'#') = coalesce (tgt.ean_num,'#')
        and coalesce (src.ctry_cd,'#') = coalesce (tgt.ctry_cd,'#')
),
final as(
    SELECT src.invnt_dt,
       src.dstr_cd,
       dstr_nm,
       src.prod_cd,
       prod_nm,
       src.ean_num,
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
       src.ctry_cd,
       crncy_cd,
       src.chn_uom,
       TGT.CRT_DTTM AS TGT_CRT_DTTM,
       current_timestamp()::timestamp_ntz(9) AS UPDT_DTTM,
       CASE
         WHEN TGT.CRT_DTTM IS NULL THEN 'I'
         ELSE 'U'
       END AS CHNG_FLG
    from transformed
)
select * from final