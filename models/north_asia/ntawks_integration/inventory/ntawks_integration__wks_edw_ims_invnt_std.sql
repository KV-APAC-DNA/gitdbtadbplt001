with itg_ims_invnt as(
    select * from {{ ref('ntaitg_integration__itg_ims_invnt') }}
),
itg_tw_ims_dstr_prod_map as(
    select * from {{ ref('ntaitg_integration__itg_tw_ims_dstr_prod_map') }}
),
itg_query_parameters as(
    select * from {{ source('ntaitg_integration', 'itg_query_parameters') }}
),
edw_material_sales_dim as(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
transformed as(
    SELECT x.invnt_dt,
       x.dstr_cd,
       x.dstr_nm,
       x.prod_cd,
       x.prod_nm,
       CASE
         WHEN x.ctry_cd = 'TW' AND lkp1.ean_cd IS NOT NULL THEN lkp1.ean_cd
         WHEN x.ctry_cd = 'HK' AND lkp2.ean_cd IS NOT NULL THEN lkp2.ean_cd
         ELSE COALESCE(x.ean_num,'#')
       END AS ean_num,
       x.cust_nm,
       x.invnt_qty,
       x.invnt_amt,
       x.avg_prc_amt,
       x.safety_stock,
       x.bad_invnt_qty,
       x.book_invnt_qty,
       x.convs_amt,
       x.prch_disc_amt,
       x.end_invnt_qty,
       x.batch_no,
       x.uom,
       x.sls_rep_cd,
       x.sls_rep_nm,
       x.ctry_cd,
       x.crncy_cd,
       x.crt_dttm,
       current_timestamp()::timestamp_ntz(9) AS updt_dttm,
       chn_uom,
        x.storage_name,
        qp.area
    FROM itg_ims_invnt x
    LEFT JOIN itg_tw_ims_dstr_prod_map lkp1
            ON x.dstr_cd = lkp1.dstr_cd
            AND (x.prod_cd = lkp1.dstr_prod_cd
            OR TRIM (UPPER (x.prod_nm)) = TRIM (UPPER (lkp1.dstr_prod_nm)))
    LEFT JOIN (SELECT MAX(ean_num) AS ean_cd,
                        matl_num
                FROM (SELECT DISTINCT LTRIM(REPLACE(REPLACE(ean_num,' ',''),'-',''),0) AS ean_num,
                            LTRIM(matl_num,0) AS matl_num
                    FROM edw_material_sales_dim
                    WHERE ean_num IS NOT NULL
                    AND   ean_num NOT IN ('','NA','TBC','N/A','NIL','TBA','#N/A','NOT APPLICABLE','NIA','N/AA','N./A','MA','0')
                    AND   sls_org IN ('1110','110S')
                    AND   dstr_chnl = 10)
                GROUP BY matl_num) lkp2 ON LTRIM (x.prod_cd,0) = lkp2.matl_num
    left join (select parameter_name as storage_name,parameter_value as area from itg_query_parameters where country_code='TW' and parameter_type='Area') qp
                on x.storage_name=qp.storage_name
    WHERE x.dstr_cd in ('107479','107485','107501','107507','107510','116047','120812','122296','123291','131953','132349','132508','135307','135561','107482','107483','132222','136454','134478')
)
select * from transformed