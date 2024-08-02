
{{
    config(
        post_hook= "update {{this}} wks_edw_pos_fact_korea
                    set ean_num = ltrim (b.ean_cd ,0)
                    from {{ ref('ntaitg_integration__itg_sales_cust_prod_master') }} b 
                    where  ltrim (wks_edw_pos_fact_korea.ean_num,0) = ltrim(b.cust_prod_cd,0) 
                    and upper(trim(wks_edw_pos_fact_korea.src_sys_cd))= upper(trim(b.src_sys_cd)); "         
        )
}}

with itg_pos as (
select * from {{ ref('ntaitg_integration__itg_pos') }}
),
itg_sales_store_master as (
select * from {{ ref('ntaitg_integration__itg_sales_store_master') }}
),
itg_pos_str_sls_grp_map as (
select * from {{ source('ntaitg_integration','itg_pos_str_sls_grp_map') }}
),
edw_customer_attr_flat_dim as (
select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
edw_product_attr_dim as (
select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
itg_sales_cust_prod_master as (
select * from {{ ref('ntaitg_integration__itg_sales_cust_prod_master') }}
),
transformed as (
SELECT src.pos_dt,
       src.vend_cd,
       src.vend_nm,
       src.prod_nm,
       CASE
         WHEN src.vend_prod_cd IS NULL OR src.vend_prod_cd = '' THEN '#'
         ELSE src.vend_prod_cd
       END AS vend_prod_cd,
       src.vend_prod_nm,
       src.brnd_nm,
       CASE
         WHEN src.barcode IS NULL OR src.barcode = '' THEN '#'
         ELSE ltrim(src.barcode,0)
       END AS ean_num,
       CASE
         WHEN src.str_num IS NULL OR src.str_num = '' THEN '#'
         ELSE ltrim (src.str_num,0 )
       END AS str_cd,
       CASE
         WHEN trim(src.str_nm) IS NULL OR src.str_nm = '' THEN trim(src.sm_store_nm)
         ELSE trim(src.str_nm)
       END AS str_nm ,                   /*11-Aug18 Changed to capture str_nm from store master when not present in ITG*/
       CASE
         WHEN src.sold_to_party IS NULL OR src.sold_to_party = '' THEN '#'
         ELSE ltrim (src.sold_to_party,0)
       END AS sold_to_party,
       src.sls_grp,
       src.mysls_brnd_nm,
       src.sls_qty,
       src.sls_amt,
       src.unit_prc_amt,
       src.sls_excl_vat_amt,
       src.stk_rtrn_amt,
       src.stk_recv_amt,
       src.avg_sell_qty,
       src.cum_ship_qty,
       src.cum_rtrn_qty,
       src.web_ordr_takn_qty,
       src.web_ordr_acpt_qty,
       src.dc_invnt_qty,
       src.invnt_qty,
       src.invnt_amt,
       src.invnt_dt,
       src.serial_num,
       src.prod_delv_type,
       src.prod_type,
       src.dept_cd,
       src.dept_nm,
       src.spec_1_desc,
       src.spec_2_desc,
       src.cat_big,
       src.cat_mid,
       src.cat_small,
       src.dc_prod_cd,
       src.cust_dtls,
       src.dist_cd,
       src.crncy_cd,
       src.src_txn_sts,
       src.src_seq_num,
       src.src_sys_cd,
       src.ctry_cd,
       src.mysls_catg,
       '' as  matl_num ,     -- src.sap_matl_num AS matl_num,
       '' as  matl_desc ,    -- e.matl_desc, /*old logic */
       src.prom_sls_amt,
       src.prom_prc_amt,
       'N' AS hist_flg,
       
       src.channel ,     /*Added for report */  
       src.store_type ,  /*Added for report */  
       CASE
         WHEN  src.sls_grp_cd IS NULL OR  src.sls_grp_cd = '' THEN sm.sls_grp_cd
         ELSE  src.sls_grp_cd
       END AS sls_grp_cd     ,   /*Added for report */  
       
       current_timestamp() AS crt_dttm,
       current_timestamp() AS updt_dttm
FROM (

      SELECT   a.*,
             b.sold_to_cd AS sold_to_party,
             CAST(a.ean_num AS VARCHAR(100)) AS barcode,
            CASE
               WHEN c.sls_grp IS NULL OR c.sls_grp = '' THEN a.src_sys_cd
               ELSE c.sls_grp
             END AS sls_grp,
             COALESCE(d.prod_hier_l4,'Others') AS mysls_brnd_nm,
             COALESCE(d.prod_hier_l3,'Others') AS mysls_catg,
                    --- d.sap_matl_num, /*old logic */
             NULL AS prom_sls_amt,
             NULL AS prom_prc_amt, 
             a.str_cd AS str_num
             , CASE
               WHEN b.sales_grp_cd IS NULL OR b.sales_grp_cd = '' THEN c.sls_grp_cd
               ELSE b.sales_grp_cd
             END AS sls_grp_cd           /*Added for report */  
             ,b.channel ,b.store_type   /*Added for report */  
             ,b.sm_store_nm as sm_store_nm /*11-Aug18 Added to capture str_nm from store master when not present in ITG*/
      FROM /*"+context.Schema_itg+"*/ itg_pos a
        LEFT JOIN (
                   SELECT  m.sales_grp_cd,n.sls_grp_cd ,
                           n.mysls_str_type ,
                          m.cust_store_cd, n.src_sys_cd ,
                           m.channel ,m.store_type,    /*Added for report */                  
                           m.store_nm as sm_store_nm ,  /*11-Aug18 Added to capture str_nm from store master when not present in ITG*/
                          CASE
                            WHEN m.sold_to IS NULL OR m.sold_to = '' THEN n.sold_to_party
                            ELSE m.sold_to
                          END AS sold_to_cd,
                          CASE
                            WHEN m.sold_to IS NULL OR m.sold_to = '' THEN 'TypeRule'
                            ELSE 'StrRule'
                          END AS RuleCatg
                   FROM /*"+context.Schema_itg+"*/ itg_sales_store_master m /*get sold to code for store code*/
                     LEFT JOIN (SELECT x.src_sys_cd,
                                       x.str_type,
                                       y.sls_grp,
                                       y.sold_to_party
                                      ,x.sls_grp_cd  ,x.mysls_str_type /* New added */
                                      
                                FROM /*"+context.Schema_itg+"*/ itg_pos_str_sls_grp_map x
                                  JOIN (SELECT  sls_grp_cd ,sls_grp, sold_to_party,
                                               ROW_NUMBER() OVER (PARTITION BY sls_grp_cd ,sls_grp ORDER BY sold_to_party ASC) AS rnk
                                        FROM edw_customer_attr_flat_dim
                                        WHERE cntry LIKE '%Korea%') y
                                    ON x.sls_grp_nm = y.sls_grp
                                   AND x.sls_grp_cd = y.sls_grp_cd
                                WHERE y.rnk = 1) n  /*get one sold to party code for same sales group, store type and retailer*/ 
                            ON 
                            m.sales_grp_cd = n.sls_grp_cd  
                  ) b
            ON (case when a.src_sys_cd='Emart' then LTRIM (a.str_cd,0) = LTRIM (b.cust_store_cd,0)
              and a.str_nm=b.sm_store_nm
              else (LTRIM (a.str_cd,0) = LTRIM (b.cust_store_cd,0))end
              AND b.src_sys_cd = a.src_sys_cd)
              
                LEFT JOIN (SELECT DISTINCT sls_grp,sls_grp_cd , /*Added for report */      
                          sold_to_party
                   FROM edw_customer_attr_flat_dim
                   WHERE sold_to_party IS NOT NULL
                   AND   sold_to_party <> '') c ON ltrim (c.sold_to_party,0) = ltrim(b.sold_to_cd,0)
            LEFT JOIN (       
                  SELECT DISTINCT LTRIM(ean,0) as ean,  --- sap_matl_num,  /* Need to remove the matl_num , old logic &*/
						-- /*Ltrim EAN to remove duplicate records because of leading zeroes in EAN*/
                          prod_hier_l4,
                          prod_hier_l3,
                          cntry
                   FROM edw_product_attr_dim where cntry= 'KR'  
                    union                 
                select m.cust_prod_cd as ean ,
                 ---- p.sap_matl_num,  /* Need to remove the matl_num , old logic */
                p.prod_hier_l4,p.prod_hier_l3 ,ctry_cd as cntry
                from itg_sales_cust_prod_master m left outer join edw_product_attr_dim p
                on ltrim(m.ean_cd,0 )= ltrim(p.ean,0 ) where m.ctry_cd = 'KR' and  p.cntry= 'KR'
                ) d
               
		ON rtrim(CAST (a.ean_num AS VARCHAR (40))) =rtrim( CAST (ltrim(d.ean,0) AS VARCHAR (40))) 
                    ---- ON CAST (a.ean_num AS VARCHAR (40)) = CAST (d.ean AS VARCHAR (40)) /* Changing as product dim ean have 0 */
              AND a.ctry_cd = d.cntry
              
      WHERE a.ctry_cd = 'KR'

      ) src
 /* LEFT JOIN (SELECT DISTINCT matl_num, matl_desc
             FROM edw_material_dim) e ON COALESCE (LTRIM (e.matl_num,0),'#') = LTRIM (COALESCE (src.sap_matl_num,'#'),0) */  --old logic
             
  left outer join itg_pos_str_sls_grp_map sm on src.sls_grp  = sm.sls_grp_nm 
),
final as (
select
pos_dt::date as pos_dt,
vend_cd::varchar(40) as vend_cd,
vend_nm::varchar(100) as vend_nm,
prod_nm::varchar(100) as prod_nm,
vend_prod_cd::varchar(40) as vend_prod_cd,
vend_prod_nm::varchar(600) as vend_prod_nm,
brnd_nm::varchar(40) as brnd_nm,
ean_num::varchar(100) as ean_num,
str_cd::varchar(40) as str_cd,
str_nm::varchar(100) as str_nm,
sold_to_party::varchar(100) as sold_to_party,
sls_grp::varchar(100) as sls_grp,
mysls_brnd_nm::varchar(500) as mysls_brnd_nm,
sls_qty::number(18,0) as sls_qty,
sls_amt::number(16,5) as sls_amt,
unit_prc_amt::number(16,5) as unit_prc_amt,
sls_excl_vat_amt::number(16,5) as sls_excl_vat_amt,
stk_rtrn_amt::number(16,5) as stk_rtrn_amt,
stk_recv_amt::number(16,5) as stk_recv_amt,
avg_sell_qty::number(16,5) as avg_sell_qty,
cum_ship_qty::number(18,0) as cum_ship_qty,
cum_rtrn_qty::number(18,0) as cum_rtrn_qty,
web_ordr_takn_qty::number(18,0) as web_ordr_takn_qty,
web_ordr_acpt_qty::number(18,0) as web_ordr_acpt_qty,
dc_invnt_qty::number(18,0) as dc_invnt_qty,
invnt_qty::number(18,0) as invnt_qty,
invnt_amt::number(16,5) as invnt_amt,
invnt_dt::date as invnt_dt,
serial_num::varchar(40) as serial_num,
prod_delv_type::varchar(40) as prod_delv_type,
prod_type::varchar(40) as prod_type,
dept_cd::varchar(40) as dept_cd,
dept_nm::varchar(100) as dept_nm,
spec_1_desc::varchar(100) as spec_1_desc,
spec_2_desc::varchar(100) as spec_2_desc,
cat_big::varchar(100) as cat_big,
cat_mid::varchar(40) as cat_mid,
cat_small::varchar(40) as cat_small,
dc_prod_cd::varchar(40) as dc_prod_cd,
cust_dtls::varchar(100) as cust_dtls,
dist_cd::varchar(40) as dist_cd,
crncy_cd::varchar(10) as crncy_cd,
src_txn_sts::varchar(40) as src_txn_sts,
src_seq_num::number(18,0) as src_seq_num,
src_sys_cd::varchar(30) as src_sys_cd,
ctry_cd::varchar(10) as ctry_cd,
mysls_catg::varchar(500) as mysls_catg,
matl_num::varchar(1) as matl_num,
matl_desc::varchar(1) as matl_desc,
prom_sls_amt::varchar(1) as prom_sls_amt,
prom_prc_amt::varchar(1) as prom_prc_amt,
hist_flg::varchar(1) as hist_flg,
channel::varchar(25) as channel,
store_type::varchar(25) as store_type,
sls_grp_cd::varchar(20) as sls_grp_cd,
crt_dttm::timestamp_ntz(9) as crt_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final