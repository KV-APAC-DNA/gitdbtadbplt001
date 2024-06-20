with wks_edw_pos_fact_korea as (
select * from {{ ref('ntawks_integration__wks_edw_pos_fact_korea') }}
),
itg_pos_invoice_prc_lookup as (
select * from {{ ref('ntaitg_integration__itg_pos_invoice_prc_lookup') }}
),
wks_pos_prc_condition_map as (
select * from {{ ref('ntawks_integration__wks_pos_prc_condition_map') }}
),
final as (
select 
  pos_dt, 
  sold_to_party, 
  ean_num, 
  cnd_type, 
  matl_num, 
  matl_desc, 
  calc_price, 
  vld_frm, 
  vld_to 
from 
  (
    select 
      pos_data.pos_dt, 
      ltrim (pos_data.sold_to_party, 0) as sold_to_party, 
      ltrim (pos_data.ean_num, 0) as ean_num, 
      map.cnd_type, 
      map.matl_num, 
      map.matl_desc, 
      map.calc_price, 
      map.vld_frm, 
      map.vld_to, 
      ROW_NUMBER() OVER (
        PARTITION BY pos_data.pos_dt, 
        ltrim(pos_data.sold_to_party, 0), 
        ltrim(pos_data.ean_num, 0), 
        map.matl_num, 
        map.cnd_type 
        ORDER BY 
          map.vld_to desc, 
          map.vld_frm desc
      ) AS rownum 
    from 
      (
        select 
          pos_dt, 
          ltrim(sold_to_party, 0) as sold_to_party, 
          ltrim(ean_num, 0) as ean_num 
        from 
         wks_edw_pos_fact_korea minus 
        select 
          pos_dt, 
          ltrim(sold_to_party, 0) as sold_to_party, 
          ltrim(ean_num, 0) as ean_num 
        from 
          itg_pos_invoice_prc_lookup
      ) pos_data 
      inner join wks_pos_prc_condition_map map on 
      /*pos_data.pos_dt between map.vld_frm and map.vld_to and*/
      --REMOVED JOIN WITH POS DATE
      ltrim (pos_data.sold_to_party, 0) = ltrim (map.sold_to_cust_cd, 0) 
      and ltrim (pos_data.ean_num, 0) = ltrim (map.ean_num, 0) 
      and pos_data.pos_dt >= map.vld_frm 
      and cnd_type = 'ZPR0' 
      /* ADDED NEW Condition */
      
      /* Added to pick only Gross price Wave 2 logic 2 */
      ) wave2 
where 
  wave2.rownum = 1)
  select * from final 
