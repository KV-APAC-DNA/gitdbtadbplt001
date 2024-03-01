
with edw_vw_my_si_pos_inv_analysis as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_si_pos_inv_analysis') }}
),
edw_my_siso_analysis as
(
    select * from {{ ref('mysedw_integration__edw_my_siso_analysis') }}
),
edw_vw_my_customer_dim as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_customer_dim') }}
),
siso_analysis as
(
    select
      year,
      cast(qrtr_no as varchar(14)) as qrtr_no,
      cast(mnth_id as varchar(21)) as mnth_id,
      mnth_no,
      cntry_nm,
      data_src,
      dstrbtr_lvl2 as dstrbtr,
      dstrbtr_lvl3 as branch,
      dstrbtr_grp_cd,
      global_prod_franchise,
      global_prod_brand,
      global_prod_sub_brand,
      global_prod_variant,
      global_prod_segment,
      global_prod_subsegment,
      global_prod_category,
      global_prod_subcategory,
      sku, 
      sku_desc,
      bill_qty_pc,
      billing_grs_trd_sls,
      end_stock_qty,
      end_stock_val,
      sls_qty_pc,
      jj_grs_trd_sls,
      in_transit_qty,
      in_transit_val,
      ret_qty_pc,
      jj_ret_val
    from edw_my_siso_analysis
    where
      to_ccy = 'MYR'
),
si_pos_inv_analysis as
(select
      year,
      cast(qrtr_no as varchar(14)) as qrtr_no,
      cast(mnth_id as varchar(21)) as mnth_id,
      mnth_no,
      cntry_nm,
      data_src,
      null as dstrbtr,
      null as branch,
      dstrbtr_grp_cd,
      global_prod_franchise,
      global_prod_brand,
      global_prod_sub_brand,
      global_prod_variant,
      global_prod_segment,
      global_prod_subsegment,
      global_prod_category,
      global_prod_subcategory,
      sku, 
      sku_desc,
      bill_qty_pc,
      billing_grs_trd_sls,
      end_stock_qty,
      end_stock_val,
      sls_qty_pc,
      jj_grs_trd_sls,
      0 as in_transit_qty,
      0 as in_transit_val,
      0 as ret_qty_pc,
      0 as jj_ret_val
    from edw_vw_my_si_pos_inv_analysis
),
combined as
(
    select * from siso_analysis
    union all
    select * from si_pos_inv_analysis
),
a as
(
  select
    year,
    qrtr_no,
    mnth_id,
    mnth_no,
    dstrbtr,
    branch,
    dstrbtr_grp_cd,
    global_prod_franchise,
    global_prod_brand,
    global_prod_sub_brand,
    global_prod_variant,
    global_prod_segment,
    global_prod_subsegment,
    global_prod_category,
    global_prod_subcategory, /* ,global_put_up_desc */
    sku,
    sku_desc,
    sum(bill_qty_pc) as si_sls_qty,
    sum(billing_grs_trd_sls) as si_gts_val,
    sum((end_stock_qty + in_transit_qty)) as inventory_quantity,
    sum(nvl(end_stock_val + in_transit_val,null)) as inventory_val,
    sum(sls_qty_pc - ret_qty_pc) as so_sls_qty,
    sum(nvl(jj_grs_trd_sls - jj_ret_val,null)) as so_trd_sls
  from combined
  where
    upper(data_src) IN ('GT SELLOUT', 'SAP BW BILLING', 'GT INVENTORY', 'IN TRANSIT', 'INVPOS', 'SIPOS')
    and year >= ( date_part(year, current_timestamp()) - 6)
  group by
    year,
    qrtr_no,
    mnth_id,
    mnth_no,
    dstrbtr,
    branch,
    dstrbtr_grp_cd,
    global_prod_franchise,
    global_prod_brand,
    global_prod_sub_brand,
    global_prod_variant,
    global_prod_segment,
    global_prod_subsegment,
    global_prod_category,
    global_prod_subcategory,
    sku,
    sku_desc
),
b as
(
  select distinct
    ltrim(sap_cust_id, 0) as sap_cust_id,
    sap_prnt_cust_key,
    sap_prnt_cust_desc
  from edw_vw_my_customer_dim
),
final as
(
select
  year,
  qrtr_no,
  mnth_id,
  mnth_no,
  nvl(nullif((branch),''),'NA') as distributor,
  dstrbtr_grp_cd,
  sap_prnt_cust_key,
  sap_prnt_cust_desc,
  nvl(nullif(sku,''),'NA') as matl_num,
  sum(si_sls_qty) as si_sls_qty,
  sum(si_gts_val) as si_gts_val,
  sum(inventory_quantity) as inventory_quantity,
  sum(inventory_val) as inventory_val,
  sum(so_sls_qty) as so_sls_qty,
  sum(so_trd_sls) as so_trd_sls
from  a
left join  b
  on a.dstrbtr_grp_cd = b.sap_cust_id
group by
  year,
  qrtr_no,
  mnth_id,
  mnth_no,
  dstrbtr,
  branch,
  dstrbtr_grp_cd,
  sap_prnt_cust_key,
  sap_prnt_cust_desc,
  sku
)
select 
  year::number(18,0) as year,
  qrtr_no::varchar(14) as qrtr_no,
  mnth_id::varchar(21) as mnth_id,
  mnth_no::number(18,0) as mnth_no,
  distributor::varchar(40) as distributor,
  dstrbtr_grp_cd::varchar(30) as dstrbtr_grp_cd,
  sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
  sap_prnt_cust_desc::varchar(50) as sap_prnt_cust_desc,
  matl_num::varchar(100) as matl_num,
  si_sls_qty::number(38,4) as si_sls_qty,
  si_gts_val::number(38,13) as si_gts_val,
  inventory_quantity::number(38,4) as inventory_quantity,
  inventory_val::number(38,13) as inventory_val,
  so_sls_qty::number(38,6) as so_sls_qty,
  so_trd_sls::number(38,17) as so_trd_sls
 from final