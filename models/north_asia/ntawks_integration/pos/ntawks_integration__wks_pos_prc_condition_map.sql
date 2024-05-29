with itg_pos_prc_condition_map as (
select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_POS_PRC_CONDITION_MAP
),
edw_material_sales_dim as (
select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_MATERIAL_SALES_DIM
),
transformed as (
  select 
    pmap.sls_org, 
    sales_grp_cd, 
    cnd_type, 
    pmap.matl_num, 
    ltrim(sold_to_cust_cd, 0) as sold_to_cust_cd, 
    price, 
    vld_frm, 
    vld_to, 
    ctry_cd, 
    case when cnd_type = 'ZPR0' then price when (
      cnd_type = 'ZKTD' 
      and price > 0
    ) then (
      price * (-1)
    ) else price end as calc_price, 
    mat_sales_dim.sls_org sls_org_msd, 
    mat_sales_dim.matl_num as matl_num_msd, 
    mat_sales_dim.med_desc as matl_desc, 
    ltrim(mat_sales_dim.ean_num, 0) as ean_num 
  from 
    itg_pos_prc_condition_map pmap 
    inner join (
      select 
        matl_num, 
        med_desc, 
        ean_num, 
        sls_org 
      from 
        (
          select 
            ROW_NUMBER() over (
              PARTITION BY trim(
                ltrim(matl_num, '0')
              ), 
              sls_org 
              order by 
                ltrim(ean_num, 0) desc
            ) as rnum, 
            trim(
              ltrim(matl_num, '0')
            ) as matl_num, 
            med_desc, 
            ltrim(ean_num, 0) as ean_num, 
            sls_org 
          from 
            edw_material_sales_dim a 
          where 
            nullif(ean_num, '') is not null 
            and ean_num <> '0' 
            and ean_num <> 'N/A' 
            and ean_num <> 'NA'
        ) SRC 
      where 
        SRC.rnum = 1
    ) mat_sales_dim on pmap.sls_org = mat_sales_dim.sls_org 
    and ltrim(pmap.matl_num, 0) = ltrim(mat_sales_dim.matl_num, 0) 
    and pmap.cnd_type <> 'ZKSD'
),
final as (
select 
sls_org::varchar(25) as sls_org,
sales_grp_cd::varchar(18) as sales_grp_cd,
cnd_type::varchar(25) as cnd_type,
matl_num::varchar(40) as matl_num,
sold_to_cust_cd::varchar(100) as sold_to_cust_cd,
price::number(18,0) as price,
vld_frm::date as vld_frm,
vld_to::date as vld_to,
ctry_cd::varchar(25) as ctry_cd,
calc_price::number(18,0) as calc_price,
sls_org_msd::varchar(4) as sls_org_msd,
matl_num_msd::varchar(18) as matl_num_msd,
matl_desc::varchar(40) as matl_desc,
ean_num::varchar(18) as ean_num
from transformed
)
select * from final
