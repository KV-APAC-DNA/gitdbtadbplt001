with source as 
(
    select * from {{ ref('phledw_integration__edw_ph_hcp_selllout_final') }}
),
final  as 
(
    select 
data_src as "data_src",
jj_mnth_id as "jj_mnth_id",
jj_year as "jj_year",
store_code as "store_code",
sku as "sku",
group_variant_code  as "group_variant_code",
territory_code as "territory_code",
team_code as "team_code",
district_code as "district_code",
sellout_target  as "sellout_target",
sell_out  as "sell_out" ,
customer_count_target as "customer_count_target"  ,
 cust_code  as "cust_code",
     cust_name  as "cust_name" ,
     cust_direct_manager_code  as "cust_direct_manager_code" 
    from source
)
select * from final 
