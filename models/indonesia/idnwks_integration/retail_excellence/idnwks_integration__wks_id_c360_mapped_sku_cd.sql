--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),

--final cte
wks_id_c360_mapped_sku_cd as (
    select * from (SELECT DISTINCT ltrim(msl_product_code,'0') AS put_up,
       LTRIM(sku_code,'0') AS sku_code,
	   msl_product_desc as sku_description, 
       ROW_NUMBER() OVER (PARTITION BY ltrim(msl_product_code,0) ORDER BY cal_date DESC, LENGTH(LTRIM(sku_code,'0')) DESC) AS rno
FROM edw_rpt_regional_sellout_offtake
WHERE COUNTRY_CODE = 'ID'
AND   data_source in ('SELL-OUT')) WHERE rno = 1
),

final as (

    select 
    put_up::varchar(150) as put_up,
    sku_code::varchar(40) as sku_code,
    sku_description::varchar(300) as sku_description,
    rno::numeric(38,0) as rno
    from wks_id_c360_mapped_sku_cd
)

--final select 
select * from final

