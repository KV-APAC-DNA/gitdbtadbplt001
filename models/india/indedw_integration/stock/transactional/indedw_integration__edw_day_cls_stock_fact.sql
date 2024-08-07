{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
                    delete from {{this}} USING {{ ref('indwks_integration__day_cls_stock_fact') }}
                    WHERE {{ ref('indwks_integration__day_cls_stock_fact') }}.distcode = {{this}}.customer_code
                    AND {{ ref('indwks_integration__day_cls_stock_fact') }}.transdate = {{this}}.stock_date
                    AND {{ ref('indwks_integration__day_cls_stock_fact') }}.prdcode = {{this}}.product_code;
                    {% endif %}"
    )
}}

with day_cls_stock_fact as 
(
    select * from {{ ref('indwks_integration__day_cls_stock_fact') }}
),
final as 
(
    SELECT 
        distcode::varchar(50) as customer_code,
        transdate::timestamp_ntz(9) as stock_date,
        prdcode::varchar(100) as product_code,
        salclsstock::number(18,0) as salclsstock,
        salclsstock * nr::float as salclsstockval,
        unsalclsstock::number(18,0) as unsalclsstock,
        offerclsstock::number(18,0) as offerclsstock,
        salstockin::number(18,0) as salstockin,
        salstockout::number(18,0) as salstockout,
        unsalstockin::number(18,0) as unsalstockin,
        unsalstockout::number(18,0) as unsalstockout,
        offerstockin::number(18,0) as offerstockin,
        offerstockout::number(18,0) as offerstockout,
        NR::float as nr
    FROM day_cls_stock_fact
)
select * from final
