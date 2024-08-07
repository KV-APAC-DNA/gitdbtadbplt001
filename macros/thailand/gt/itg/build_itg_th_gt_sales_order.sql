{% macro build_itg_th_gt_sales_order(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started building itg table -> itg_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    
    {{ log("Setting query to delete records from itg table -> itg_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_from_itg_query %}
    DELETE FROM 
                {% if target.name=='prod' %}
                    thaitg_integration.itg_th_gt_sales_order
                {% else %}
                    {{schema}}.thaitg_integration__itg_th_gt_sales_order
                {% endif %}	
    where (UPPER(saleunit),orderdate) in (
        select DISTINCT UPPER(saleunit),orderdate from
                {% if target.name=='prod' %}
                    thawks_integration.wks_th_gt_sales_order_flag_incl
                {% else %}
                    {{schema}}.thawks_integration__wks_th_gt_sales_order_flag_incl
                {% endif %}	
        );
    {% endset %}
    { log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query set to delete records from itg table -> itg_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running the query to delete records from itg table -> itg_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_from_itg_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running the query to delete records from itg table -> itg_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build itg table -> itg_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_itg_model %}
        insert into  {% if target.name=='prod' %}
                        thaitg_integration.itg_th_gt_sales_order
                    {% else %}
                        {{schema}}.thaitg_integration__itg_th_gt_sales_order
                    {% endif %}	
            with source as (
                select  * from 
                {% if target.name=='prod' %}
                    thawks_integration.wks_th_gt_sales_order_flag_incl
                {% else %}
                    {{schema}}.thawks_integration__wks_th_gt_sales_order_flag_incl
                {% endif %}	
            ),
            final as (
            SELECT 
                cntry_cd::varchar(5) as cntry_cd,
                crncy_cd::varchar(5) as crncy_cd,
                saleunit::varchar(10) as saleunit,
                orderid::varchar(50) as orderid,
                orderdate::date as orderdate,
                customer_id::varchar(50) as customer_id,
                customer_name::varchar(150) as customer_name,
                city::varchar(50) as city,
                region::varchar(50) as region,
                saledistrict::varchar(50) as saledistrict,
                saleoffice::varchar(50) as saleoffice,
                salegroup::varchar(50) as salegroup,
                customertype::varchar(50) as customertype,
                storetype::varchar(50) as storetype,
                saletype::varchar(50) as saletype,
                salesemployee::varchar(50) as salesemployee,
                salename::varchar(150) as salename,
                productid::varchar(50) as productid,
                productname::varchar(150) as productname,
                megabrand::varchar(50) as megabrand,
                brand::varchar(50) as brand,
                baseproduct::varchar(50) as baseproduct,
                variant::varchar(50) as variant,
                putup::varchar(50) as putup,
                priceref::number(18,6) as priceref,
                backlog::number(18,6) as backlog,
                qty::number(18,6) as qty,
                subamt1::number(18,6) as subamt1,
                discount::number(18,6) as discount,
                subamt2::number(18,6) as subamt2,
                discountbtline::number(18,6) as discountbtline,
                totalbeforevat::number(18,6) as totalbeforevat,
                total::number(18,6) as total,
                sales_order_line_no::varchar(10) as sales_order_line_no,
                cancelled::varchar(10) as cancelled,
                documentid::varchar(50) as documentid,
                return_reason::varchar(150) as return_reason,
                promotioncode::varchar(50) as promotioncode,
                promotioncode1::varchar(50) as promotioncode1,
                promotioncode2::varchar(50) as promotioncode2,
                promotioncode3::varchar(50) as promotioncode3,
                promotioncode4::varchar(50) as promotioncode4,
                promotioncode5::varchar(50) as promotioncode5,
                promotion_code::varchar(50) as promotion_code,
                promotion_code2::varchar(50) as promotion_code2,
                promotion_code3::varchar(50) as promotion_code3,
                avgdiscount::number(18,6) as avgdiscount,
                ordertype::varchar(10) as ordertype,
                approverstatus::varchar(10) as approverstatus,
                pricelevel::varchar(10) as pricelevel,
                optional::date as optional,
                deliverydate::date as deliverydate,
                ordertime::varchar(50) as ordertime,
                shipto::varchar(50) as shipto,
                billto::varchar(50) as billto,
                deliveryrouteid::varchar(50) as deliveryrouteid,
                approved_date::date as approved_date,
                approved_time::varchar(50) as approved_time,
                ref_15::varchar(255) as ref_15,
                paymenttype::varchar(50) as paymenttype,
                load_flag::varchar(2) as load_flag,
                filename::varchar(100) as filename,
                run_id::varchar(50) as run_id,
                crt_dttm::timestamp_ntz(9) as crt_dttm
            from source
            )
            select * from final;
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query setting completed to build itg table -> itg_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running query to build itg table -> itg_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_itg_model) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to build itg table -> itg_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to delete records from wks staging table -> wks_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_wks_staging_data_by_file_query %}
    delete from {{ ref('thawks_integration__wks_th_gt_sales_order') }} where filename= '{{filename}}';
    {% endset %}
    {{ log("Started running query to delete records from wks staging table -> wks_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_wks_staging_data_by_file_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to delete records from wks staging table -> wks_th_gt_sales_order for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
{% endmacro %}