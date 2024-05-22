{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['dstrbtr_grp_cd', 'dstrbtr_cust_id', 'order_dt', 'invoice_dt', 'order_no', 'dstrbtr_prod_id', 'invoice_no'],
        pre_hook= ["delete from {{this}} where dstrbtr_grp_cd || dstrbtr_cust_id || nvl(to_char(order_dt, 'YYYYMMDD'), '') || to_char(invoice_dt, 'YYYYMMDD') || nvl(order_no, '') || nvl(invoice_no, '') || dstrbtr_prod_id in 
        ( select distinct dstrbtr_grp_cd || dstrbtr_cust_id || 
        nvl(to_char(case when try_to_date(order_dt,'MM/DD/YYYY HH12:MI:SS AM') is not null then try_to_date(order_dt,'MM/DD/YYYY HH12:MI:SS AM') else to_date(order_dt,'MM/DD/YYYY HH24:MI')    end,'YYYYMMDD'), '') || 
        to_char(case when try_to_date(invoice_dt,'MM/DD/YYYY HH12:MI:SS AM') is not null then try_to_date(invoice_dt,'MM/DD/YYYY HH12:MI:SS AM') else to_date(invoice_dt,'MM/DD/YYYY HH24:MI') end,'YYYYMMDD') || 
        nvl(order_no, '') || nvl(invoice_no, '') || dstrbtr_prod_id 
        from {{ source('phlsdl_raw', 'sdl_ph_dms_sellout_sales_fact') }} );"]
    )
}}

with sdl_ph_dms_sellout_sales_fact as(
    select *, 
        case 
            when try_to_date(order_dt,'MM/DD/YYYY HH12:MI:SS AM') is not null then try_to_date(order_dt,'MM/DD/YYYY HH12:MI:SS AM')
            else  to_date(order_dt,'MM/DD/YYYY HH24:MI') 
        end as order_dt_mod,
        case 
            when try_to_date(invoice_dt,'MM/DD/YYYY HH12:MI:SS AM') is not null then try_to_date(invoice_dt,'MM/DD/YYYY HH12:MI:SS AM')
            else  to_date(invoice_dt,'MM/DD/YYYY HH24:MI') 
        end as invoice_dt_mod
    from {{ source('phlsdl_raw', 'sdl_ph_dms_sellout_sales_fact') }}

),
source as (
    select *, dense_rank() over (partition by dstrbtr_grp_cd || dstrbtr_cust_id || nvl(to_char(order_dt_mod,'YYYYMMDD'), '') || to_char(invoice_dt_mod,'YYYYMMDD') || nvl(order_no, '') || nvl(invoice_no, '') || dstrbtr_prod_id order by cdl_dttm desc) as rnk 
    from sdl_ph_dms_sellout_sales_fact
    qualify rnk=1
),
final as(
    select 
        dstrbtr_grp_cd::varchar(20) as dstrbtr_grp_cd,
        cntry_cd::varchar(20) as cntry_cd,
        dstrbtr_cust_id::varchar(30) as dstrbtr_cust_id,
        
            case 
                when len(dstrbtr_cust_id) < 12
                    then case 
                            when substring(right('000000000000' || dstrbtr_cust_id, 12), 1, 3) = '000'
                                then dstrbtr_grp_cd || substring(right('000000000000' || dstrbtr_cust_id, 12), 4, 12)
                            when substring(right('000000000000' || dstrbtr_cust_id, 12), 1, 3) = dstrbtr_grp_cd
                                then right('000000000000' || dstrbtr_cust_id, 12)
                            else dstrbtr_grp_cd || right('000000000000' || dstrbtr_cust_id, 12)
                            end
                else case 
                        when substring(dstrbtr_cust_id, 1, 3) = '000'
                            then dstrbtr_grp_cd || substring(dstrbtr_cust_id, 4, 12)
                        when substring(dstrbtr_cust_id, 1, 3) = dstrbtr_grp_cd
                            then dstrbtr_cust_id
                        else dstrbtr_grp_cd || dstrbtr_cust_id
                        end
                end::varchar(30) as trnsfrm_cust_id,
        case 
            when try_to_date(order_dt,'MM/DD/YYYY HH12:MI:SS AM') is not null then try_to_date(order_dt,'MM/DD/YYYY HH12:MI:SS AM')
            else  to_date(order_dt,'MM/DD/YYYY HH24:MI') 
        end as order_dt,
        case 
            when try_to_date(invoice_dt,'MM/DD/YYYY HH12:MI:SS AM') is not null then try_to_date(invoice_dt,'MM/DD/YYYY HH12:MI:SS AM')
            else  to_date(invoice_dt,'MM/DD/YYYY HH24:MI') 
        end as invoice_dt,
        order_no::varchar(20) as order_no,
        invoice_no::varchar(20) as invoice_no,
        sls_route_id::varchar(20) as sls_route_id,
        sls_route_nm::varchar(50) as sls_route_nm,
        sls_grp::varchar(50) as sls_grp,
        sls_rep_id::varchar(20) as sls_rep_id,
        sls_rep_nm::varchar(50) as sls_rep_nm,
        dstrbtr_prod_id::varchar(30) as dstrbtr_prod_id,
        uom::varchar(20) as uom,
        cast(gross_price as numeric(15, 4)) as gross_price,
        cast(qty as numeric(15, 4)) as qty,
        cast(gts_val as numeric(15, 4)) as gts_val,
        cast(dscnt as numeric(15, 4)) as dscnt,
        cast(nts_val as numeric(15, 4)) as nts_val,
        cast(line_num as numeric(15, 4)) as line_num,
        prom_id::varchar(20) as prom_id,
        cdl_dttm::varchar(50) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        null::timestamp_ntz(9) as updt_dttm,
        wh_id::varchar(50) as wh_id,
        sls_rep_type::varchar(50) as sls_rep_type,
        cast(order_qty as numeric(15, 4)) as order_qty,
        case 
            when try_to_date(order_delivery_dt,'MM/DD/YYYY HH12:MI:SS AM') is not null then try_to_date(order_delivery_dt,'MM/DD/YYYY HH12:MI:SS AM')
            else  to_date(order_delivery_dt,'MM/DD/YYYY HH24:MI') 
        end as order_delivery_dt,
        order_status::varchar(50) as order_status
    from source
)

select * from final