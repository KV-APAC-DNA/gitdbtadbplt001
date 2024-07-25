{{    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} 
                            where (trim(upper(jj_sap_dstrbtr_id)),trim(bill_doc),trim(jj_sap_prod_id),bill_dt) in 
                                (
                                    SELECT 
                                        DISTINCT TRIM(SDII.DISTRIBUTOR_CODE),
                                        TRIM(SDII.INVOICE_NO),
                                        TRIM(SDII.PRODUCT_CODE),
                                        replace(sdii.invoice_date,'T',' ')::timestamp_ntz(9)
                                FROM {{ source('idnsdl_raw', 'sdl_distributor_ivy_invoice') }} SDII) 
                                AND 
                                    (
                                        (nvl(upper(trim(DSTRBTR_GRP_CD)),'NA')) in 
                                        (
                                            select distinct distributor_cd 
                                            from -- ref('idnitg_integration__itg_mds_id_dist_reporting_control_sellout_sales')
                                                  {{ ref('idnitg_integration__itg_mds_id_dist_reporting_control_sellout_sales') }}      
                                            where source_system = 'DMS')
                                    );
                                    
    {% endif %}"
    )
}}


with source as (
    select * from {{ source('idnsdl_raw', 'sdl_distributor_ivy_invoice') }}
),
itg_mds_id_dist_reporting_control_sellout_sales as (
    select * from {{ ref('idnitg_integration__itg_mds_id_dist_reporting_control_sellout_sales') }}
),
edw_time_dim as (
    select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),
edw_distributor_dim as (
    select * from  {{ ref('idnedw_integration__edw_distributor_dim') }}
),
edw_product_dim as (
    select * from {{ ref('idnedw_integration__edw_product_dim') }}
),
itg_mds_id_dms_dist_margin_control as (
    select * from {{ ref('idnitg_integration__itg_mds_id_dms_dist_margin_control') }}
),
edw_distributor_ivy_outlet_master as (
    select * from  {{ ref('idnedw_integration__edw_distributor_ivy_outlet_master') }}
),
sdii as (
    select t1.*
			,etd.jj_mnth_id
			,etd.jj_wk
		from source t1
			,edw_time_dim etd
		where date(etd.cal_date) = date(trim(t1.invoice_date))
),
invoice as (
    select 
        trim(sdii.distributor_code || sdii.retailer_code || sdii.product_code || sdii.invoice_no || (to_date(sdii.invoice_date))) as trans_key
        ,upper(trim(dstrbtr_grp_cd)) as inv_distributorcode
        ,upper(trim(franchise)) as inv_franchise
        ,upper(trim(brand)) as inv_brand
        ,product_code
        ,line_value
        ,jj_mnth_id
        ,trim(sdii.batch_no) as batch_no
        ,trim(sdii.uom) as uom
        ,invoice_status
    from sdii
        ,edw_distributor_dim edd
        ,edw_product_dim epd
    where trim(upper(edd.jj_sap_dstrbtr_id)) = trim(upper(sdii.distributor_code))
        and sdii.product_code = epd.jj_sap_prod_id
),
joined as (
    select 
        product_code
        ,lookup1.distributorcode
        ,lookup1.franchise
        ,lookup1.brand
        ,lookup1.margin as niv_margin
        ,lookup1.type as niv_type
        ,lookup2.distributorcode
        ,lookup2.franchise
        ,lookup2.brand
        ,lookup2.margin as hna_margin
        ,lookup2.type as hna_type
        ,case 
            when inv_distributorcode = lookup1.distributorcode
                then 1
            else 2
            end as disflag1
        ,case 
            when inv_franchise = lookup1.franchise
                then 1
            else 2
            end as franchiseflag1
        ,case 
            when inv_brand = lookup1.brand
                then 1
            else 2
            end as brandflag1
        ,case 
            when inv_distributorcode = lookup2.distributorcode
                then 1
            else 2
            end as disflag2
        ,case 
            when inv_franchise = lookup2.franchise
                then 1
            else 2
            end as franchiseflag2
        ,case 
            when inv_brand = lookup2.brand
                then 1
            else 2
            end as brandflag2
        ,rank() OVER (
            PARTITION BY inv_distributorcode
            ,inv_franchise
            ,inv_brand
            ,jj_mnth_id ORDER BY disflag1
                ,franchiseflag1
                ,brandflag1
            ) AS rnk1
        ,rank() OVER (
            PARTITION BY inv_distributorcode
            ,inv_franchise
            ,inv_brand
            ,jj_mnth_id ORDER BY disflag2
                ,franchiseflag2
                ,brandflag2
            ) AS rnk2
        ,CASE 
            WHEN niv_type = 'NIV'
                THEN CASE 
                        WHEN invoice_status = 'R'
                            THEN (line_value::number(18,0) * niv_margin) * (- 1)
                        ELSE line_value::number(18,0) * niv_margin
                        END
            ELSE 0::number(18,0)
            END::DECIMAL(38, 4) AS netvalue
        ,CASE 
            WHEN hna_type = 'HNA'
                THEN CASE 
                        WHEN invoice_status = 'R'
                            THEN (line_value::number(18,0) * hna_margin) * (- 1)
                        ELSE line_value::number(18,0) * hna_margin
                        END
            ELSE line_value::number(18,0)
            END::DECIMAL(38, 4) AS grossvalue
        ,invoice.*
    from invoice
    left join itg_mds_id_dms_dist_margin_control lookup1 on lookup1.distributorcode = (
            case 
                when inv_distributorcode = lookup1.distributorcode
                    then inv_distributorcode
                else 'ALL'
                end
            )
        and lookup1.franchise = (
            case 
                when inv_franchise = lookup1.franchise
                    then inv_franchise
                else 'ALL'
                end
            )
        and lookup1.brand = (
            case 
                when inv_brand = lookup1.brand
                    then inv_brand
                else 'ALL'
                end
            )
        and lookup1.type = 'NIV'
        and invoice.jj_mnth_id between lookup1.effective_from
            and lookup1.effective_to
    left join itg_mds_id_dms_dist_margin_control lookup2 on lookup2.distributorcode = (
            case 
                when inv_distributorcode = lookup2.distributorcode
                    then inv_distributorcode
                else 'ALL'
                end
            )
        and lookup2.franchise = (
            case 
                when inv_franchise = lookup2.franchise
                    then inv_franchise
                else 'ALL'
                end
            )
        and lookup2.brand = (
            case 
                when inv_brand = lookup2.brand
                    then inv_brand
                else 'ALL'
                end
            )
        and lookup2.type = 'HNA'
        and invoice.jj_mnth_id between lookup2.effective_from
            and lookup2.effective_to
        qualify rnk1=1 and rnk2 = 1
),
transformed as (
    SELECT 
        trim(sdii.distributor_code || sdii.retailer_code || sdii.product_code || sdii.invoice_no || (to_date(sdii.invoice_date))) as trans_key
		,trim(sdii.invoice_no) as bill_doc
		,replace(sdii.invoice_date,'T',' ')::timestamp_ntz(9) as bill_dt
		,trim(sdii.order_id) as order_id
		,trim(sdii.jj_mnth_id) as jj_mnth_id
		,trim(sdii.jj_wk) as jj_wk
		,trim(edd.dstrbtr_grp_cd) as dstrbtr_grp_cd
		,trim(edd.dstrbtr_id) as dstrbtr_id
		,trim(sdii.distributor_code) as jj_sap_dstrbtr_id
		,trim(sdii.retailer_code) as dstrbtr_cust_id
		,'0' as dstrbtr_prod_id
		,trim(sdii.product_code) as jj_sap_prod_id
		,trim(ediom.outlet_type) as dstrbtn_chnl
		,null as grp_outlet
		,trim(sdii.user_code) as dstrbtr_slsmn_id
		,case 
			when trim(upper(sdii.uom)) = 'CASE'
				then trim(sdii.qty * sdii.uom_count)::numeric(18, 4)
			when trim(substring(sdii.product_code, 9, 10)) like '99%'
				then trim(sdii.qty / decode(epd.uom, null, 1, 0, 1, epd.uom))::numeric(18, 4)
			else trim(sdii.qty)::numeric(18, 4)
			end as sls_qty
		------------------------------------------------
		--GRS_VAL,
		--JJ_NET_VAL,
		------------------------------------------
		,0 as trd_dscnt
		,0 as dstrbtr_net_val
		,case 
			when sdii.qty < 0
				then sdii.qty * - 1
			else 0
			end as rtrn_qty
		,case 
			when sdii.qty < 0
				then sdii.line_value * - 1
			else 0
			end as rtrn_val
		,current_timestamp()::timestamp_ntz(9) crtd_dttm
		,null::timestamp_ntz(9) updt_dttm
		,sdii.run_id
		,trim(sdii.batch_no) as batch_no
		,trim(sdii.uom) as uom
		,trim(invoice_status) as invoice_status
	FROM sdii
		,edw_distributor_dim edd
		,edw_product_dim epd
		,edw_distributor_ivy_outlet_master ediom
	where 
		trim(upper(edd.jj_sap_dstrbtr_id(+))) = trim(upper(sdii.distributor_code))
		and sdii.jj_mnth_id between edd.effective_from(+)
			and edd.effective_to(+)
		and trim(ediom.jj_sap_dstrbtr_id(+)) = trim(sdii.distributor_code)
		and sdii.product_code = epd.jj_sap_prod_id
		and sdii.jj_mnth_id between epd.effective_from(+)
			and epd.effective_to(+)
		and trim(ediom.cust_id(+)) = trim(sdii.retailer_code)
		and (
			(nvl(upper(trim(edd.dstrbtr_grp_cd)), 'NA')) in (
				select distinct distributor_cd
				from itg_mds_id_dist_reporting_control_sellout_sales
				where source_system = 'DMS'
				)
			)
),
final as (
    select 
        transformed.trans_key::varchar(100) as trans_key
        ,bill_doc::varchar(100) as bill_doc
        ,bill_dt::timestamp_ntz(9) as bill_dt
        ,order_id::varchar(50) as order_id
        ,transformed.jj_mnth_id::varchar(10) as jj_mnth_id
        ,jj_wk::varchar(4) as jj_wk
        ,dstrbtr_grp_cd::varchar(20) as dstrbtr_grp_cd
        ,dstrbtr_id::varchar(50) as dstrbtr_id
        ,jj_sap_dstrbtr_id::varchar(50) as jj_sap_dstrbtr_id
        ,dstrbtr_cust_id::varchar(100) as dstrbtr_cust_id
        ,dstrbtr_prod_id::varchar(100) as dstrbtr_prod_id
        ,jj_sap_prod_id::varchar(50) as jj_sap_prod_id
        ,dstrbtn_chnl::varchar(100) as dstrbtn_chnl
        ,grp_outlet::varchar(5) as grp_outlet
        ,dstrbtr_slsmn_id::varchar(100) as dstrbtr_slsmn_id
        ,sls_qty::number(18,4) as sls_qty
        ,lkp.grossvalue::number(18,4)  as grs_val
        ,lkp.netvalue::number(18,4)  as jj_net_val
        ,trd_dscnt::number(18,4) as trd_dscnt
        ,dstrbtr_net_val::number(18,4) as dstrbtr_net_val
        ,rtrn_qty::number(18,4) as rtrn_qty
        ,rtrn_val::number(18,4) as rtrn_val
        ,crtd_dttm::timestamp_ntz(9) as crtd_dttm
        ,updt_dttm::timestamp_ntz(9) as updt_dttm
        ,run_id::number(14,0) as run_id
    from transformed
    left join joined lkp on coalesce(lkp.trans_key,'') || coalesce(lkp.batch_no,'') || coalesce(lkp.uom,'')  =   coalesce(transformed.trans_key,'') || coalesce(transformed.batch_no,'') || coalesce(transformed.uom,'')
)

select * from final