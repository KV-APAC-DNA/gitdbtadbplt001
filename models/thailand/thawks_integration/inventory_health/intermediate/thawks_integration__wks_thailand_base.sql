with edw_th_inventory_analysis_base_6year_filter as
(
    select * from dev_dna_core.snaposeedw_integration.edw_th_inventory_analysis_base_6year_filter
),
edw_billing_fact as
(
    select * from dev_dna_core.snapaspedw_integration.edw_billing_fact
),
edw_calendar_dim as
(
    select * from dev_dna_core.snapaspedw_integration.edw_calendar_dim
),
edw_vw_th_customer_dim as
(
    select * from dev_dna_core.snaposeedw_integration.edw_vw_os_customer_dim
    where sap_cntry_cd = 'TH' 
),
edw_vw_th_material_dim as
(
    select * from dev_dna_core.snaposeedw_integration.edw_vw_os_material_dim
    where cntry_key = 'TH'
),
edw_vw_th_dstrbtr_customer_dim as
(
    select * from dev_dna_core.snaposeedw_integration.edw_vw_os_dstrbtr_customer_dim
        where cntry_cd = 'TH'
),
itg_th_dtsdistributor as
(
    select * from dev_dna_core.snaposeitg_integration.itg_th_dtsdistributor
),
vw_edw_th_mt_sellout as
(
    select * from dev_dna_core.snaposeedw_integration.vw_edw_th_mt_sellout
),
vw_edw_th_mt_inventory as
(
    select * from dev_dna_core.snaposeedw_integration.vw_edw_th_mt_inventory
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
t1 as
(
    select 
        bill_dt,
        ltrim(sold_to, '0') as sold_to,
        ltrim(material, '0') as matl_num,
        bill_type,
        ltrim(ship_to, '0') as ship_to,
        sum(bill_qty) as bill_qty_pc,
        sum(subtotal_1) as grs_trd_sls
    from edw_billing_fact
    where sls_org in ('2400', '2500')
        and bill_type = 'ZF2L'
        and ltrim(sold_to, '0') not in (
            select distinct sap_cust_id
            from edw_vw_th_customer_dim
                where sap_prnt_cust_key in (
                    'PC4564',
                    'PC4572',
                    'PC1728',
                    'PC0004',
                    'PC0509',
                    'PC0638'
                ) --and sap_cust_id in ('108830','108832','116819','108835')
                and sap_sls_org in ('2400', '2500')
        )
        and cast(to_char(bill_dt::date, 'YYYY') as int) >= (date_part(year, current_date) -6)
    group by bill_dt,
        bill_type,
        sold_to,
        ship_to,
        matl_num
),
t3 as 
(
    select * from edw_vw_th_customer_dim
    where sap_prnt_cust_key NOT IN 
    (
        'PC4564',
        'PC4572',
        'PC1728',
        'PC0004',
        'PC0509',
        'PC0638'
    )
),
t5 as 
(
    select distinct dstrbtr_grp_cd,
        sap_soldto_code
    from edw_vw_th_dstrbtr_customer_dim
        where dstrbtr_grp_cd <> 'KCS'
),
t6 as
(
    select distinct dstrbtr_id,
        dstrbtr_cd,
        ship_to_code
    from itg_th_dtsdistributor
    where ship_to_code is not null
    union all
    select 'KCS' as dstrbtr_id,
        '130553' as dstrbtr_cd,
        '625672' as ship_to_code
),
main as
(
    select 
        TO_CHAR(T1.BILL_DT, 'YYYY-MM-DD') as BILL_DT,
        'SELLIN' as DATA_TYPE,
        'Thailand' as COUNTRY_NAME,
        t6.dstrbtr_id as DISTRIBUTOR_ID,
        T4.gph_prod_frnchse as FRANCHISE,
        T4.GPH_PROD_BRND as BRAND,
        T4.GPH_PROD_SUB_BRND as PROD_SUB_BRAND,
        T4.GPH_PROD_VRNT as VARIANT,
        T4.GPH_PROD_SGMNT as SEGMENT,
        T4.GPH_PROD_SUBSGMNT as PROD_SUBSEGMENT,
        T4.GPH_PROD_CTGRY as PROD_CATEGORY,
        T4.GPH_PROD_SUBCTGRY as PROD_SUBCATEGORY,
        LTRIM(T4.SAP_MATL_NUM) as SKU_CD,
        T4.SAP_MAT_DESC as SKU_DESCRIPTION,
        T3.SAP_PRNT_CUST_KEY,
        T3.SAP_PRNT_CUST_DESC,
        T3.SAP_CUST_CHNL_KEY,
        T3.SAP_CUST_CHNL_DESC,
        T3.SAP_CUST_SUB_CHNL_KEY,
        T3.SAP_SUB_CHNL_DESC,
        T3.SAP_GO_TO_MDL_KEY,
        T3.SAP_GO_TO_MDL_DESC,
        T3.SAP_BNR_KEY,
        T3.SAP_BNR_DESC,
        T3.SAP_BNR_FRMT_KEY,
        T3.SAP_BNR_FRMT_DESC,
        T3.RETAIL_ENV,
        NULL as sap_region,
        T2.CAL_YR as CAL_YEAR,
        cast(T2.CAL_QTR_1 as VARCHAR) as CAL_QRTR_NO,
        cast(T2.CAL_MO_1 as VARCHAR) as CAL_MNTH_ID,
        T2.CAL_MO_2 as CAL_MNTH_NO,
        bill_qty_pc as SALES_QUANTITY,
        grs_trd_sls as SI_GROSS_TRADE_SALES_VALUE,
        0 as INVENTORY_QUANTITY,
        0 as INVENTORY,
        0 as GROSS_TRADE_SALES
    from t1,
        edw_calendar_dim t2,
        t3,
        edw_vw_th_material_dim as t4,
        t5,
        t6
    where t1.bill_dt::date = t2.cal_day::date
        and ltrim(t1.sold_to, '0') = ltrim(t3.sap_cust_id(+), '0')
        and ltrim(t1.matl_num, '0') = ltrim(t4.sap_matl_num(+), '0')
        and ltrim(t1.sold_to, '0') = ltrim(t5.sap_soldto_code(+), '0')
        and ltrim(t1.ship_to, '0') = ltrim(t6.ship_to_code(+), '0')
        and ltrim(t1.sold_to, '0') = ltrim(t6.dstrbtr_cd(+), '0')
),
inventory_analysis_time_dim as
(
    select a.*,
        time.cal_year,
        cast(time.cal_qrtr_no as varchar) as cal_qrtr_no,
        cast(time.cal_mnth_id as varchar) as cal_mnth_id,
        time.cal_mnth_no
    from edw_th_inventory_analysis_base_6year_filter a,
        (
            select distinct cal_year,
                cal_qrtr_no,
                cal_mnth_id,
                cal_mnth_no,
                cal_date
            from edw_vw_os_time_dim
        ) as time
    where a.order_date::date = time.cal_date(+)::date
        and upper(a.data_type) in ('INVENTORY')
),
inv as
(
    select data_type,
        country_code,
        cal_year as year,
        cal_mnth_no as month_number,
        distributor_id,
        max(order_date) as mx_order_date,
        'Y' as flg
    from 
    inventory_analysis_time_dim as a
    where upper(data_type) = 'INVENTORY'
    group by data_type,
        country_code,
        cal_year,
        cal_mnth_no,
        distributor_id
),
th_gt as 
(
    select 
        a.cal_mnth_id,
        a.distributor_id as dstr_nm,
        a.sap_prnt_cust_key,
        a.sap_prnt_cust_desc,
        nvl(nullif(a.sku_code, ''), 'NA') as sku_cd,
        sum(
            case
                when upper(a.data_type) = 'SELLIN' then sales_quantity
                else 0
            end
        ) as si_sls_qty,
        sum(a.si_gross_trade_sales_value) as si_gts_val,
        sum(
            case
                when inv.flg = 'Y' then a.inventory_quantity
                else 0
            end
        ) as inventory_quantity,
        sum(
            case
                when inv.flg = 'Y' then a.inventory
                else 0
            end
        ) as inventory_val,
        sum(
            case
                when upper(a.data_type) = 'SALES' then sales_quantity
                else 0
            end
        ) as so_sls_qty,
        sum(a.gross_trade_sales) as so_grs_trd_sls
    from 
    (
        select 
            A.ORDER_DATE,
            A.DATA_TYPE,
            A.COUNTRY_NAME,
            A.DISTRIBUTOR_ID,
            A.FRANCHISE,
            A.BRAND,
            A.PROD_SUB_BRAND,
            A.VARIANT,
            A.SEGMENT,
            A.PROD_SUBSEGMENT,
            A.PROD_CATEGORY,
            A.PROD_SUBCATEGORY,
            A.SKU_CODE,
            A.SKU_DESCRIPTION,
            A.SAP_PRNT_CUST_KEY,
            A.SAP_PRNT_CUST_DESC,
            A.SAP_CUST_CHNL_KEY,
            A.SAP_CUST_CHNL_DESC,
            A.SAP_CUST_SUB_CHNL_KEY,
            A.SAP_SUB_CHNL_DESC,
            A.SAP_GO_TO_MDL_KEY,
            A.SAP_GO_TO_MDL_DESC,
            A.SAP_BNR_KEY,
            A.SAP_BNR_DESC,
            A.SAP_BNR_FRMT_KEY,
            A.SAP_BNR_FRMT_DESC,
            A.RETAIL_ENV,
            A.sap_region,
            TIME.CAL_YEAR,
            cast(TIME.CAL_QRTR_NO as VARCHAR) as CAL_QRTR_NO,
            cast(TIME.CAL_MNTH_ID as VARCHAR) as CAL_MNTH_ID,
            TIME.CAL_MNTH_NO,
            A.SALES_QUANTITY,
            A.SI_GROSS_TRADE_SALES_VALUE,
            A.INVENTORY_QUANTITY,
            A.INVENTORY,
            A.GROSS_TRADE_SALES
        from edw_th_inventory_analysis_base_6year_filter A,
            (
                select distinct cal_year,
                    cal_qrtr_no,
                    cal_mnth_id,
                    cal_mnth_no,
                    cal_date
                from edw_vw_os_time_dim
            ) as TIME
        where a.order_date::date= time.cal_date(+)::date
            and upper(a.data_type) iN ('INVENTORY', 'SALES')
        union all
        select 
            bill_dt,
            data_type,
            country_name,
            case
                when main.distributor_id is null
                and main.sap_prnt_cust_key <> 'PC3159' then dist.distributor_id
                when main.distributor_id is null
                and main.sap_prnt_cust_key = 'PC3159' then 'TPD'
                else main.distributor_id
            end as distributor_id,
            franchise,
            brand,
            prod_sub_brand,
            variant,
            segment,
            prod_subsegment,
            prod_category,
            prod_subcategory,
            sku_cd,
            sku_description,
            main.sap_prnt_cust_key,
            sap_prnt_cust_desc,
            sap_cust_chnl_key,
            sap_cust_chnl_desc,
            sap_cust_sub_chnl_key,
            sap_sub_chnl_desc,
            sap_go_to_mdl_key,
            sap_go_to_mdl_desc,
            sap_bnr_key,
            sap_bnr_desc,
            sap_bnr_frmt_key,
            sap_bnr_frmt_desc,
            retail_env,
            sap_region,
            cal_year,
            cal_qrtr_no,
            cal_mnth_id,
            cal_mnth_no,
            sales_quantity,
            si_gross_trade_sales_value,
            inventory_quantity,
            inventory,
            gross_trade_sales
        from  main
        left join 
        (
            select distinct distributor_id,
                sap_prnt_cust_key
            from edw_th_inventory_analysis_base_6year_filter
            where sap_prnt_cust_key <> 'PC3159'
        ) dist on ltrim(main.sap_prnt_cust_key, '0') = ltrim(dist.sap_prnt_cust_key, '0')
    ) A 
    left join inv on
    a.data_type = inv.data_type
    and a.cal_year = inv.year
    and a.cal_mnth_no = inv.month_number
    and a.distributor_id = inv.distributor_id
    and inv.mx_order_date = a.order_date
    group by 
    cal_mnth_id,
    a.cal_mnth_id,
    a.distributor_id,
    a.sap_prnt_cust_key,
    a.sap_prnt_cust_desc,
    a.sku_code
),
transformed as
(
    select * from th_gt
    union all
    select 
        cast(mnth_id as VARCHAR) mnth_id,
        case
            when sap_prnt_cust_key = 'PC4572' then 'BigC'
            when sap_prnt_cust_key = 'PC0638' then '7-Eleven'
            when sap_prnt_cust_key = 'PC0509' then 'Tops'
            else sap_prnt_cust_desc
        end as dstr_nm,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        nvl(nullif(matl_num, ''), 'NA') as matl_num,
        sum(si_sls_qty) si_sls_qty,
        sum(si_gts_val) si_gts_val,
        sum(inventory_quantity) inventory_quantity,
        sum(inventory_val) inventory_val,
        sum(sellout_quantity) sellout_quantity,
        sum(sellout_value) sellout_value
    from
    (
        select 
            cast(mnth_id as VARCHAR) mnth_id,
            sap_prnt_cust_key,
            sap_prnt_cust_desc,
            matl_num,
            0 as si_sls_qty,
            0 as si_gts_val,
            0 as inventory_quantity,
            0 as inventory_val,
            sum(sellout_quantity) sellout_quantity,
            sum(sellout_value) sellout_value
        from vw_edw_th_mt_sellout
        where left (mnth_id, 4) >= (DATE_PART(YEAR, current_date) -6)
        group by mnth_id,
            sap_prnt_cust_key,
            sap_prnt_cust_desc,
            matl_num
        union all
        select 
            cast(mnth_id as VARCHAR) as mnth_id,
            sap_prnt_cust_key,
            sap_prnt_cust_desc,
            matl_num,
            0 as si_sls_qty,
            0 as si_gts_val,
            sum(inventory_quantity) inventory_quantity,
            sum(inventory_valUE) inventory_val,
            0 as sellout_quantity,
            0 as sellout_value
        from vw_edw_th_mt_inventory
        where left (mnth_id, 4) >= (DATE_PART(YEAR, current_date) -6)
        group by mnth_id,
            sap_prnt_cust_key,
            sap_prnt_cust_desc,
            matl_num
        union all
        select 
            cast(mnth_id as VARCHAR) as mnth_id,
            sap_prnt_cust_key,
            sap_prnt_cust_desc,
            matl_num,
            sum(bill_qty_pc) as bill_qty_pc,
            sum(grs_trd_sls) as grs_trd_sls,
            0 as INV_QTY,
            0 as INV_VAL,
            0 as SO_QTY,
            0 as sO_VAL
        from 
        (
            select BILL_DT,
                sold_to,
                matl_num,
                sum(bill_qty_pc) bill_qty_pc,
                sum(grs_trd_sls) grs_trd_sls
            from 
            (
                select 
                    bill_dt,
                    ltrim(sold_to, '0') as sold_to,
                    ltrim(material, '0') as matl_num,
                    bill_type,
                    sum(bill_qty) as bill_qty_pc,
                    sum(subtotal_1) as grs_trd_sls
                from edw_billing_fact
                where LTRIM(SOLD_TO, '0') IN (
                        select DISTINCT sap_cust_id
                        from edw_vw_th_customer_dim
                            where sap_prnt_cust_key IN (
                                'PC4564',
                                'PC4572',
                                'PC1728',
                                'PC0004',
                                'PC0509',
                                'PC0638'
                            ) --and sap_cust_id in ('108830','108832','116819','108835')
                            AND sap_sls_org IN ('2400', '2500')
                    )
                    and sls_org in ('2400', '2500')
                    and bill_type = 'ZF2L'
                    and cast(to_char(bill_dt::date, 'YYYY') as int) >= (date_part(year, current_date) -6)
                group by bill_dt,
                    bill_type,
                    sold_to,
                    matl_num
            )
            group by 
            bill_dt,
            sold_to,
            matl_num
        ) T1
        join edw_vw_os_time_dim t2 on t1.bill_dt = t2.cal_date
        left join edw_vw_th_customer_dim c on ltrim (t1.sold_to, 0) = ltrim (c.sap_cust_id, 0)
        group by 
        mnth_id,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        matl_num
    )
    group by mnth_id,
    sap_prnt_cust_key,
    sap_prnt_cust_desc,
    matl_num
),
final as
(
    select 
        cal_mnth_id::varchar(23) as cal_mnth_id,
        dstr_nm::varchar(50) as dstr_nm,
        sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
        sap_prnt_cust_desc::varchar(50) as sap_prnt_cust_desc,
        sku_cd::varchar(50) as sku_cd,
        si_sls_qty::numeric(38,6) as si_sls_qty,
        si_gts_val::numeric(38,5) as si_gts_val,
        inventory_quantity::double precision as inventory_quantity,
        inventory_val::double precision as inventory_val,
        so_sls_qty::double precision as so_sls_qty,
        so_grs_trd_sls::double precision as so_grs_trd_sls,
    from transformed
)
select * from final