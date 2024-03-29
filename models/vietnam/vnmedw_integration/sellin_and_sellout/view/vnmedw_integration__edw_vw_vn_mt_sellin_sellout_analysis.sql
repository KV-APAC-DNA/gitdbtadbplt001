with edw_vw_vn_mt_pos_offtake as (
select * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.EDW_VW_VN_MT_POS_OFFTAKE
),
edw_vw_vn_mt_sell_in_analysis as (
select * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.EDW_VW_VN_MT_SELL_IN_ANALYSIS
),
itg_spiral_mti_offtake as (
select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_SPIRAL_MTI_OFFTAKE
),
edw_vw_vn_mt_dist_products as (
select * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.EDW_VW_VN_MT_DIST_PRODUCTS
),
edw_calendar_dim as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_CALendAR_DIM
),
union_3 as (select 'POS' as data_source,
    'Offtake' as data_type,
    null::varchar as invoice_date,
    pos.year::integer as "year",
    null::integer as qrtr_no,
    (timedim.qrtr)::varchar as qrtr,
    (timedim.mnth_id)::varchar as mnth_id,
    null::varchar as mnth_desc,
    pos.month::integer as mnth_no,
    null::varchar as mnth_shrt,
    null::varchar as mnth_long,
    null::bigint as wk,
    null::bigint as mnth_wk_no,
    null::integer as cal_year,
    null::integer as cal_qrtr_no,
    null::integer as cal_mnth_id,
    null::integer as cal_mnth_no,
    null::varchar as cal_mnth_nm,
    null::date as cal_date,
    null::varchar as cal_date_id,
    pos.supcode as supplier_code,
    pos.supname as supplier_name,
    null::varchar as plant,
    pos.barcode as productid,
    pos.productname as product_name,
    pos.barcode,
    null as jnj_sap_code,
    (dst_prod.franchise)::varchar as franchise,
    (dst_prod.category)::varchar as category,
    (dst_prod.sub_brand)::varchar as sub_category,
    (dst_prod.sub_category)::varchar as sub_brand,
    null::varchar as size,
    'MT' as channel,
    pos.shopcode as custcode,
    pos.shopname as name,
    pos.address,
    null::varchar as sub_channel,
    
    null::varchar as group_account,
    pos.customername as "account",
    null::varchar as "region",
    null::varchar as province,
    pos.channelname as retail_environment,
    null::varchar as district,
    null::varchar as "zone",
    null::varchar as period,
    pos.supname as sales_supervisor,
    null::varchar as kam,
    ((pos.quantity)::numeric(18, 0))::numeric(38, 4) as sales_qty,
    ((pos.amount)::numeric(18, 0))::numeric(38, 9) as sales_amt_lcy,
    (pos.amountusd)::numeric(38, 14) as sales_amt_usd,
    ((null::numeric)::numeric(18, 0))::numeric(28, 10) as target_lcy,
    ((null::numeric)::numeric(18, 0))::numeric(28, 10) as target_usd
from (
        (
            itg_spiral_mti_offtake pos
            left join (
                select DISTINCT edw_vw_vn_mt_dist_products.barcode,
                    upper(edw_vw_vn_mt_dist_products.franchise) as franchise,
                    upper(edw_vw_vn_mt_dist_products.category) as category,
                    upper(edw_vw_vn_mt_dist_products.sub_brand) as sub_brand,
                    upper(edw_vw_vn_mt_dist_products.sub_category) as sub_category,
                    row_number() over(
                        partition by edw_vw_vn_mt_dist_products.barcode order by null
                    ) as rn
                from edw_vw_vn_mt_dist_products
            ) dst_prod on (((pos.barcode)::text = (dst_prod.barcode)::text))
        )
        left join (
            select DISTINCT time_dim."year" as jnj_year,
                time_dim.qrtr,
                time_dim.mnth_id,
                time_dim.mnth_no
            from (
                    select ecd.fisc_yr as "year",
                        case
                            when (ecd.pstng_per = 1) then 1
                            when (ecd.pstng_per = 2) then 1
                            when (ecd.pstng_per = 3) then 1
                            when (ecd.pstng_per = 4) then 2
                            when (ecd.pstng_per = 5) then 2
                            when (ecd.pstng_per = 6) then 2
                            when (ecd.pstng_per = 7) then 3
                            when (ecd.pstng_per = 8) then 3
                            when (ecd.pstng_per = 9) then 3
                            when (ecd.pstng_per = 10) then 4
                            when (ecd.pstng_per = 11) then 4
                            when (ecd.pstng_per = 12) then 4
                            else null::integer
                        end as qrtr_no,
                        (
                            (
                                ((ecd.fisc_yr)::varchar)::text || ('/'::varchar)::text
                            ) || (
                                case
                                    when (ecd.pstng_per = 1) then 'Q1'::varchar
                                    when (ecd.pstng_per = 2) then 'Q1'::varchar
                                    when (ecd.pstng_per = 3) then 'Q1'::varchar
                                    when (ecd.pstng_per = 4) then 'Q2'::varchar
                                    when (ecd.pstng_per = 5) then 'Q2'::varchar
                                    when (ecd.pstng_per = 6) then 'Q2'::varchar
                                    when (ecd.pstng_per = 7) then 'Q3'::varchar
                                    when (ecd.pstng_per = 8) then 'Q3'::varchar
                                    when (ecd.pstng_per = 9) then 'Q3'::varchar
                                    when (ecd.pstng_per = 10) then 'Q4'::varchar
                                    when (ecd.pstng_per = 11) then 'Q4'::varchar
                                    when (ecd.pstng_per = 12) then 'Q4'::varchar
                                    else null::varchar
                                end
                            )::text
                        ) as qrtr,
                        (
                            ((ecd.fisc_yr)::varchar)::text || trim(
                                to_char(ecd.pstng_per, ('00'::varchar)::text)
                            )
                        ) as mnth_id,
                        (
                            (
                                ((ecd.fisc_yr)::varchar)::text || ('/'::varchar)::text
                            ) || (
                                case
                                    when (ecd.pstng_per = 1) then 'JAN'::varchar
                                    when (ecd.pstng_per = 2) then 'FEB'::varchar
                                    when (ecd.pstng_per = 3) then 'MAR'::varchar
                                    when (ecd.pstng_per = 4) then 'APR'::varchar
                                    when (ecd.pstng_per = 5) then 'MAY'::varchar
                                    when (ecd.pstng_per = 6) then 'JUN'::varchar
                                    when (ecd.pstng_per = 7) then 'JUL'::varchar
                                    when (ecd.pstng_per = 8) then 'AUG'::varchar
                                    when (ecd.pstng_per = 9) then 'SEP'::varchar
                                    when (ecd.pstng_per = 10) then 'OCT'::varchar
                                    when (ecd.pstng_per = 11) then 'NOV'::varchar
                                    when (ecd.pstng_per = 12) then 'DEC'::varchar
                                    else null::varchar
                                end
                            )::text
                        ) as mnth_desc,
                        ecd.pstng_per as mnth_no,
                        case
                            when (ecd.pstng_per = 1) then 'JAN'::varchar
                            when (ecd.pstng_per = 2) then 'FEB'::varchar
                            when (ecd.pstng_per = 3) then 'MAR'::varchar
                            when (ecd.pstng_per = 4) then 'APR'::varchar
                            when (ecd.pstng_per = 5) then 'MAY'::varchar
                            when (ecd.pstng_per = 6) then 'JUN'::varchar
                            when (ecd.pstng_per = 7) then 'JUL'::varchar
                            when (ecd.pstng_per = 8) then 'AUG'::varchar
                            when (ecd.pstng_per = 9) then 'SEP'::varchar
                            when (ecd.pstng_per = 10) then 'OCT'::varchar
                            when (ecd.pstng_per = 11) then 'NOV'::varchar
                            when (ecd.pstng_per = 12) then 'DEC'::varchar
                            else null::varchar
                        end as mnth_shrt,
                        case
                            when (ecd.pstng_per = 1) then 'JANUARY'::varchar
                            when (ecd.pstng_per = 2) then 'FEBRUARY'::varchar
                            when (ecd.pstng_per = 3) then 'MARCH'::varchar
                            when (ecd.pstng_per = 4) then 'APRIL'::varchar
                            when (ecd.pstng_per = 5) then 'MAY'::varchar
                            when (ecd.pstng_per = 6) then 'JUNE'::varchar
                            when (ecd.pstng_per = 7) then 'JULY'::varchar
                            when (ecd.pstng_per = 8) then 'AUGUST'::varchar
                            when (ecd.pstng_per = 9) then 'SEPTEMBER'::varchar
                            when (ecd.pstng_per = 10) then 'OCTOBER'::varchar
                            when (ecd.pstng_per = 11) then 'NOVEMBER'::varchar
                            when (ecd.pstng_per = 12) then 'DECEMBER'::varchar
                            else null::varchar
                        end as mnth_long,
                        cyrwkno.yr_wk_num as wk,
                        cmwkno.mnth_wk_num as mnth_wk_no,
                        ecd.cal_yr as cal_year,
                        ecd.cal_qtr_1 as cal_qrtr_no,
                        ecd.cal_mo_1 as cal_mnth_id,
                        ecd.cal_mo_2 as cal_mnth_no,
                        case
                            when (ecd.cal_mo_2 = 1) then 'JANUARY'::varchar
                            when (ecd.cal_mo_2 = 2) then 'FEBRUARY'::varchar
                            when (ecd.cal_mo_2 = 3) then 'MARCH'::varchar
                            when (ecd.cal_mo_2 = 4) then 'APRIL'::varchar
                            when (ecd.cal_mo_2 = 5) then 'MAY'::varchar
                            when (ecd.cal_mo_2 = 6) then 'JUNE'::varchar
                            when (ecd.cal_mo_2 = 7) then 'JULY'::varchar
                            when (ecd.cal_mo_2 = 8) then 'AUGUST'::varchar
                            when (ecd.cal_mo_2 = 9) then 'SEPTEMBER'::varchar
                            when (ecd.cal_mo_2 = 10) then 'OCTOBER'::varchar
                            when (ecd.cal_mo_2 = 11) then 'NOVEMBER'::varchar
                            when (ecd.cal_mo_2 = 12) then 'DECEMBER'::varchar
                            else null::varchar
                        end as cal_mnth_nm,
                        ecd.cal_day::date as cal_date,
                        "replace"(
                            ((ecd.cal_day)::varchar)::text,
                            ('-'::varchar)::text,
                            (''::varchar)::text
                        ) as cal_date_id
                    from edw_calendar_dim ecd,
                        (
                            select row_number() OVER(
                                    partition by a.fisc_per
                                    ORDER BY a.cal_wk
                                ) as mnth_wk_num,
                               
                                    dateadd(
                                        DAY,
                                        -6,
                                        a.cal_day::date
                                    )
                                as cal_day_first,
                                a.cal_day as cal_day_last
                            from edw_calendar_dim a
                            WHERE (
                                    a.cal_day IN (
                                        select edw_calendar_dim.cal_day
                                        from edw_calendar_dim
                                        WHERE (edw_calendar_dim.wkday = 7)
                                    )
                                )
                            ORDER BY a.cal_wk
                        ) cmwkno,
                        (
                            select row_number() OVER(
                                    partition by a.fisc_yr
                                    ORDER BY a.cal_wk
                                ) as yr_wk_num,
                                    dateadd(
                                        DAY,-6,a.cal_day::date
                                    )
                                 as cal_day_first,
                                a.cal_day as cal_day_last
                            from edw_calendar_dim a
                            WHERE (
                                    a.cal_day IN (
                                        select edw_calendar_dim.cal_day
                                        from edw_calendar_dim
                                        WHERE (edw_calendar_dim.wkday = 7)
                                    )
                                )
                            ORDER BY a.cal_wk
                        ) cyrwkno
                    WHERE (
                            (
                                (
                                    (ecd.cal_day >= cmwkno.cal_day_first)
                                    AND (ecd.cal_day <= cmwkno.cal_day_last)
                                )
                                AND (ecd.cal_day >= cyrwkno.cal_day_first)
                            )
                            AND (ecd.cal_day <= cyrwkno.cal_day_last)
                        )
                ) time_dim
        ) timedim ON (
            (
                case
                    when (length((pos.month)::text) = 1) then concat(
                        (pos.year)::text,
                        concat(
                            ((0)::varchar)::text,
                            (pos.month)::text
                        )
                    )
                    else concat((pos.year)::text, (pos.month)::text)
                end = timedim.mnth_id
            )
        )
    )
WHERE (
        (dst_prod.rn = 1)
        OR (dst_prod.rn IS null)
    )),
union_2 as ( 
    select 'POS' as data_source,
        'Offtake' as data_type,
        null::varchar as invoice_date,
        edw_vw_vn_mt_pos_offtake.year,
        null::integer as qrtr_no,
        (edw_vw_vn_mt_pos_offtake.quarter)::varchar as qrtr,
        (edw_vw_vn_mt_pos_offtake.month_id)::varchar as mnth_id,
        null::varchar as mnth_desc,
        edw_vw_vn_mt_pos_offtake.month as mnth_no,
        null::varchar as mnth_shrt,
        null::varchar as mnth_long,
        null::bigint as wk,
        null::bigint as mnth_wk_no,
        null::integer as cal_year,
        null::integer as cal_qrtr_no,
        null::integer as cal_mnth_id,
        null::integer as cal_mnth_no,
        null::varchar as cal_mnth_nm,
        null::date as cal_date,
        null::varchar as cal_date_id,
        null::varchar as supplier_code,
        null::varchar as supplier_name,
        null::varchar as plant,
        edw_vw_vn_mt_pos_offtake.product_cd as productid,
        (edw_vw_vn_mt_pos_offtake.product_name)::varchar as product_name,
        edw_vw_vn_mt_pos_offtake.barcode,
        null as jnj_sap_code,
        (edw_vw_vn_mt_pos_offtake.franchise)::varchar as franchise,
        (edw_vw_vn_mt_pos_offtake.category)::varchar as category,
        (edw_vw_vn_mt_pos_offtake.sub_brand)::varchar as sub_category,
        (edw_vw_vn_mt_pos_offtake.sub_category)::varchar as sub_brand,
        null::varchar as size,
        'MT' as channel,
        edw_vw_vn_mt_pos_offtake.customer_cd as custcode,
        edw_vw_vn_mt_pos_offtake.store_name as name,
        null::varchar as address,
        null::varchar as sub_channel,
        null::varchar as group_account,
        edw_vw_vn_mt_pos_offtake.account,
        null::varchar as "region",
        null::varchar as province,
        null::varchar as retail_environment,
        null::varchar as district,
        null::varchar as "zone",
        null::varchar as period,
        null::varchar as sales_supervisor,
        null::varchar as kam,
        edw_vw_vn_mt_pos_offtake.quantity as sales_qty,
        edw_vw_vn_mt_pos_offtake.amount as sales_amt_lcy,
        edw_vw_vn_mt_pos_offtake.amount_usd as sales_amt_usd,
        ((null::numeric)::numeric(18, 0))::numeric(28, 10) as target_lcy,
        ((null::numeric)::numeric(18, 0))::numeric(28, 10) as target_usd
    from edw_vw_vn_mt_pos_offtake
),
union_1 as (
    select edw_vw_vn_mt_sell_in_analysis.data_source,
        edw_vw_vn_mt_sell_in_analysis.data_type,
        edw_vw_vn_mt_sell_in_analysis.invoice_date,
        edw_vw_vn_mt_sell_in_analysis.year,
        edw_vw_vn_mt_sell_in_analysis.qrtr_no,
        edw_vw_vn_mt_sell_in_analysis.qrtr,
        edw_vw_vn_mt_sell_in_analysis.mnth_id,
        edw_vw_vn_mt_sell_in_analysis.mnth_desc,
        edw_vw_vn_mt_sell_in_analysis.mnth_no,
        edw_vw_vn_mt_sell_in_analysis.mnth_shrt,
        edw_vw_vn_mt_sell_in_analysis.mnth_long,
        edw_vw_vn_mt_sell_in_analysis.wk,
        edw_vw_vn_mt_sell_in_analysis.mnth_wk_no,
        edw_vw_vn_mt_sell_in_analysis.cal_year,
        edw_vw_vn_mt_sell_in_analysis.cal_qrtr_no,
        edw_vw_vn_mt_sell_in_analysis.cal_mnth_id,
        edw_vw_vn_mt_sell_in_analysis.cal_mnth_no,
        edw_vw_vn_mt_sell_in_analysis.cal_mnth_nm,
        edw_vw_vn_mt_sell_in_analysis.cal_date,
        edw_vw_vn_mt_sell_in_analysis.cal_date_id,
        edw_vw_vn_mt_sell_in_analysis.supplier_code,
        edw_vw_vn_mt_sell_in_analysis.supplier_name,
        edw_vw_vn_mt_sell_in_analysis.plant,
        edw_vw_vn_mt_sell_in_analysis.productid,
        edw_vw_vn_mt_sell_in_analysis.product_name,
        edw_vw_vn_mt_sell_in_analysis.barcode,
        edw_vw_vn_mt_sell_in_analysis.jnj_sap_code,
        edw_vw_vn_mt_sell_in_analysis.franchise,
        edw_vw_vn_mt_sell_in_analysis.category,
        edw_vw_vn_mt_sell_in_analysis.sub_category,
        edw_vw_vn_mt_sell_in_analysis.sub_brand,
        edw_vw_vn_mt_sell_in_analysis.size,
        edw_vw_vn_mt_sell_in_analysis.channel,
        edw_vw_vn_mt_sell_in_analysis.custcode,
        edw_vw_vn_mt_sell_in_analysis.name,
        edw_vw_vn_mt_sell_in_analysis.address,
        edw_vw_vn_mt_sell_in_analysis.sub_channel,
        edw_vw_vn_mt_sell_in_analysis.group_account,
        edw_vw_vn_mt_sell_in_analysis.account,
        edw_vw_vn_mt_sell_in_analysis.region,
        edw_vw_vn_mt_sell_in_analysis.province,
        edw_vw_vn_mt_sell_in_analysis.retail_environment,
        null::varchar as district,
        edw_vw_vn_mt_sell_in_analysis.zone,
        edw_vw_vn_mt_sell_in_analysis.period,
        edw_vw_vn_mt_sell_in_analysis.sales_supervisor,
        edw_vw_vn_mt_sell_in_analysis.kam,
        edw_vw_vn_mt_sell_in_analysis.sales_qty,
        edw_vw_vn_mt_sell_in_analysis.sales_amt_lcy,
        edw_vw_vn_mt_sell_in_analysis.sales_amt_usd,
        edw_vw_vn_mt_sell_in_analysis.target_lcy,
        edw_vw_vn_mt_sell_in_analysis.target_usd
    from edw_vw_vn_mt_sell_in_analysis

),
final as (
    select * from union_1
    union ALL
    select * from union_2
    union ALL
    select * from union_3
)
select * from final