with wks_edw_pos_fact_korea as
(
    select * from {{ ref('ntawks_integration__wks_edw_pos_fact_korea') }}
),
wks_pos_prc_condition_map as (
    select * from {{ ref('ntawks_integration__wks_pos_prc_condition_map') }}
),
current_table as
(
    select
        pos_dt,
        ean_num,
        sold_to_party,
        matl_num,
        matl_desc,
        calc_invoice_price,
        ctry_cd,
        wave,
        crt_dttm,
        updt_dttm,
        sls_grp_cd,
        pricing_sold_to
    from {{this}}
),
wave_1 as
(
    select
        pos_dt,
        ean_num,
        sold_to_party,
        matl_num,
        matl_desc::varchar as matl_desc,
        calc_invoice_price,
        'KR' as ctry_cd,
        1 as wave,
        current_timestamp() as crt_dttm,
        current_timestamp() as updt_dttm,
        null as sls_grp_cd,
        null as pricing_sold_to
    from
        (
            select
                pos_dt,
                sold_to_party,
                ean_num,
                matl_num,
                matl_desc,
                calc_invoice_price,
                ROW_NUMBER() OVER ( PARTITION BY pos_dt, sold_to_party, ean_num ORDER BY calc_invoice_price DESC) AS rnk
            /*Changed from ASc to desc Req chnage on3-aug18*/
        from
            (
                select
                    pos_dt,
                    sold_to_party,
                    ean_num,
                    matl_num,
                    matl_desc,
                    sum (calc_price) as calc_invoice_price
                from
                    (
                        select
                            pos_data.pos_dt,
                            pos_data.sold_to_party,
                            pos_data.ean_num,
                            map.cnd_type,
                            map.matl_num,
                            map.matl_desc,
                            map.sales_grp_cd,
                            map.sold_to_cust_cd,
                            map.ean_num as pc_ean_num,
                            map.calc_price
                        from
                            (
                                select pos_dt,
                                    ltrim(sold_to_party, 0) as sold_to_party,
                                    ltrim(ean_num, 0) as ean_num
                                from wks_edw_pos_fact_korea
                                minus
                                select pos_dt,
                                    ltrim(sold_to_party, 0) as sold_to_party,
                                    ltrim(ean_num, 0) as ean_num
                                from current_table
                                    --
                            ) pos_data
                            inner join wks_pos_prc_condition_map map on pos_data.pos_dt between map.vld_frm and map.vld_to
                            and ltrim (pos_data.sold_to_party, 0) = ltrim (map.sold_to_cust_cd, 0)
                            and ltrim (pos_data.ean_num, 0) = ltrim (map.ean_num, 0)
                            and map.cnd_type <> 'ZKSD' ---------- Added new filter per as 3rd Aug Req,ZKSD to be ignored
                    ) pos_map
                group by pos_dt,
                    sold_to_party,
                    ean_num,
                    matl_num,
                    matl_desc
                having sum (calc_price) > 0
                    /*HANDLE CASE3 - TAKE LOWEST AMONG POSITIVE PRICES*/
            ) a
        ) src
    where src.rnk = 1

),
wks_pos_gross_prc_condition_map as
(
    select
        pos_dt,
        sold_to_party,
        ean_num,
        cnd_type,
        matl_num,
        matl_desc::varchar as matl_desc,
        calc_price,
        vld_frm,
        vld_to
    from
        (
            select
                pos_data.pos_dt,
                ltrim (pos_data.sold_to_party, 0) as sold_to_party,
                ltrim (pos_data.ean_num, 0) as ean_num,
                map.cnd_type,
                map.matl_num,
                map.matl_desc,
                map.calc_price,
                map.vld_frm,
                map.vld_to,
                ROW_NUMBER() OVER
                (
                    PARTITION BY pos_data.pos_dt, ltrim(pos_data.sold_to_party, 0), ltrim(pos_data.ean_num, 0), map.matl_num, map.cnd_type
                    ORDER BY map.vld_to desc, map.vld_frm desc
                ) AS rownum
            from
                (
                    select pos_dt,
                        ltrim(sold_to_party, 0) as sold_to_party,
                        ltrim(ean_num, 0) as ean_num
                    from wks_edw_pos_fact_korea
                    minus
                    select pos_dt,
                        ltrim(sold_to_party, 0) as sold_to_party,
                        ltrim(ean_num, 0) as ean_num
                    from
                    (
                        select * from current_table
                        union all
                        select * from wave_1
                    )

                ) pos_data
                inner join wks_pos_prc_condition_map map on
                /*pos_data.pos_dt between map.vld_frm and map.vld_to and*/
                --REMOVED JOIN WITH POS DATE
                ltrim (pos_data.sold_to_party, 0) = ltrim (map.sold_to_cust_cd, 0)
                and ltrim (pos_data.ean_num, 0) = ltrim (map.ean_num, 0)
                and pos_data.pos_dt >= map.vld_frm
                and cnd_type = 'ZPR0'
                /* ADDED NEW Condition */
                /* Added to pick only Gross price Wave 2 logic 2 */
        ) wave2
    where wave2.rownum = 1
),
wave_2 as
(
    select
        pos_dt,
        ean_num,
        sold_to_party,
        matl_num,
        matl_desc::varchar as matl_desc,
        calc_invoice_price,
        'KR' as ctry_cd,
        2 as wave,
        current_timestamp() as crt_dttm,
        current_timestamp() as updt_dttm,
        null as sls_grp_cd,
        null as pricing_sold_to
    from
        (
            select
                pos_dt,
                sold_to_party,
                ean_num,
                matl_num,
                matl_desc,
                calc_invoice_price,
                ROW_NUMBER() OVER ( PARTITION BY pos_dt, sold_to_party, ean_num ORDER BY calc_invoice_price DESC) AS rnk
                /*Changed from ASc to desc Req change on3-aug18*/
            from
                (
                    select pos_dt,
                        sold_to_party,
                        ean_num,
                        matl_num,
                        matl_desc,
                        sum (calc_price) as calc_invoice_price
                    from
                        (
                            select
                                pos_dt,
                                sold_to_party,
                                ean_num,
                                cnd_type,
                                matl_num,
                                matl_desc,
                                calc_price,
                                vld_frm,
                                vld_to
                            from wks_pos_gross_prc_condition_map
                            union
                            select
                                a.pos_dt,
                                a.sold_to_party,
                                a.ean_num,
                                map.cnd_type,
                                a.matl_num,
                                a.matl_desc,
                                map.calc_price,
                                map.vld_frm,
                                map.vld_to
                            from wks_pos_gross_prc_condition_map a
                                inner join wks_pos_prc_condition_map map on ltrim (a.sold_to_party, 0) = ltrim (map.sold_to_cust_cd, 0)
                                and ltrim (a.ean_num, 0) = ltrim (map.ean_num, 0)
                                and ltrim (a.matl_num) = ltrim(map.matl_num)
                                and a.vld_to between map.vld_frm and map.vld_to
                                and map.cnd_type not in ('ZKSD', 'ZPR0') ---------  3-aug18 only  ZKTD values to be considered
                        ) pos_map
                    group by pos_dt,
                        sold_to_party,
                        ean_num,
                        matl_num,
                        matl_desc
                    having sum (calc_price) > 0
                        /*HANDLE CASE3 - TAKE LOWEST AMONG POSITIVE PRICES*/
                ) src
        ) source
    where source.rnk = 1
),
wave_3 as
(
    select
        pos_dt,
        ean_num,
        edw_sold_to_party as sold_to_party,
        matl_num,
        matl_desc::varchar as matl_desc,
        calc_invoice_price,
        'KR' as ctry_cd,
        3 as wave,
        current_timestamp() as crt_dttm,
        current_timestamp() as updt_dttm,
        sales_grp_cd,
        pc_sold_to_party
    from
        (
            select
                pos_dt,
                edw_sold_to_party,
                pc_sold_to_party,
                sales_grp_cd,
                ean_num,
                matl_num,
                matl_desc,
                calc_invoice_price,
                ROW_NUMBER() OVER (
                    PARTITION BY pos_dt,
                    edw_sold_to_party,
                    ean_num
                    ORDER BY calc_invoice_price DESC
                ) AS rnk
            from
                (
                    select
                        pos_dt,
                        edw_sold_to_party,
                        pc_sold_to_party,
                        sales_grp_cd,
                        ean_num,
                        matl_num,
                        matl_desc,
                        sum(calc_price) as calc_invoice_price
                    from
                        (
                            select
                                pos_data.pos_dt,
                                pos_data.edw_sold_to_party,
                                pos_data.ean_num,
                                map.cnd_type,
                                map.matl_num,
                                map.matl_desc,
                                map.sales_grp_cd,
                                ltrim (map.sold_to_cust_cd, 0) as pc_sold_to_party,
                                map.ean_num as pc_ean_num,
                                map.calc_price
                            from
                                (
                                    select distinct pos_dt,
                                        ltrim(sold_to_party, 0) as edw_sold_to_party,
                                        ltrim(ean_num, 0) as ean_num,
                                        sls_grp_cd
                                    from wks_edw_pos_fact_korea fact -- TAKE SALES GROUP FROM HERE
                                    where not exists (
                                            -- USE NOT IN instead of minus
                                            select 1
                                            from (
                                                select * from current_table
                                                union all
                                                select * from wave_1
                                                union all
                                                select * from wave_2
                                            ) lookup
                                            where lookup.pos_dt = fact.pos_dt
                                                and ltrim(lookup.sold_to_party, 0) = fact.sold_to_party
                                                and ltrim(lookup.ean_num, 0) = ltrim(fact.ean_num, 0)
                                        )
                                ) pos_data
                                inner join wks_pos_prc_condition_map map on pos_data.pos_dt between map.vld_frm and map.vld_to
                                and ltrim (pos_data.sls_grp_cd, 0) = ltrim (map.sales_grp_cd, 0) -- JOIN WITH SALES GROUP and NOT sold to
                                and ltrim (pos_data.ean_num, 0) = ltrim (map.ean_num, 0)
                                and map.cnd_type <> 'ZKSD' ---------- ZKSD to be ignored
                        ) pos_map
                    group by pos_dt,
                        edw_sold_to_party,
                        pc_sold_to_party,
                        sales_grp_cd,
                        ean_num,
                        matl_num,
                        matl_desc
                    having sum (calc_price) > 0
                        /*TAKE HIGHEST AMONG POSITIVE PRICES*/
                ) a
        ) src
    where src.rnk = 1
),
final as
(
    select
        pos_dt::date as pos_dt,
        ean_num::varchar(100) as ean_num,
        sold_to_party::varchar(100) as sold_to_party,
        calc_invoice_price::number(16,5) as calc_invoice_price,
        matl_num::varchar(40) as matl_num,
        matl_desc::varchar(100) as matl_desc,
        ctry_cd::varchar(10) as ctry_cd,
        wave::number(18,0) as wave,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        sls_grp_cd::varchar(18) as sls_grp_cd,
        pricing_sold_to::varchar(100) as pricing_sold_to
    from
        (
            select * from wave_1
            union all
            select * from wave_2
            union all
            select * from wave_3
            union all
            select * from current_table
        )
)
select * from final
