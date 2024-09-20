with edw_copa_trans_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_company_dim as
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
itg_custgp_cogs_fg_control as
(
    select * from {{ source('aspitg_integration', 'itg_custgp_cogs_fg_control') }}
),
itg_ecc_standard_cost_history as
(
    select * from {{ ref('aspitg_integration__itg_ecc_standard_cost_history') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_customer_base_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
itg_mds_pre_apsc_master as
(
    select * from {{ ref('aspitg_integration__itg_mds_pre_apsc_master') }}
),
sdl_customerpl_stdcost_2022 as
(
    select * from {{ source('aspsdl_raw', 'sdl_customerpl_stdcost_2022') }}
),
copa as
(
    ---Fetch COGS and Free Goods from COPA-----
    SELECT DISTINCT fisc_yr,
        fisc_yr_per,
        cmp.ctry_group,
        cogs.co_cd,
        prft_Ctr,
        (CASE 
            WHEN nullif(sls_org,'') IS NULL THEN 
            (CASE 
                WHEN cogs.co_cd = '4130' THEN '2100' 
                WHEN cogs.co_cd = '8266' THEN '320S' 
                WHEN cogs.co_cd = '4481' THEN '2210' 
                ELSE nullif(sls_org,'')
                END) 
            WHEN nullif(sls_org,'') = '2400' AND ltrim(coalesce(cust_num,'0'),'0') NOT IN ('41812332','41802332') 
                THEN '2500' 
            ELSE nullif(sls_org,'')
            END) as sls_org,
        obj_crncy_co_obj,
        LTRIM(nullif(cust_num,''),'0') AS cust_num,
        LTRIM(nullif(matl_num,''),'0') AS matl_num,
        CASE
            WHEN cogs.acct_hier_shrt_desc = 'NTS' THEN SUM(amt_obj_crncy)
            ELSE 0
        END AS nts,
        CASE
            WHEN cogs.acct_hier_shrt_desc = 'FG' THEN SUM(amt_obj_crncy)
            ELSE 0
        END AS freegood_amt,
        CASE
            WHEN cogs.acct_hier_shrt_desc = 'NTS' THEN SUM(round(qty))
            ELSE 0
        END AS nts_volume
    FROM edw_copa_trans_fact cogs
    LEFT JOIN edw_company_dim cmp ON cogs.co_cd = cmp.co_cd
    INNER JOIN itg_custgp_cogs_fg_control fgctl on cogs.acct_hier_shrt_desc	= fgctl.acct_hier_shrt_desc and 	
                                                        cogs.co_cd = fgctl.co_cd and 
                                                        cogs.fisc_yr_per >= fgctl.valid_from and
                                                        cogs.fisc_yr_per < fgctl.valid_to and 
                                                        case when cogs.acct_hier_shrt_desc = 'FG' 
                                                                then ltrim(cogs.acct_num,'0') = fgctl.gl_acct_num 
                                                                when cogs.acct_hier_shrt_desc = 'NTS' 
                                                                then '0' = fgctl.gl_acct_num end and
                                                        fgctl.active = 'Y'													   

    WHERE   cogs.fisc_yr::CHARACTER VARYING::TEXT >= 2023 and cogs.acct_hier_shrt_desc in ('NTS','FG')
    and ltrim(coalesce(cust_num,'0'),'0') not in ('140327','140328')
    GROUP BY fisc_yr,
            fisc_yr_per,
            cogs.co_cd,
            cmp.ctry_group,
            prft_ctr,
            CASE 
            WHEN nullif(sls_org,'') IS NULL THEN 
            (CASE 
                WHEN cogs.co_cd = '4130' THEN '2100' 
                WHEN cogs.co_cd = '8266' THEN '320S' 
                WHEN cogs.co_cd = '4481' THEN '2210' 
                ELSE nullif(sls_org,'')
                END) 
            WHEN nullif(sls_org,'') = '2400' AND ltrim(coalesce(cust_num,'0'),'0') NOT IN ('41812332','41802332') 
                THEN '2500' 
            ELSE nullif(sls_org,'')
            END,
            obj_crncy_co_obj,
            LTRIM(nullif(cust_num,''),'0'),
            LTRIM(nullif(matl_num,''),'0'),
            cogs.acct_hier_shrt_desc
),
stdc as
(
    select DISTINCT (CASE WHEN bwkey = '2400' AND LTRIM(matnr,'0') NOT IN ('41812332','41802332') THEN '2500' 
                    ELSE bwkey END) as bwkey,
                    ltrim(matnr,'0') as matnr,
                    nvl(nullif(lfgja||'0'||lpad(lfmon,2,'0'),'0000'),'1900001') as valid_from,                  
                    nvl(lead(lfgja||'0'||lpad(lfmon,2,'0'),1) over (partition by bwkey,ltrim(matnr,'0') order by lfgja||'0'||lpad(lfmon,2,'0')),'2099012') as valid_to, 
                    peinh as unit,
                    stprs as std_price
    from itg_ecc_standard_cost_history
),
emd as 
(
    select matl_num,matl_desc from edw_material_dim
),
ecd as 
(
    select distinct cust_num,cust_nm from edw_customer_base_dim
),
pac as
(
    select market_code,materialnumber,materialdescription,valid_from, 
                nvl(valid_to,'2099012') as valid_to, pre_apsc_per_pc as pre_apsc_cper_pc
         from 
        (
          select market_code,materialnumber,materialdescription, year_code||'0'||lpad(month_code,2,'0') as valid_from, 
                 lead(year_code||'0'||lpad(month_code,2,'0'),1) over (partition by market_code, materialnumber order by year_code||'0'||lpad(month_code,2,'0') ) as valid_to,
                  pre_apsc_per_pc 
            from itg_mds_pre_apsc_master 
         )  base 

        order by market_code, materialnumber, valid_from
),
copa1 as
(
    ---Fetch COGS and Free Goods from COPA-----
    SELECT DISTINCT fisc_yr,
        fisc_yr_per,
        cmp.ctry_group,
        cogs.co_cd,
        prft_Ctr,
        (CASE 
            WHEN sls_org IS NULL THEN 
            (CASE 
                WHEN cogs.co_cd = '4130' THEN '2100' 
                WHEN cogs.co_cd = '8266' THEN '320S' 
                WHEN cogs.co_cd = '4481' THEN '2210' 
                ELSE sls_org 
                END) 
            WHEN sls_org = '2400' AND LTRIM(matl_num,'0') NOT IN ('41812332','41802332') 
                THEN '2500' 
            ELSE sls_org 
            END) as sls_org,
        obj_crncy_co_obj,
        LTRIM(cust_num,'0') AS cust_num,
        LTRIM(matl_num,'0') AS matl_num,
        CASE
            WHEN cogs.acct_hier_shrt_desc = 'NTS' THEN SUM(amt_obj_crncy)
            ELSE 0
        END AS nts,
        CASE
            WHEN cogs.acct_hier_shrt_desc = 'FG' THEN SUM(amt_obj_crncy)
            ELSE 0
        END AS freegood_amt,
        CASE
            WHEN cogs.acct_hier_shrt_desc = 'NTS' THEN SUM(round(qty))
            ELSE 0
        END AS nts_volume
    FROM edw_copa_trans_fact cogs
    LEFT JOIN edw_company_dim cmp ON cogs.co_cd = cmp.co_cd
    INNER JOIN itg_custgp_cogs_fg_control fgctl on cogs.acct_hier_shrt_desc	= fgctl.acct_hier_shrt_desc and 	
                                                        cogs.co_cd = fgctl.co_cd and 
                                                        cogs.fisc_yr_per >= fgctl.valid_from and
                                                        cogs.fisc_yr_per < fgctl.valid_to and 
                                                        case when cogs.acct_hier_shrt_desc = 'FG' 
                                                                then ltrim(cogs.acct_num,'0') = fgctl.gl_acct_num 
                                                                when cogs.acct_hier_shrt_desc = 'NTS' 
                                                                then '0' end = fgctl.gl_acct_num and
                                                        fgctl.active = 'Y'
                                                        
    WHERE   cogs.fisc_yr::CHARACTER VARYING::TEXT = 2022 and cogs.acct_hier_shrt_desc in ('NTS','FG')
    GROUP BY fisc_yr,
            fisc_yr_per,
            cogs.co_cd,
            cmp.ctry_group,
            prft_ctr,
            sls_org,
            obj_crncy_co_obj,
            LTRIM(cust_num,'0'),
            LTRIM(matl_num,'0'),
            cogs.acct_hier_shrt_desc
),
stdc1 as
(
    select case when market ='TH' then 'Thailand'
        when market ='HK' then 'Hongkong'
        when market ='KR' then 'Korea'
        when market ='TW' then 'Taiwan'
        when market ='VN' then 'Vietnam'
        when market ='SG' then 'Singapore'
        when market ='MY' then 'Malaysia'
        end as Ctry
        , Materialnumber
        , (StdCost/Qty) as StdCostperUnit
    from sdl_customerpl_stdcost_2022
),
emd1 as
(
    select matl_num,matl_desc from edw_material_dim
),
ecd1 as 
(
    select distinct cust_num,cust_nm from edw_customer_base_dim
),
pac1 as
(
    select market_code,materialnumber,materialdescription,valid_from, 
                nvl(valid_to,'2099012') as valid_to, pre_apsc_per_pc as pre_apsc_cper_pc
         from 
        (
          select market_code,materialnumber,materialdescription, year_code||'0'||lpad(month_code,2,'0') as valid_from, 
                 lead(year_code||'0'||lpad(month_code,2,'0'),1) over (partition by market_code, materialnumber order by year_code||'0'||lpad(month_code,2,'0') ) as valid_to,
                 pre_apsc_per_pc 
            from itg_mds_pre_apsc_master 
         )  base 

        order by market_code, materialnumber, valid_from
),
final as
(
    SELECT DISTINCT copa.fisc_yr,
        copa.fisc_yr_per AS period,
        copa.co_cd,
        copa.obj_crncy_co_obj AS currency,
        copa.sls_org AS plant,
        copa.prft_Ctr AS profit_cntr,
        copa.matl_num,
        emd.matl_desc,
        copa.cust_num,
        ecd.cust_nm,
        copa.nts,
        copa.nts_volume,
        copa.freegood_amt AS free_goods_value,
        stdc.std_price AS standard_cost,
        copa.freegood_amt*stdc.std_price AS freegoods_cost_per_unit,
        stdc.unit,
        case when sls_org = '320S' 
                then standard_cost/unit*100
             when sls_org = '260S' or sls_org = '260A'
                then standard_cost/unit*100
                else standard_cost/unit end as Standard_cost_per_unit,
        pac.pre_apsc_cper_pc,
        copa.nts_volume*pac.pre_apsc_cper_pc AS COGS_at_Pre_APSC,
        ((CASE
            WHEN Standard_cost_per_unit = 0 THEN NULL
            ELSE (free_goods_value) / Standard_cost_per_unit
        END 
    )*pac.pre_apsc_cper_pc) * -1 AS Free_Goods_COGS_at_Pre_APSC,
        COGS_at_Pre_APSC + Free_Goods_COGS_at_Pre_APSC AS Total_APSC
    FROM copa
    LEFT JOIN stdc
            ON copa.matl_num = stdc.matnr
            AND copa.sls_org = stdc.bwkey
            AND copa.fisc_yr_per >= stdc.valid_from  
            AND copa.fisc_yr_per < stdc.valid_to 
    LEFT JOIN emd ON LTRIM (emd.matl_num,'0') = copa.matl_num
    LEFT JOIN ecd ON LTRIM (ecd.cust_num,'0') = copa.cust_num
    LEFT JOIN pac
            ON pac.materialnumber = copa.matl_num
            AND pac.market_code = copa.ctry_group
            AND copa.fisc_yr_per >= pac.valid_from  
            AND copa.fisc_yr_per < pac.valid_to

    UNION ALL

    SELECT DISTINCT copa1.fisc_yr,
        copa1.fisc_yr_per AS period,
        copa1.co_cd,
        copa1.obj_crncy_co_obj AS currency,
        copa1.sls_org AS plant,
        copa1.prft_Ctr AS profit_cntr,
        copa1.matl_num,
        emd1.matl_desc,
        copa1.cust_num,
        ecd1.cust_nm,
        copa1.nts,
        copa1.nts_volume,
        copa1.freegood_amt AS free_goods_value,
        round(stdc1.StdCostperUnit,2) AS standard_cost,
        copa1.freegood_amt*stdc1.StdCostperUnit AS freegoods_cost_per_unit,
        1 as unit,
        StdCostperUnit AS Standard_cost_per_unit,
        pac1.pre_apsc_cper_pc,
        copa1.nts_volume*pac1.pre_apsc_cper_pc AS COGS_at_Pre_APSC,
        ((CASE
                WHEN Standard_cost_per_unit = 0 THEN NULL
                ELSE (free_goods_value) / Standard_cost_per_unit
            END 
                )*pac1.pre_apsc_cper_pc) * -1 AS Free_Goods_COGS_at_Pre_APSC,
        COGS_at_Pre_APSC + Free_Goods_COGS_at_Pre_APSC AS Total_APSC
    FROM copa1
    LEFT JOIN stdc1
            ON copa1.matl_num = stdc1.Materialnumber
            AND copa1.ctry_group = stdc1.Ctry
    LEFT JOIN emd1 ON LTRIM (emd1.matl_num,'0') = copa1.matl_num
    LEFT JOIN ecd1 ON LTRIM (ecd1.cust_num,'0') = copa1.cust_num
    LEFT JOIN pac1
            ON pac1.materialnumber = copa1.matl_num
            AND pac1.market_code = copa1.ctry_group
            AND copa1.fisc_yr_per >= pac1.valid_from  
            AND copa1.fisc_yr_per < pac1.valid_to
)
select fisc_yr::number(38,0) as fisc_yr,
    period::number(38,0) as period,
    co_cd::varchar(4) as co_cd,
    currency::varchar(5) as currency,
    plant::varchar(4) as plant,
    profit_cntr::varchar(10) as profit_cntr,
    matl_num::varchar(18) as matl_num,
    matl_desc::varchar(100) as matl_desc,
    cust_num::varchar(10) as cust_num,
    cust_nm::varchar(100) as cust_nm,
    nts::number(38,5) as nts,
    nts_volume::number(38,0) as nts_volume,
    free_goods_value::number(38,5) as free_goods_value,
    standard_cost::number(11,2) as standard_cost,
    freegoods_cost_per_unit::number(38,7) as freegoods_cost_per_unit,
    unit::number(5,0) as unit,
    standard_cost_per_unit::number(21,8) as standard_cost_per_unit,
    pre_apsc_cper_pc::number(28,4) as pre_apsc_cper_pc,
    cogs_at_pre_apsc::number(38,4) as cogs_at_pre_apsc,
    free_goods_cogs_at_pre_apsc::number(38,8) as free_goods_cogs_at_pre_apsc,
    total_apsc::number(38,8) as total_apsc
 from final