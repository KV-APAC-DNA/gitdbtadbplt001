with v_intrm_reg_crncy_exch_fiscper as
(
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
sdl_mds_jp_ecom_nts as
(
    select * from {{ source('jpdclsdl_raw', 'sdl_mds_jp_ecom_nts') }}
),
sdl_mds_cn_ecom_nts as
(
    select * from {{ source('chnsdl_raw', 'sdl_mds_cn_ecom_nts') }}
),
sdl_mds_vn_ecom_nts as
(
    select * from {{ source('vnmsdl_raw', 'sdl_mds_vn_ecom_nts') }}
),
sdl_mds_pacific_ecom_nts as
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_ecom_nts') }}
),
final as
(
    Select 
        convert_timezone('UTC',current_timestamp()) as load_date,
        jp_ecomm_nts.year, 
        jp_ecomm_nts.month, 
        jp_ecomm_nts.market as country, 
        jp_ecomm_nts.gfo, 
        jp_ecomm_nts.need_state, 
        jp_ecomm_nts.brand, 
        jp_ecomm_nts.customer_name,
        crncy.ex_rt as ex_rt_to_usd,
        jp_ecomm_nts.nts_lcy,
        jp_ecomm_nts.nts_lcy * ex_rt AS nts_usd, 
        jp_ecomm_nts.market as subject_area,
        jp_ecomm_nts.crncy_cd AS from_crncy
    from 
    (
        select 
        year,
        month,
        year || '0' ||
        case 
        when month = '1' then '01'
        when month = '2' then '02'
        when month = '3' then '03'
        when month = '4' then '04'
        when month = '5' then '05'
        when month = '6' then '06'
        when month = '7' then '07'
        when month = '8' then '08'
        when month = '9' then '09'
        when month = '10' then '10'
        when month = '11' then '11'
        when month = '12' then '12'
        end AS fisc_per,
        market,
        gfo, 
        need_state, 
        brand, 
        customer_name,	
        nts_lcy AS nts_lcy,
        crncy_cd AS crncy_cd  
        from sdl_mds_jp_ecom_nts 
    ) jp_ecomm_nts
    left outer join
    (
    select * from v_intrm_reg_crncy_exch_fiscper
    where
    to_crncy = 'USD'
    ) crncy
    on
    jp_ecomm_nts.fisc_per = crncy.fisc_per
    and
    jp_ecomm_nts.crncy_cd = crncy.from_crncy 

    UNION ALL

    Select 
        convert_timezone('UTC',current_timestamp()) as load_date,
        cn_ecomm_nts.year, 
        cn_ecomm_nts.month, 
        cn_ecomm_nts.market, 
        cn_ecomm_nts.gfo, 
        cn_ecomm_nts.need_state, 
        cn_ecomm_nts.brand, 
        cn_ecomm_nts.customer_name,
        crncy.ex_rt AS ex_rt,
        cn_ecomm_nts.nts_lcy,
        cn_ecomm_nts.nts_lcy * ex_rt AS nts_usd, 
        cn_ecomm_nts.subject_area as subject_area,
        cn_ecomm_nts.crncy_cd AS from_crncy
    from 
    (
        select 
        year,
        month,
        year || '0' ||
        case 
        when month = '1' then '01'
        when month = '2' then '02'
        when month = '3' then '03'
        when month = '4' then '04'
        when month = '5' then '05'
        when month = '6' then '06'
        when month = '7' then '07'
        when month = '8' then '08'
        when month = '9' then '09'
        when month = '10' then '10'
        when month = '11' then '11'
        when month = '12' then '12'
        end AS fisc_per,
        market_code as market,
        gfo, 
        need_state, 
        brand, 
        customer_name,	
        nts_lcy AS nts_lcy,
        case
            when market_code = 'China Selfcare' then 'ChinaSelfcare' 
            when market_code = 'China Selfcare (O2O)' then 'ChinaSelfcare_O2O' 
            when market_code = 'China Omni' then 'ChinaOmni' 
        end as subject_area,
        crncy_cd AS crncy_cd  
        from sdl_mds_cn_ecom_nts 
    ) cn_ecomm_nts
    left outer join
    (
    select * from v_intrm_reg_crncy_exch_fiscper
    where
    to_crncy = 'USD'
    ) crncy
    on
    cn_ecomm_nts.fisc_per = crncy.fisc_per
    and
    cn_ecomm_nts.crncy_cd = crncy.from_crncy 

    UNION ALL

    Select 
        convert_timezone('UTC',current_timestamp()) as load_date,
        vn_ecomm_nts.year, 
        vn_ecomm_nts.month, 
        vn_ecomm_nts.market, 
        vn_ecomm_nts.gfo, 
        vn_ecomm_nts.need_state, 
        vn_ecomm_nts.brand, 
        vn_ecomm_nts.customer_name,
        crncy.ex_rt AS ex_rt,
        vn_ecomm_nts.nts_lcy,
        vn_ecomm_nts.nts_lcy * ex_rt AS nts_usd,
        vn_ecomm_nts.market AS subject_area,
        vn_ecomm_nts.crncy_cd AS from_crncy
    from 
    (
        select 
        year,
        month,
        year || '0' ||
        case 
        when month = '1' then '01'
        when month = '2' then '02'
        when month = '3' then '03'
        when month = '4' then '04'
        when month = '5' then '05'
        when month = '6' then '06'
        when month = '7' then '07'
        when month = '8' then '08'
        when month = '9' then '09'
        when month = '10' then '10'
        when month = '11' then '11'
        when month = '12' then '12'
        end AS fisc_per,
        market,
        gfo, 
        need_state, 
        brand, 
        customer_name,	
        nts_lcy AS nts_lcy,
        crncy_cd AS crncy_cd  
        from sdl_mds_vn_ecom_nts 
    ) vn_ecomm_nts
    left outer join
    (
    select * from v_intrm_reg_crncy_exch_fiscper
    where
    to_crncy = 'USD'
    ) crncy
    on
    vn_ecomm_nts.fisc_per = crncy.fisc_per
    and
    vn_ecomm_nts.crncy_cd = crncy.from_crncy

    UNION ALL

    Select 
        convert_timezone('UTC',current_timestamp()) as load_date,
        pac_ecomm_nts.year, 
        pac_ecomm_nts.month, 
        pac_ecomm_nts.market, 
        pac_ecomm_nts.gfo, 
        pac_ecomm_nts.need_state, 
        pac_ecomm_nts.brand, 
        pac_ecomm_nts.customer_name,
        crncy.ex_rt AS ex_rt,
        pac_ecomm_nts.nts_lcy,
        pac_ecomm_nts.nts_lcy * ex_rt AS nts_usd, 
        pac_ecomm_nts.market AS subject_area,
        pac_ecomm_nts.crncy_cd AS from_crncy
    from 
    (
        select 
        year,
        month,
        year || '0' ||
        case 
        when month = '1' then '01'
        when month = '2' then '02'
        when month = '3' then '03'
        when month = '4' then '04'
        when month = '5' then '05'
        when month = '6' then '06'
        when month = '7' then '07'
        when month = '8' then '08'
        when month = '9' then '09'
        when month = '10' then '10'
        when month = '11' then '11'
        when month = '12' then '12'
        end AS fisc_per,
        market,
        gfo, 
        need_state, 
        brand, 
        customer_name,	
        nts_lcy AS nts_lcy,
        crncy_cd AS crncy_cd  
        from sdl_mds_pacific_ecom_nts 
    ) pac_ecomm_nts
    left outer join
    (
    select * from v_intrm_reg_crncy_exch_fiscper
    where
    to_crncy = 'USD'
    ) crncy
    on
    pac_ecomm_nts.fisc_per = crncy.fisc_per
    and
    pac_ecomm_nts.crncy_cd = crncy.from_crncy
)
select * from final