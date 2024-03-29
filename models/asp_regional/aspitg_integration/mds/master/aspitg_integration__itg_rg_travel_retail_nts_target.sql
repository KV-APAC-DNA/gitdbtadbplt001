--Import CTE
with dt as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_apac_dcl_targets') }}
),
th as (
    select * from {{ source('aspwks_integration', 'wks_apac_dcl_targets_header') }}
),

--Logical CTE
jan as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.jan::number(18,0) as month,
        (
            trim(year) || th.jan
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.jan, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),
feb as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.feb::number(18,0) as month,
        (
            trim(year) || th.feb
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.feb, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),
mar as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.mar::number(18,0) as month,
        (
            trim(year) || th.mar
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.mar, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),
apr as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.apr::number(18,0) as month,
        (
            trim(year) || th.apr
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.apr, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),
may as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.may::number(18,0) as month,
        (
            trim(year) || th.may
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.may, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),
jun as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.jun::number(18,0) as month,
        (
            trim(year) || th.jun
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.jun, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),
jul as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.jul::number(18,0) as month,
        (
            trim(year) || th.jul
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.jul, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),
aug as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.aug::number(18,0) as month,
        (
            trim(year) || th.aug
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.aug, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),
sep as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.sep::number(18,0) as month,
        (
            trim(year) || th.sep
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.sep, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),
oct as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.oct::number(18,0) as month,
        (
            trim(year) || th.oct
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.oct, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),
nov as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.nov::number(18,0) as month,
        (
            trim(year) || th.nov
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.nov, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),
dec as (
    select
        upper(trim(target_type_code))::varchar(10) as target_type,
        upper(trim(target_type_name))::varchar(50) as target_type_name,
        trim(year_code)::number(18,0) as year,
        th.dec::number(18,0) as month,
        (
            trim(year) || th.dec
        )::varchar(10) as year_month,
        upper(trim(country_code))::varchar(5) as ctry_cd,
        trim(country_name)::varchar(30) as country_name,
        case
            when upper(trim(country_code)) = 'KR'
            then 'KRW'
            when upper(trim(country_code)) = 'HK'
            then 'HKD'
            when upper(trim(country_code)) = 'SG'
            then 'SGD'
            when upper(trim(country_code)) = 'CN'
            then 'RMB'
            when upper(trim(country_code)) = 'TW'
            then 'TWD'
        end :: varchar(3) as crncy_cd,
        trim(sales_channel_name)::varchar(50) as sales_channel_name,
        coalesce(dt.dec, 0) as nts_tgt,
        trim(enterdatetime)::timestamp_ntz(9) as crt_dttm
    from dt
    join th on upper(trim(dt.target_type_code)) = upper(th.target_type)
),

transformed as (
    select * from jan
    union all
    select * from feb
    union all
    select * from mar
    union all
    select * from apr
    union all
    select * from may
    union all
    select * from jun
    union all
    select * from jul
    union all
    select * from aug
    union all
    select * from sep
    union all
    select * from oct
    union all
    select * from nov
    union all
    select * from dec    
),

--Final CTE
final as (
    select 
        target_type,
        target_type_name,
        year,
        month,
        year_month,
        ctry_cd,
        country_name,
        crncy_cd,
        sales_channel_name as sales_channel,
        truncate(nts_tgt,5)::number(21,5) as nts_tgt,
        crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from transformed
)
--Final select
select * from final
