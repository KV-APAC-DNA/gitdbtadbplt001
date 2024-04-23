with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_px_weekly_sell') }}
),
transformed as
(
    select 
        cast(t1.promotionrowid as integer) as promotionrowid,
        cast(p_promonumber as varchar(10)) as p_promonumber,
        cast(transactionlongname as varchar(40)) as transactionlongname,
        cast(t1.gltt_rowid as float8) as gltt_rowid,
        cast(ac_longname as varchar(40)) as ac_longname,
        cast(ac_code as varchar(20)) as ac_code,
        cast(activity as varchar(40)) as activity,
        cast(t1.sku_stockcode as varchar(40)) as sku_stockcode,
        cast(sku_longname as varchar(40)) as sku_longname,
        cast(sku_profitcentre as varchar(40)) as sku_profitcentre,
        to_date(promotionforecastweek,'yyyymmdd') as promotionforecastweek,
        sum(cast(planspend_total as float8)) as planspend_total,
        case
            when sum(cast(t2.paid_total as numeric(21, 2))) = sum(cast(t2.committed_amount as numeric(21, 2))) then sum(cast(t1.committed_amount as float8))
            when sum(cast(t2.paid_total as numeric(21, 2))) != sum(cast(t2.committed_amount as numeric(21, 2)))
                and (max(t2.committed_amount_max) = min(t2.committed_amount_min)) 
            then sum(cast(t2.paid_total_e as float8))
            when sum(cast(t2.paid_total as numeric(21, 2))) != sum(cast(t2.committed_amount as numeric(21, 2)))
                and (max(t2.committed_amount_max) != min(t2.committed_amount_min)) 
            then sum(t2.paid_total) *(sum(cast(t1.committed_amount as float8)) / sum(t2.committed_amount))
        end as paid_total,
        sum(cast(writeback_tot as float8))  as writeback_tot,
        sum(cast(balance_tot as float8))  as balance_tot,
        sum(cast(case_quantity as float8))  as case_quantity,
        cast(t3.promotionitemstatus as varchar(40)) as promotionitemstatus
    from source t1
    left join 
    (
        select 
            distinct promotionrowid,
            gltt_rowid,
            sku_stockcode,
            max(cast(committed_amount as float8)) committed_amount_max,
            min(cast(committed_amount as float8)) committed_amount_min,
            sum(cast(committed_amount as float8)) committed_amount,
            sum(cast(paid_total as float8)) paid_total,
            sum(cast(paid_total as float8)) / count(sku_stockcode) as paid_total_e
        from source
        group by promotionrowid,
            gltt_rowid,
            sku_stockcode
    ) t2 --tranformation1
    on coalesce(t1.promotionrowid, '-999999') = coalesce(t2.promotionrowid, '-999999')
    and coalesce(t1.gltt_rowid, '-999999') = coalesce(t2.gltt_rowid, '-999999')
    and coalesce(t1.sku_stockcode, '-999999') = coalesce(t2.sku_stockcode, '-999999')
    left join 
    (
        select distinct promotionrowid,
            gltt_rowid,
            sku_stockcode,
            promotionitemstatus
        from source
        where promotionitemstatus is not null
    ) t3 
    on coalesce(t1.promotionrowid, '-999999') = coalesce(t3.promotionrowid, '-999999')
    and coalesce(t1.gltt_rowid, '-999999') = coalesce(t3.gltt_rowid, '-999999')
    and coalesce(t1.sku_stockcode, '-999999') = coalesce(t3.sku_stockcode, '-999999')
    group by 
        t1.PromotionRowId,
        t1.p_promonumber,
        t1.gltt_rowid,
        t1.TransactionLongName,
        t1.sku_stockcode,
        t1.ac_longname,
        t1.Activity,
        t1.ac_code,
        t1.sku_longname,
        t1.sku_profitcentre,
        t1.PromotionForecastWeek,
        t3.promotionitemstatus
),
final as
(
    select
        promotionrowid::number(18,0) as promotionrowid,
        p_promonumber::varchar(10) as p_promonumber,
        transactionlongname::varchar(40) as transactionlongname,
        gltt_rowid::float as gltt_rowid,
        ac_longname::varchar(40) as ac_longname,
        ac_code::varchar(20) as ac_code,
        activity::varchar(40) as activity,
        sku_stockcode::varchar(40) as sku_stockcode,
        sku_longname::varchar(40) as sku_longname,
        sku_profitcentre::varchar(40) as sku_profitcentre,
        promotionforecastweek::timestamp_ntz(9) as promotionforecastweek,
        planspend_total::float as planspend_total,
        paid_total::float as paid_total,
        writeback_tot::float as writeback_tot,
        balance_tot::float as balance_tot,
        case_quantity::float as case_quantity,
        promotionitemstatus::varchar(40) as promotionitemstatus
    from transformed
)
select * from final