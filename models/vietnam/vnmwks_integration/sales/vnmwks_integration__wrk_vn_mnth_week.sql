with edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
weeks as (
    SELECT DISTINCT 
        t."year",
        t.qrtr_no,
        t.qrtr,
        t.mnth_id,
        t.mnth_desc,
        t.mnth_no,
        t.mnth_shrt,
        t.mnth_long,
        t.wk,
        t.mnth_wk_no,
        min(t.cal_date_id) AS frst_day,
        max(t.cal_date_id) AS lst_day,
        count(mnth_wk_no) over (partition by mnth_id) cnt
    FROM edw_vw_os_time_dim t
    GROUP BY 
        t."year",
        t.qrtr_no,
        t.qrtr,
        t.mnth_id,
        t.mnth_desc,
        t.mnth_no,
        t.mnth_shrt,
        t.mnth_long,
        t.wk,
        t.mnth_wk_no
    order by mnth_id,
        mnth_wk_no
),
union_1 as
(
    select w.*,
        lag (mnth_id, 1) over(order by rn desc) as p_1,
        lag (mnth_id, 2) over(order by rn desc) as p_2
    from 
    (
        select distinct mnth_id,
            row_number() over (order by mnth_id desc) rn
        from (
                select distinct mnth_id from weeks where cnt = 5
            )
        order by rn desc
    ) w
               
),
union_2 as
(
    select w.*,
        lag (mnth_id, 1) over(order by rn desc) as p_1,
        lag (mnth_id, 2) over(order by rn desc) as p_2
    from 
    (
        select distinct mnth_id,
            row_number() over (order by mnth_id desc) rn
        from (
                select distinct mnth_id from weeks where cnt = 4
            )
        order by rn desc
    ) w
               
),
p as(
        select * from union_1 
        union all
        select * from union_2
),
transformed as
(
    select 
        prim_month."year"::number(18,0) as year,
        prim_month.qrtr_no::number(18,0) as qrtr_no,
        prim_month.qrtr::varchar(14) as qrtr,
        prim_month.mnth_id::varchar(23) as mnth_id,
        prim_month.mnth_desc::varchar(15) as mnth_desc,
        prim_month.mnth_no::number(18,0) as mnth_no,
        prim_month.mnth_shrt::varchar(3) as mnth_shrt,
        prim_month.mnth_long::varchar(9) as mnth_long,
        prim_month.wk::number(38,0) as wk,
        prim_month.mnth_wk_no::number(38,0) as mnth_wk_no,
        prim_month.frst_day::varchar(13) as frst_day,
        prim_month.lst_day::varchar(13) as lst_day,
        prim_month.cnt::number(38,0) as cnt,
        p.p_1::varchar(23) as p_1,
        p.p_2::varchar(23) as p_2,
    from weeks prim_month,p
    where prim_month.mnth_id = p.mnth_id (+)
    order by mnth_id,wk
)
select * from transformed