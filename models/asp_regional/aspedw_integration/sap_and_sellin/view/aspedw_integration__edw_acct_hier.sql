{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="view"
    )
}}

with 
edw_account_dim as (
    select * from {{ ref('aspedw_integration__edw_account_dim') }}
),

edw_account_jjplbw_hier as (
    select
        chrt_acct,
        acct_num,
        cast('' as varchar) as acct_nm,
        cast('acct_num' as varchar) as acct_col,
        hier_node as bravo_acct_no
      from {{source('aspedw_integration','edw_account_jjplbw_hier')}}
      where
        (
          length(cast((
            hier_node
          ) as text)) <> 0
        )
),
edw_account_xref as (
    select * from {{source('aspedw_integration', 'edw_account_xref')}}
),

bravo_acct_l1 as (
    select
        chrt_acct,
        acct_num,
        acct_nm,
        cast('bravo_acct_l1' as varchar) as acct_col,
        bravo_acct_l1 as bravo_acct_no
        from edw_account_dim
        where
        (length(cast((bravo_acct_l1) as text)) <> 0)
),

bravo_acct_l2 as (
    select
                chrt_acct,
                acct_num,
                acct_nm,
                cast('bravo_acct_l2' as varchar) as acct_col,
                bravo_acct_l2 as bravo_acct_no
              from edw_account_dim
              where
                (length(cast((bravo_acct_l2) as text)) <> 0)
),

bravo_acct_l3 as (
    select
              chrt_acct,
              acct_num,
              acct_nm,
              cast('bravo_acct_l3' as varchar) as acct_col,
              bravo_acct_l3 as bravo_acct_no
            from edw_account_dim
            where
              (length(cast((bravo_acct_l3) as text)) <> 0)
),

bravo_acct_l4 as (
    select
            chrt_acct,
            acct_num,
            acct_nm,
            cast('bravo_acct_l4' as varchar) as acct_col,
            bravo_acct_l4 as bravo_acct_no
          from edw_account_dim
          where
            (length(cast((bravo_acct_l4) as text)) <> 0)
),

bravo_acct_l5 as (
    select
          chrt_acct,
          acct_num,
          acct_nm,
          cast('bravo_acct_l5' as varchar) as acct_col,
          bravo_acct_l5 as bravo_acct_no
        from edw_account_dim
        where
          (length(cast((bravo_acct_l5) as text)) <> 0)
),
acct_hier_dim_derived as(
    select * from bravo_acct_l1
    union all
    select * from bravo_acct_l2
    union all
    select * from bravo_acct_l3
    union all
    select * from bravo_acct_l4
    union all
    select * from bravo_acct_l5
    union all
    select * from edw_account_jjplbw_hier
),

acct_hier_dim_transformed as (
    select
        a.chrt_acct,
        a.acct_num,
        a.acct_nm,
        a.acct_col,
        a.bravo_acct_no,
        b.measure_code,
        b.measure_name,
        b.multiplication_factor
    from acct_hier_dim_derived as a
    join edw_account_xref as b
        on (((upper(cast((a.acct_col) as text)) = upper(cast((b.lookup_col_name) as text)))
        and (cast((a.bravo_acct_no) as text) = cast((b.lookup_value) as text))))
),


final as (
    select * from acct_hier_dim_transformed 
)

select * from final