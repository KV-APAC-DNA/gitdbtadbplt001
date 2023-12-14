{{
    config(
        alias="wks_edw_profit_center_dim",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        tags=[""]
    )
}}

with 

itg_prft_ctr as (

    select * from {{ ref('aspitg_integration__itg_prft_ctr') }}
),

itg_prft_ctr_text as (

    select * from {{ ref('aspitg_integration__itg_prft_ctr_text') }}
),

itg_prft_ctr_filtered as (
    select
        *
      from (
        select
          cntl_area,
          prft_ctr,
          vld_to_dt,
          vld_from_dt,
          prsn_resp,
          crncy_key,
          strng_hold,
          need_stat,
          rank() over (partition by prft_ctr order by vld_from_dt desc) as rnk
        from itg_prft_ctr
        where
          not cntl_area is null and cntl_area <> '' and not prft_ctr is null and prft_ctr <> ''
      )
      where
        rnk = 1
),

itg_prft_ctr_text_filtered as (
    select
        *
      from (
        select
          lang_key,
          cntl_area,
          prft_ctr,
          vld_to_dt,
          vld_from_dt,
          shrt_desc,
          med_desc,
          rank() over (partition by prft_ctr order by vld_from_dt desc) as rnk
        from itg_prft_ctr_text
        where
          lang_key = 'E'
      )
      where
        rnk = 1
),

--joins
final_temp as (
    select
      a.*,
      b.lang_key as lang_key,
      b.shrt_desc as shrt_desc,
      b.med_desc as med_desc
    from itg_prft_ctr_filtered as a
    left outer join itg_prft_ctr_text_filtered as b 
    on ltrim(a.prft_ctr, 0) = ltrim(b.prft_ctr, 0)
),

final as (
    select
        lang_key,
        cntl_area,
        prft_ctr,
        vld_to_dt,
        vld_from_dt,
        shrt_desc,
        med_desc,
        prsn_resp,
        crncy_key,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from final_temp
)

select * from final