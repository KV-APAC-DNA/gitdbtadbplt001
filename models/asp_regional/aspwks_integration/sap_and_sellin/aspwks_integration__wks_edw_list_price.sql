
with itg_list_price as (
    select * from {{ ref('aspitg_integration__itg_list_price') }}
),

itg_list_price_ranked1 as (
    select
        row_number() over (partition by material, sls_org, cond_rec_no, to_date(valid_to, 'YYYYMMDD'), to_date(dt_from, 'YYYYMMDD') order by crtd_dttm desc) as rn,
        *
    from {{ ref('aspitg_integration__itg_list_price') }}
),
itg_list_price_ranked1_latest as (
    select * from itg_list_price_ranked1 where rn=1
),
itg_list_price_ranked2 as (
    select
        row_number() over (partition by material, sls_org order by to_date(valid_to, 'YYYYMMDD') desc, to_date(dt_from, 'YYYYMMDD') desc, cond_rec_no desc,crtd_dttm desc) as rn,
        *
      from {{ ref('aspitg_integration__itg_list_price') }}
),
itg_list_price_ranked2_latest as (
    select * from itg_list_price_ranked2 where rn=1 and amount = 0 and sls_org::string = '2100'
),
itg_list_price_ranked3 as (
    select
        sls_org,
        material,
        cond_rec_no,
        valid_to,
        dt_from,
        amount,
        coalesce(
          first_value(nullif(amount, 0) ignore nulls) over (partition by material, sls_org order by crtd_dttm desc, to_date(valid_to, 'YYYYMMDD') desc, to_date(dt_from, 'yyyymmdd') desc, cond_rec_no desc rows between current row and unbounded following),
          0
        ) as prev_value,
        row_number() over (partition by material, sls_org order by crtd_dttm desc, to_date(valid_to, 'YYYYMMDD') desc, to_date(dt_from, 'YYYYMMDD') desc, cond_rec_no desc) as rn
      from itg_list_price
),
itg_list_price_ranked3_latest as (
    select * from itg_list_price_ranked3 where rn=1 and amount = 0 and sls_org::string = '2100'
),
transformed as (
select
  sls_org,
  material,
  cond_rec_no,
  matl_grp,
  valid_to,
  knart,
  dt_from,
  amount,
  currency,
  unit,
  record_mode,
  comp_cd,
  price_unit,
  zcurrfpa,
  cdl_dttm,
  crtd_dttm,
  updt_dttm
from itg_list_price_ranked1_latest
),
transformed_2 as (
    select
    wks.sls_org,
    wks.material,
    wks.cond_rec_no,
    wks.valid_to,
    wks.dt_from,
    itg.prev_value as amount
  from (
    select
      sls_org,
      material,
      cond_rec_no,
      valid_to,
      dt_from,
      amount
    from itg_list_price_ranked2_latest
  ) as wks
  left join (
    select
      sls_org,
      material,
      prev_value
    from itg_list_price_ranked3_latest
  ) as itg
    on trim(wks.sls_org::string) = trim(itg.sls_org::string)
    and trim(wks.material, 0) = trim(itg.material, 0)
),
final as (
    select 
       -- concat(transformed.material,'_',transformed.sls_org,'_',transformed.cond_rec_no,'_',transformed.dt_from,'_',transformed.valid_to) as c_pk,
        transformed.sls_org,
        transformed.material,
        transformed.cond_rec_no,
        transformed.matl_grp,
        transformed.valid_to,
        transformed.knart,
        transformed.dt_from,
        coalesce(transformed_2.amount,transformed.amount) as amount,
        transformed.currency,
        transformed.unit,
        transformed.record_mode,
        transformed.comp_cd,
        transformed.price_unit,
        transformed.zcurrfpa,
        transformed.cdl_dttm,
        transformed.crtd_dttm,
        transformed.updt_dttm
    from transformed
    left join transformed_2 
    on 
        trim(transformed.sls_org::string) = trim(transformed_2.sls_org::string)
        and ltrim(transformed.material, 0) = ltrim(transformed_2.material, 0)
        and transformed.cond_rec_no = transformed_2.cond_rec_no
        and to_date(transformed.valid_to, 'YYYYMMDD') = to_date(transformed_2.valid_to, 'YYYYMMDD')
        and to_date(transformed.dt_from, 'YYYYMMDD') = to_date(transformed_2.dt_from, 'YYYYMMDD')
)
select * from final


  