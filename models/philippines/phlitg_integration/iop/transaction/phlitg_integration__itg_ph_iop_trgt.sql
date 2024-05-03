
{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "delete from {{this}} where left(jj_mnth_id,4) in (select distinct year 
					   from {{ source('phlsdl_raw', 'sdl_ph_iop_trgt') }} );"
    )
}}
with sdl_ph_iop_trgt as (
select * from {{ source('phlsdl_raw', 'sdl_ph_iop_trgt') }}
),

transformed as (
select CAST(JJ_MNTH_ID AS INTEGER) as JJ_MNTH_ID,
       CAST(CUST_ID AS INTEGER) as CUST_ID,
       TRGT_TYPE,
       BRND_CD,
       TP_TRGT_AMT,
       FILENAME,
       null as CDL_DTTM,
       convert_timezone('SGT',current_timestamp()) AS crtd_dttm,
       NULL AS UPDT_DTTM,
	   segment,
	   GTS_TRGT_AMT
 from (
 select TRGT_TYPE,year,BRND_CD,segment,cust_id,account,jj_mnth_id,sum(GTS_TRGT_AMT)as GTS_TRGT_AMT,sum(TP_TRGT_AMT) as TP_TRGT_AMT,filename from (
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'Jan' as month,concat(year,'01') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(jan AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(jan AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT 
union all
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'Feb' as month,concat(year,'02') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(feb AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(feb AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT 
union all
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'Mar' as month,concat(year,'03') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(mar AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(mar AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT 
union all
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'Apr' as month,concat(year,'04') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(apr AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(apr AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT 
union all
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'May' as month,concat(year,'05') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(may AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(may AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT
union all
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'Jun' as month,concat(year,'06') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(jun AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(jun AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT 
union all
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'Jul' as month,concat(year,'07') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(jul AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(jul AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT
union all
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'Aug' as month,concat(year,'08') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(aug AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(aug AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT 
union all
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'Sep' as month,concat(year,'09') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(sep AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(sep AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT
union all
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'Oct' as month,concat(year,'10') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(oct AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(oct AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT
union all
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'Nov' as month,concat(year,'11') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(nov AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(nov AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT
union all
SELECT  measure,target_type as TRGT_TYPE,year,brand as BRND_CD,segment,customer_code as cust_id,account,'Dec' as month,concat(year,'12') as jj_mnth_id,
case when measure='GTS ex-Returns' then cast(dec AS NUMERIC(15,4)) end as GTS_TRGT_AMT,case when measure='Pre-WW7' then cast(dec AS NUMERIC(15,4)) end as TP_TRGT_AMT ,filename FROM SDL_PH_IOP_TRGT
) abc
 group by TRGT_TYPE,year,BRND_CD,segment,cust_id,account,jj_mnth_id,filename )
),
final as (
select 
jj_mnth_id::number(18,0) as jj_mnth_id,
cust_id::number(18,0) as cust_id,
trgt_type::varchar(10) as trgt_type,
brnd_cd::varchar(100) as brnd_cd,
tp_trgt_amt::number(15,4) as tp_trgt_amt,
filename::varchar(100) as filename,
cdl_dttm::varchar(50) as cdl_dttm,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm,
segment::varchar(50) as segment,
gts_trgt_amt::number(15,4) as gts_trgt_amt
from transformed
)
select * from final 