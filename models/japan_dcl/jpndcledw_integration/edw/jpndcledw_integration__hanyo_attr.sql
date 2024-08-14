
{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        post_hook = "{{macro_update_hanyo_attrandtm06dmout()}}"
    )
}}

with cir07dmprn as (
    select * from  {{ ref('jpndcledw_integration__cir07dmprn') }}
),
tm06dmout as (
    select * from  {{ ref('jpndcledw_integration__tm06dmout') }}
),
hanyo_work_temp as (
select 'DMDAIKUBUN' as kbnmei,
      dm.dmprnno as attr1,
      substring(dm.comment1, 1, 40) as attr2,
      '01' as attr3,
      '会報誌' as attr4,
      null as attr5
from cir07dmprn dm
where dm.comment1 like '%clﾌﾟﾚﾐｱﾑ%'
      or dm.comment1 like '%ｼｰﾗﾊﾞｰ%'
),
cte1 as (
select *
from {{this}} hanyo_attr where not exists (
    select 1
    from hanyo_work_temp
    where hanyo_attr.kbnmei = hanyo_work_temp.kbnmei
          and hanyo_attr.attr1 = hanyo_work_temp.attr1
)) ,
insert_1 as  (
    select * from cte1 
    union all
    select kbnmei,
      attr1,
      attr2,
      attr3,
      attr4,
      attr5,
      sysdate(),
    null as updatedate,
    null as inserted_date,
    null as inserted_by,
    null as updated_date,
    null as updated_by,
    null as source_file_date
from hanyo_work_temp
),
tm06_next as (
      select cast(max(lpad(dmchubuncode, 3, '0')) || 1 as varchar) as dmchubuncode
      from tm06dmout tm06dmout
      where dmdaibunname = '会報誌'
      ) ,
hanyo_work_temp_2 as (
select 'DMCHUKUBUN' as kbnmei,
      dm.dmprnno as attr1,
      substring(dm.comment1, 1, 40) as attr2,
      tm06_next.dmchubuncode as attr3,
      case 
            when dm.comment1 like '%clﾌﾟﾚﾐｱﾑ%'
                  then replace(regexp_substr(dm.comment1, 'clﾌﾟﾚﾐｱﾑ.*号'), 'clﾌﾟﾚﾐｱﾑ', '')
            when dm.comment1 like '%ｼｰﾗﾊﾞｰ%'
                  then replace(regexp_substr(dm.comment1, 'ｼｰﾗﾊﾞｰ.*号'), 'ｼｰﾗﾊﾞｰ', '')
            end as attr4,
      null as attr5
from cir07dmprn dm
inner join  tm06_next on 1 = 1
where dm.comment1 like '%clﾌﾟﾚﾐｱﾑ%'
      or dm.comment1 like '%ｼｰﾗﾊﾞｰ%'
),
cte2 as (
    select hanyo_attr.* 
from insert_1 hanyo_attr where not exists (
    select 1
    from hanyo_work_temp_2
    where hanyo_attr.kbnmei = hanyo_work_temp_2.kbnmei
          and hanyo_attr.attr1 = hanyo_work_temp_2.attr1
)
) ,
insert_2 as (
select * from cte2
union all
select kbnmei,
      attr1,
      attr2,
      attr3,
      attr4,
      attr5,
      sysdate(),
    null as updatedate,
    null as inserted_date,
    null as inserted_by,
    null as updated_date,
    null as updated_by,
    null as source_file_date
from hanyo_work_temp_2
),
hanyo_work_temp_3 as (
select distinct 'DMSYOKUBUN' as kbnmei,
      dm.dmprnno as attr1,
      substring(dm.comment1, 1, 40) as attr2,
      case 
            when dm.comment1 like '%clﾌﾟﾚﾐｱﾑ%'
                  then '0003'
            when dm.comment1 like '%ｼｰﾗﾊﾞｰ%'
                  then '0001'
            end as attr3,
      case 
            when dm.comment1 like '%clﾌﾟﾚﾐｱﾑ%'
                  then regexp_substr(dm.comment1, 'clﾌﾟﾚﾐｱﾑ.*号')
            when dm.comment1 like '%ｼｰﾗﾊﾞｰ%'
                  then regexp_substr(dm.comment1, 'ｼｰﾗﾊﾞｰ.*号')
            end as attr4,
      null as attr5
from cir07dmprn dm
where dm.comment1 like '%clﾌﾟﾚﾐｱﾑ%'
      or dm.comment1 like '%ｼｰﾗﾊﾞｰ%'
),
cte_3 as (
select hanyo_attr.*  
from insert_2 hanyo_attr where not exists (
    select 1
    from hanyo_work_temp_3
    where hanyo_attr.kbnmei = hanyo_work_temp_3.kbnmei
          and hanyo_attr.attr1 = hanyo_work_temp_3.attr1
)
),
insert_3 as (
select * from cte_3
union all
select kbnmei,
      attr1,
      attr2,
      attr3,
      attr4,
      attr5,
      sysdate() as insertdate,
    null as updatedate,
    null as inserted_date,
    null as inserted_by,
    null as updated_date,
    null as updated_by,
    null as source_file_date
from hanyo_work_temp_3
),
final as (
select 
    KBNMEI::VARCHAR(45)  as KBNMEI,
	ATTR1::VARCHAR(60) as ATTR1,
	ATTR2::VARCHAR(160) as ATTR2,
	ATTR3::VARCHAR(60) as ATTR3,
	ATTR4::VARCHAR(60) as ATTR4,
	ATTR5::VARCHAR(60) as ATTR5,
	INSERTDATE::TIMESTAMP_NTZ(9) as INSERTDATE,
	UPDATEDATE::TIMESTAMP_NTZ(9)  as UPDATEDATE,
	INSERTED_DATE::TIMESTAMP_NTZ(9) as INSERTED_DATE,
	'ETL_Batch'::VARCHAR(100) as inserted_by,
	UPDATED_DATE::TIMESTAMP_NTZ(9) as UPDATED_DATE,
	UPDATED_BY::VARCHAR(100) as UPDATED_BY,
	SOURCE_FILE_DATE::VARCHAR(10) as SOURCE_FILE_DATE
from insert_3
)
select * from final