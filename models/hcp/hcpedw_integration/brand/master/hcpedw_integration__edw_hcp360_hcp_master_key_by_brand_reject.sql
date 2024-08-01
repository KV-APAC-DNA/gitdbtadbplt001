with source as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_hcp_master_key_by_brand') }}
),
final as
(
    SELECT brand,
       master_hcp_key,
       mobile_phone,
       person_email,
       account_source_id,
       ventasys_team_name,
       ventasys_custid,
       subscriber_key
    FROM (SELECT case when brand = 'ORSL' then 'ORSL'
                  when brand = 'JB' then 'JBABY'
                  when brand = 'DERMA' then 'DERMA'
                  when brand not in ('ORSL','JB','DERMA') then null
             end as brand,
             master_hcp_key,
             mobile_phone,
             person_email,
             account_source_id,
             ventasys_team_name,
             ventasys_custid,
             subscriber_key
      FROM source
      WHERE ventasys_custid IN (SELECT ventasys_custid
                                FROM source
                                WHERE ventasys_custid IS NOT NULL
                                GROUP BY 1
                                HAVING COUNT(*) > 1)
      UNION
      SELECT case when brand = 'ORSL' then 'ORSL'
                  when brand = 'JB' then 'JBABY'
                  when brand = 'DERMA' then 'DERMA'
                  when brand not in ('ORSL','JB','DERMA') then null
             end as brand,
             master_hcp_key,
             mobile_phone,
             person_email,
             account_source_id,
             ventasys_team_name,
             ventasys_custid,
             subscriber_key
      FROM source
      WHERE account_source_id IN (SELECT account_source_id
                                  FROM source
                                  WHERE account_source_id IS NOT NULL
                                  GROUP BY 1
                                  HAVING COUNT(*) > 1)
      UNION
      SELECT case when brand = 'ORSL' then 'ORSL'
                  when brand = 'JB' then 'JBABY'
                  when brand = 'DERMA' then 'DERMA'
                  when brand not in ('ORSL','JB','DERMA') then null
             end as brand,
             master_hcp_key,
             mobile_phone,
             person_email,
             account_source_id,
             ventasys_team_name,
             ventasys_custid,
             subscriber_key
      FROM source
      WHERE subscriber_key IN (SELECT subscriber_key
                               FROM source
                               WHERE subscriber_key IS NOT NULL
                               GROUP BY 1
                               HAVING COUNT(*) > 1))
)
select brand::varchar(20) as brand,
    master_hcp_key::varchar(32) as master_hcp_key,
    mobile_phone::varchar(40) as mobile_phone,
    person_email::varchar(180) as person_email,
    account_source_id::varchar(18) as account_source_id,
    ventasys_team_name::varchar(20) as ventasys_team_name,
    ventasys_custid::varchar(20) as ventasys_custid,
    subscriber_key::varchar(50) as subscriber_key
from final