with 
itg_sfmc_consumer_master as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_SFMC_CONSUMER_MASTER
),
final as (
SELECT 
  derived_table2.subscriber_key, 
  derived_table2.cntry_cd, 
  derived_table2.subscriber_status, 
  derived_table2.file_date, 
  derived_table2.file_year_mnth AS subscriber_status_year_mnth 
FROM 
  (
    SELECT 
      derived_table1.subscriber_key, 
      derived_table1.cntry_cd, 
      derived_table1.subscriber_status, 
      derived_table1.file_date, 
      derived_table1.file_year_mnth, 
      row_number() OVER(
        PARTITION BY derived_table1.subscriber_key, 
        derived_table1.file_year_mnth 
        ORDER BY 
          derived_table1.subscriber_key, 
          derived_table1.file_date DESC
      ) AS rno 
    FROM 
      (
        SELECT 
          itg_sfmc_consumer_master.subscriber_key, 
          itg_sfmc_consumer_master.cntry_cd, 
          itg_sfmc_consumer_master.subscriber_status, 
          "substring"(
            (
              itg_sfmc_consumer_master.file_name
            ):: text, 
            24, 
            8
          ) AS file_date, 
          "substring"(
            (
              itg_sfmc_consumer_master.file_name
            ):: text, 
            24, 
            6
          ) AS file_year_mnth 
        FROM 
          itg_sfmc_consumer_master 
        WHERE 
          (
            (
              (
                itg_sfmc_consumer_master.cntry_cd
              ):: text = ('PH' :: character varying):: text
            ) 
            AND (
              itg_sfmc_consumer_master.subscriber_status IS NOT NULL
            )
          )
      ) derived_table1
  ) derived_table2 
WHERE 
  (derived_table2.rno = 1)
  )
  select * from final 
