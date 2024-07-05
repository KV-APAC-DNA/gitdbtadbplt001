{{
    config
    (
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["salinvdate"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}
with wks_csl_tbl_schemewise_apno as 
(
  select * from dev_dna_core.snapindwks_integration.wks_csl_tbl_schemewise_apno
),
final as 
(
  SELECT schid::number(18, 0) AS schid,
    apno::VARCHAR(200) AS apno,
    createduserid::number(18, 0) AS createduserid,
    createddate::timestamp_ntz(9) AS createddate,
    schcategorytype1code::VARCHAR(50) AS schcategorytype1code,
    schcategorytype2code::VARCHAR(50) AS schcategorytype2code,
    current_timestamp()::timestamp_ntz(9) AS crt_dttm,
    current_timestamp()::timestamp_ntz(9) AS updt_dttm
  FROM wks_csl_tbl_schemewise_apno
)
select * from final
