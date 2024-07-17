with edw_hcp360_hcp_master_key_by_brand_reject as(
    select * from snapindedw_integration.edw_hcp360_hcp_master_key_by_brand_reject
),
final as(
    SELECT edw_hcp360_hcp_master_key_by_brand_reject.brand,
    edw_hcp360_hcp_master_key_by_brand_reject.master_hcp_key,
    edw_hcp360_hcp_master_key_by_brand_reject.mobile_phone,
    edw_hcp360_hcp_master_key_by_brand_reject.person_email,
    edw_hcp360_hcp_master_key_by_brand_reject.account_source_id,
    edw_hcp360_hcp_master_key_by_brand_reject.ventasys_team_name,
    edw_hcp360_hcp_master_key_by_brand_reject.ventasys_custid,
    edw_hcp360_hcp_master_key_by_brand_reject.subscriber_key
FROM edw_hcp360_hcp_master_key_by_brand_reject
)
select * from final

