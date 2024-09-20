with kesaiid_dummy_tbl as(
    select * from {{ ref('jpndcledw_integration__kesaiid_dummy_tbl') }}
),
final as(
    SELECT kesaiid_dummy_tbl.c_dikesaiid_before
	,kesaiid_dummy_tbl.c_dikesaiid_after
	,kesaiid_dummy_tbl.diinquireid
	,kesaiid_dummy_tbl.c_diinquirekesaiid
    FROM kesaiid_dummy_tbl
)
select * from final