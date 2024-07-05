{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['diid']
    )
}}

with source as
(
    select * from {{ source('jpndclsdl_raw', 'tbperson') }}
),

final as
(
    select 
        diid::number(38,0)  as diid,
		dsname::varchar(48) as dsname,
		dslogin::varchar(48) as dslogin,
		dsmail::varchar(192) as dsmail,
		dspasswd::varchar(96) as dspasswd,
		diauthid::number(38,0) as diauthid,
		diduty::number(38,0) as diduty,
		diflag::number(38,0)  as diflag,
		dsopt::varchar(1536) as dsopt,
		didivid::number(38,0) as didivid,
		diacslimit::varchar(1)  as diacslimit,
		dsprep::varchar(56)   as dsprep,
		dsren::varchar(56)   as dsren,
		dselim::varchar(28) as dselim,
		diprepusr::number(38,0)  as diprepusr,
		direnusr::number(38,0)  as direnusr,
		dielimflg::varchar(1)  as dielimflg,
		diaspusrid::number(38,0)  as diaspusrid,
		direport::number(38,0) as direport,
		c_disaleschannel::number(38,0) as c_disaleschannel,
		c_ditenkandispflg::number(38,0)   as c_ditenkandispflg,
		diapprovalflg::varchar(1) as diapprovalflg,
		dsbirthday::varchar(28) as dsbirthday,
		c_dsprefcd::varchar(3) as c_dsprefcd,
		c_dioperatorrank::number(38,0) as c_dioperatorrank,
		c_dstelcompanycd::varchar(4)  as c_dstelcompanycd,
		c_dsctioperatorkbn::varchar(1)  as c_dsctioperatorkbn,
		c_disalesskill::number(38,0) as c_disalesskill,
		c_diansweringskill::number(38,0) as c_diansweringskill,
		c_dicosmeticsskill::number(38,0) as c_dicosmeticsskill,
		c_difoodsskill::number(38,0) as c_difoodsskill,
		c_dscampaign01flg::varchar(1) as c_dscampaign01flg,
		c_dscampaign02flg::varchar(1) as c_dscampaign02flg,
		c_dscampaign03flg::varchar(1) as c_dscampaign03flg,
		c_dscampaign04flg::varchar(1) as c_dscampaign04flg,
		c_dscampaign05flg::varchar(1) as c_dscampaign05flg,
		c_dscampaign06flg::varchar(1) as c_dscampaign06flg,
		c_dscampaign07flg::varchar(1) as c_dscampaign07flg,
		c_dscampaign08flg::varchar(1) as c_dscampaign08flg,
		c_dscampaign09flg::varchar(1) as c_dscampaign09flg,
		c_dscampaign10flg::varchar(1) as c_dscampaign10flg,
		c_dimedicinesskill::number(38,0) as c_dimedicinesskill,
		c_direserve01skill::number(38,0) as c_direserve01skill,
		c_direserve02skill::number(38,0) as c_direserve02skill,
		c_direserve03skill::number(38,0) as c_direserve03skill,
		null::varchar(10) as source_file_date,
		inserted_date::timestamp_ntz(9)  as inserted_date,
		null::varchar(100) as inserted_by,
		updated_date::timestamp_ntz(9)  as updated_date,
		null::varchar(100) as updated_by
    from source
)

select * from source