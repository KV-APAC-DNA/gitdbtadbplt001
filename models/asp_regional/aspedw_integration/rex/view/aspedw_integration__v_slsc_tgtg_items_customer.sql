with v_slsc_salescampaign as 
(
    select * from {{ ref('aspitg_integration__v_slsc_salescampaign') }}
),
v_slsc_targetgroups as
(
    select * from {{ ref('aspitg_integration__v_slsc_targetgroups') }}
),
v_tgtg_targetgroup as 
(
    select * from {{ ref('aspitg_integration__v_tgtg_targetgroup') }}
),
v_tgtg_items as 
(
    select * from {{ ref('aspitg_integration__v_tgtg_items') }}
),
slsc as 
(
    SELECT 
        salescampaignid,
        salescampaignname,
        salescampaigndescription,
        startdate,
        enddate
    FROM v_slsc_salescampaign
    WHERE rank = 1
),
slsc_tgtg as
(
    SELECT 
        salescampaignid,
        targetgroupid
    FROM v_slsc_targetgroups
    WHERE rank = 1
),
tgtg as 
(
    SELECT 
        targetgroupid,
        target as "target"
    FROM v_tgtg_targetgroup
    WHERE (rank = 1) AND  ("target")::text = ('customer'::character varying)::text
),
tgtg_items as 
(
    SELECT v_tgtg_items.targetgroupid,
        v_tgtg_items.itemid
    FROM v_tgtg_items
    WHERE (v_tgtg_items.rank = 1)
),
final as 
(
    SELECT 
        DISTINCT slsc.salescampaignid,
        slsc.salescampaignname,
        slsc.salescampaigndescription,
        slsc.startdate AS salescampaignstartdt,
        slsc.enddate AS salescampaignenddt,
        tgtg.targetgroupid,
        tgtg."target",
        tgtg_items.itemid AS customerid
    FROM slsc
    LEFT JOIN slsc_tgtg ON (slsc.salescampaignid)::text = (slsc_tgtg.salescampaignid)::text
    LEFT JOIN tgtg ON (slsc_tgtg.targetgroupid)::text = (tgtg.targetgroupid)::text
    LEFT JOIN tgtg_items ON (tgtg.targetgroupid)::text = (tgtg_items.targetgroupid)::text
)
select * from final