{{
    config(
        materialized="incremental",
        incremental_strategy='append',
        pre_hook="{% if var('invnt_job_to_execute') == 'tw_ims_distributor_standard_stock' %}
            {% if is_incremental() %}
            DELETE
            FROM {{this}} edw_ims_inventory_fact USING (SELECT DISTINCT dstr_cd FROM {{ ref('ntawks_integration__wks_edw_ims_invnt_std') }} ) WKS_EDW_IMS_INVNT_STD
            WHERE edw_ims_inventory_fact.dstr_cd = WKS_EDW_IMS_INVNT_STD.dstr_cd;
            {% endif %}
            {% elif var('invnt_job_to_execute') == 'edw_ims_inventory_fact_load' %}
            {% if is_incremental() %}
            delete from {{this}} edw_ims_inventory_fact using (select distinct dstr_cd from {{ ref('ntawks_integration__wks_edw_ims_invnt') }}) wks_edw_ims_invnt
            where edw_ims_inventory_fact.dstr_cd = wks_edw_ims_invnt.dstr_cd;
            {% endif %}
            {% endif %}
            "
    )
}}


with 
source as(
    select * from {{ ref('ntawks_integration__wks_edw_ims_invnt_std') }}
),
wks_edw_ims_invnt as (
    select * from {{ ref('ntawks_integration__wks_edw_ims_invnt') }}
)
{% if var('invnt_job_to_execute') == 'tw_ims_distributor_standard_stock' %}
,
taiwan as(
    select 
        to_date(invnt_dt) as invnt_dt,
        dstr_cd::varchar(30) as dstr_cd,
        dstr_nm::varchar(100) as dstr_nm,
        prod_cd::varchar(20) as prod_cd,
        prod_nm::varchar(200) as prod_nm,
        ean_num::varchar(20) as ean_num,
        cust_nm::varchar(100) as cust_nm,
        invnt_qty::number(21,5) as invnt_qty,
        invnt_amt::number(21,5) as invnt_amt,
        avg_prc_amt::number(21,5) as avg_prc_amt,
        safety_stock::number(21,5) as safety_stock,
        bad_invnt_qty::number(21,5) as bad_invnt_qty,
        book_invnt_qty::number(21,5) as book_invnt_qty,
        convs_amt::number(21,5) as convs_amt,
        prch_disc_amt::number(21,5) as prch_disc_amt,
        end_invnt_qty::number(21,5) as end_invnt_qty,
        batch_no::varchar(20) as batch_no,
        uom::varchar(20) as uom,
        sls_rep_cd::varchar(20) as sls_rep_cd,
        sls_rep_nm::varchar(50) as sls_rep_nm,
        ctry_cd::varchar(2) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        chn_uom::varchar(100) as chn_uom,
        storage_name::varchar(50) as storage_name,
        area::varchar(50) as area
    from source
)
select * from taiwan
{% elif var('invnt_job_to_execute') == 'edw_ims_inventory_fact_load' %}
,

hk as (
select
to_date(invnt_dt) as invnt_dt,
dstr_cd :: varchar(30) as dstr_cd,
dstr_nm :: varchar(100) as dstr_nm,
prod_cd :: varchar(20) as prod_cd,
prod_nm :: varchar(200) as prod_nm,
ean_num :: varchar(20) as ean_num,
cust_nm :: varchar(100) as cust_nm,
invnt_qty :: number(21, 5) as invnt_qty,
invnt_amt :: number(21, 5) as invnt_amt,
avg_prc_amt :: number(21, 5) as avg_prc_amt,
safety_stock :: number(21, 5) as safety_stock,
bad_invnt_qty :: number(21, 5) as bad_invnt_qty,
book_invnt_qty :: number(21, 5) as book_invnt_qty,
convs_amt :: number(21, 5) as convs_amt,
prch_disc_amt :: number(21, 5) as prch_disc_amt,
end_invnt_qty :: number(21, 5) as end_invnt_qty,
batch_no :: varchar(20) as batch_no,
uom :: varchar(20) as uom,
sls_rep_cd :: varchar(20) as sls_rep_cd,
sls_rep_nm :: varchar(50) as sls_rep_nm,
ctry_cd :: varchar(2) as ctry_cd,
crncy_cd :: varchar(3) as crncy_cd,
crt_dttm :: timestamp_ntz(9) as crt_dttm,
current_timestamp() :: timestamp_ntz(9) as updt_dttm,
chn_uom :: varchar(100) as chn_uom,
null::varchar(50) as storage_name,
null::varchar(50) as area
from wks_edw_ims_invnt
)
select * from hk
{% endif %}





