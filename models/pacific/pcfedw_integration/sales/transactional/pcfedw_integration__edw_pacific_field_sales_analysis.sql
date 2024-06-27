--REQ000000180060: SSR O&A display points content change
with edw_pacific_field_sales as
(
    select * from {{ ref('pcfedw_integration__edw_pacific_field_sales') }}
),
edw_time_dim as
(
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_perenso_account_dim as
(
    select * from {{ ref('pcfedw_integration__edw_perenso_account_dim') }}
),
itg_perenso_product_group as
(
    select * from {{ ref('pcfitg_integration__itg_perenso_product_group') }}
),
itg_perenso_users as
(
    select * from {{ ref('pcfitg_integration__itg_perenso_users') }}
),
itg_pacific_perenso_call_coverage_targets as
(
    select * from {{ ref('pcfitg_integration__itg_pacific_perenso_call_coverage_targets') }}
),
etdw as 
(
    select 
        etd.jj_year,
        etd.jj_mnth_id,
        etd.jj_wk,
        row_number() over (partition by etd.jj_year,etd.jj_mnth_id order by etd.jj_year,etd.jj_mnth_id,etd.jj_wk) as jj_mnth_wk
    from 
    (
            select 
                distinct etd.jj_year,
                etd.jj_mnth_id,
                etd.jj_wk
            from edw_time_dim etd
    ) etd
),
etdc as 
(
    select 
        etd.*,
        case
            when (
                (row_number () over ( partition by etd.cal_year order by to_date(etd.cal_date)))%7::bigint) = 0 
                then floor((row_number () over (partition by etd.cal_year order by to_date(etd.cal_date)))/7)
                else floor((row_number () over (partition by etd.cal_year order by to_date(etd.cal_date)))/7 + 1)
        end as cal_wk
    from edw_time_dim etd
),
etdcm as (
    select 
        etdcw.cal_year,
        etdcw.cal_mnth_id,
        etdcw.cal_wk,
        row_number() over (partition by etdcw.cal_year,etdcw.cal_mnth_id order by etdcw.cal_year,etdcw.cal_mnth_id,etdcw.cal_wk) as cal_mnth_wk
    from 
    (
        select 
            distinct etdc.cal_year,
            etdc.cal_mnth_id,
            etdc.cal_wk
        from 
        (
            select etd.cal_year,
                etd.cal_mnth_id,
                case
                    when (
                        (row_number () over ( partition by etd.cal_year order by to_date(etd.cal_date)))%7::bigint) = 0 
                        then floor((row_number () over (partition by etd.cal_year order by to_date(etd.cal_date)))/7)
                        else floor((row_number () over (partition by etd.cal_year order by to_date(etd.cal_date)))/7 + 1)
                end as cal_wk
            from edw_time_dim etd
        ) etdc
    ) etdcw
),
etd as
(
    select 
        etd.*,
        etdw.jj_mnth_wk,
        etdc.cal_wk,
        etdcm.cal_mnth_wk
    from edw_time_dim etd,
    etdw,
    etdc,
    etdcm
    where etd.jj_year = etdw.jj_year
        and etd.jj_mnth_id = etdw.jj_mnth_id
        and etd.jj_wk = etdw.jj_wk
        and to_date(etd.cal_date) = to_date(etdc.cal_date)
        and etdc.cal_wk = etdcm.cal_wk
        and etdc.cal_year = etdcm.cal_year
        and etdc.cal_mnth_id = etdcm.cal_mnth_id
),
epad as
(
    select 
        epad.*,
        ippcct.weekly_targets,
        ippcct.monthly_targets,
        ippcct.yearly_targets,
        ippcct.call_per_week
    from edw_perenso_account_dim epad,
        itg_pacific_perenso_call_coverage_targets ippcct
    where upper(epad.acct_type_desc) = upper(ippcct.account_type_description(+))
        and upper(epad.acct_grade) = upper(ippcct.account_grade(+))
) ,
transformed as
(
    select 
        epfs.perenso_source,
        etd.time_id,
        etd.jj_year,
        etd.jj_qrtr,
        etd.jj_mnth,
        etd.jj_wk,
        etd.jj_mnth_wk,
        etd.jj_mnth_id,
        etd.jj_mnth_tot,
        etd.jj_mnth_day,
        etd.jj_mnth_shrt,
        etd.jj_mnth_long,
        etd.cal_year,
        etd.cal_qrtr,
        etd.cal_mnth,
        etd.cal_wk,
        etd.cal_mnth_wk,
        etd.cal_mnth_id,
        etd.cal_mnth_nm,
        epfs.diary_item_type_desc,
        case
            when upper(epfs.diary_item_category) = 'ACTIVITY ON ACCOUNT' then 'Call'
            else epfs.diary_item_category
        end diary_item_category,
        epfs.diary_start_date,
        epfs.diary_start_time,
        epfs.diary_end_date,
        epfs.diary_end_time,
        epfs.diary_complete,
        epfs.count_in_call_rate,
        epfs.todo_type,
        epfs.to_do_option_desc,
        epfs.acct_key,
        epad.acct_display_name,
        epad.acct_type_desc,
        epad.acct_street_1,
        epad.acct_street_2,
        epad.acct_street_3,
        epad.acct_suburb,
        epad.acct_postcode,
        epad.acct_phone_number,
        epad.acct_fax_number,
        epad.acct_email,
        epad.acct_country,
        epad.acct_region,
        epad.acct_state,
        epad.acct_banner_country,
        epad.acct_banner_division,
        epad.acct_banner_type,
        epad.acct_banner,
        epad.acct_type,
        epad.acct_sub_type,
        epad.acct_grade,
        epad.acct_nz_pharma_country,
        epad.acct_nz_pharma_state,
        epad.acct_nz_pharma_territory,
        epad.acct_nz_groc_country,
        epad.acct_nz_groc_state,
        epad.acct_nz_groc_territory,
        epad.acct_ssr_country,
        epad.acct_ssr_state,
        epad.acct_ssr_team_leader,
        epad.acct_ssr_territory,
        epad.acct_ssr_sub_territory,
        epad.acct_ind_groc_country,
        epad.acct_ind_groc_state,
        epad.acct_ind_groc_territory,
        epad.acct_ind_groc_sub_territory,
        epad.acct_au_pharma_country,
        epad.acct_au_pharma_state,
        epad.acct_au_pharma_territory,
        epad.acct_au_pharma_ssr_country,
        epad.acct_au_pharma_ssr_state,
        epad.acct_au_pharma_ssr_territory,
        case
            when upper(epad.acct_ind_groc_state) != 'NOT ASSIGNED' then epad.acct_ind_groc_state
            when upper(epad.acct_au_pharma_state) != 'NOT ASSIGNED' then epad.acct_au_pharma_state
            when upper(epad.acct_nz_pharma_state) != 'NOT ASSIGNED' then epad.acct_nz_pharma_state
            when upper(epad.acct_nz_groc_state) != 'NOT ASSIGNED' then epad.acct_nz_groc_state
            when upper(epad.acct_ssr_team_leader) != 'NOT ASSIGNED' then epad.acct_ssr_team_leader
            else 'NOT ASSIGNED'
        end acct_tsm,
        case
            when upper(epad.acct_ind_groc_territory) != 'NOT ASSIGNED' then epad.acct_ind_groc_territory
            when upper(epad.acct_au_pharma_territory) != 'NOT ASSIGNED' then epad.acct_au_pharma_territory
            when upper(epad.acct_nz_pharma_territory) != 'NOT ASSIGNED' then epad.acct_nz_pharma_territory
            when upper(epad.acct_nz_groc_territory) != 'NOT ASSIGNED' then epad.acct_nz_groc_territory
            when upper(epad.acct_ssr_territory) != 'NOT ASSIGNED' then epad.acct_ssr_territory
            when upper(epad.acct_au_pharma_ssr_territory) != 'NOT ASSIGNED' then epad.acct_au_pharma_ssr_territory
            else 'NOT ASSIGNED'
        end acct_terriroty,
        epad.acct_store_code as acct_store_code,
        epad.acct_fax_opt_out,
        epad.acct_email_opt_out,
        epad.acct_contact_method,
        epad.ssr_grade,
        epfs.create_user_key,
        ipu.display_name as create_user_name,
        ipu.user_type_desc as create_user_desc,
        ipu.email_address as create_user_email_address,
        epfs.store_chk_date,
        epfs.prod_grp_key,
        ippg.prod_grp_desc,
        epfs.todo_desc,
        epfs.to_do_start_date,
        epfs.to_do_end_date,
        epfs.work_item_desc,
        case
            when upper(epfs.work_item_type) = 'CHECK THE STORE' THEN 'Walk The Store'
            else epfs.work_item_type
        end work_item_type,
        epfs.work_item_start_date,
        epfs.work_item_end_date,
        epfs.survery_notes,
        epfs.fail_reason_desc,
        epfs.req_start_date,
        epfs.req_end_date,
        epfs.oa_acct_key,
        epfs.oa_start_date,
        epfs.oa_end_date,
        epfs.oa_points,
        epfs.batch_count,
        epfs.activated,
        epfs.over_and_above_notes,
        current_timestamp as load_date,
        epfs.question_response,
        epfs.dash_start_time,
        epfs.dash_end_time,
        epfs.cycle_start_date,
        epfs.cycle_end_date,
        epfs.cycle_number,
        epfs.parent_question,
        epfs.question_order,
        epfs.survey_target_type,
        epfs.survey_target,
        survey_category,
        case
            when upper(perenso_source) = 'CALL' then epad.weekly_targets
            else null
        end call_weekly_targets,
        case
            when upper(perenso_source) = 'CALL' then epad.monthly_targets /(
                max(jj_mnth_wk) over (partition by etd.jj_year, etd.jj_mnth_id)
            )::double precision
            else null
        end call_monthly_targets,
        case
            when upper(perenso_source) = 'CALL' then epad.yearly_targets
            else null
        end call_yearly_targets,
        case
            when upper(perenso_source) = 'CALL' then epad.call_per_week
            else null
        end CALLs_per_week,
        call_description,
        assigned_user_key,
        assigned_user_name,
        due_dt,
        completed_dt,
        status,
        diary_session_start_time,
        diary_session_end_time,
        duration,
        latitude,
        longitude
    from edw_pacific_field_sales epfs,
        etd,
        epad,
        itg_perenso_users ipu,
        itg_perenso_product_group ippg
    where to_date(epfs.diary_start_date) = to_date(etd.cal_date)
        and epfs.acct_key = epad.acct_id(+)
        and epfs.create_user_key = ipu.user_key(+)
        and epfs.prod_grp_key = ippg.prod_grp_key(+)
),
final as
(
    select 
        perenso_source::varchar(20) as perenso_source,
        time_id::number(18,0) as time_id,
        jj_year::number(18,0) as jj_year,
        jj_qrtr::number(18,0) as jj_qrtr,
        jj_mnth::number(18,0) as jj_mnth,
        jj_wk::number(18,0) as jj_wk,
        jj_mnth_wk::number(38,0) as jj_mnth_wk,
        jj_mnth_id::number(18,0) as jj_mnth_id,
        jj_mnth_tot::number(18,0) as jj_mnth_tot,
        jj_mnth_day::number(18,0) as jj_mnth_day,
        jj_mnth_shrt::varchar(3) as jj_mnth_shrt,
        jj_mnth_long::varchar(10) as jj_mnth_long,
        cal_year::number(18,0) as cal_year,
        cal_qrtr::number(18,0) as cal_qrtr,
        cal_mnth::number(18,0) as cal_mnth,
        cal_wk::number(38,0) as cal_wk,
        cal_mnth_wk::number(38,0) as cal_mnth_wk,
        cal_mnth_id::number(18,0) as cal_mnth_id,
        cal_mnth_nm::varchar(10) as cal_mnth_nm,
        diary_item_type_desc::varchar(255) as diary_item_type_desc,
        diary_item_category::varchar(255) as diary_item_category,
        diary_start_date::date as diary_start_date,
        diary_start_time::varchar(18) as diary_start_time,
        diary_end_date::date as diary_end_date,
        diary_end_time::varchar(18) as diary_end_time,
        diary_complete::varchar(5) as diary_complete,
        count_in_call_rate::varchar(5) as count_in_call_rate,
        todo_type::varchar(20) as todo_type,
        to_do_option_desc::varchar(256) as to_do_option_desc,
        acct_key::number(10,0) as acct_key,
        acct_display_name::varchar(256) as acct_display_name,
        acct_type_desc::varchar(50) as acct_type_desc,
        acct_street_1::varchar(256) as acct_street_1,
        acct_street_2::varchar(256) as acct_street_2,
        acct_street_3::varchar(256) as acct_street_3,
        acct_suburb::varchar(255) as acct_suburb,
        acct_postcode::varchar(255) as acct_postcode,
        acct_phone_number::varchar(255) as acct_phone_number,
        acct_fax_number::varchar(50) as acct_fax_number,
        acct_email::varchar(256) as acct_email,
        acct_country::varchar(256) as acct_country,
        acct_region::varchar(256) as acct_region,
        acct_state::varchar(256) as acct_state,
        acct_banner_country::varchar(256) as acct_banner_country,
        acct_banner_division::varchar(256) as acct_banner_division,
        acct_banner_type::varchar(256) as acct_banner_type,
        acct_banner::varchar(256) as acct_banner,
        acct_type::varchar(256) as acct_type,
        acct_sub_type::varchar(256) as acct_sub_type,
        acct_grade::varchar(256) as acct_grade,
        acct_nz_pharma_country::varchar(256) as acct_nz_pharma_country,
        acct_nz_pharma_state::varchar(256) as acct_nz_pharma_state,
        acct_nz_pharma_territory::varchar(256) as acct_nz_pharma_territory,
        acct_nz_groc_country::varchar(256) as acct_nz_groc_country,
        acct_nz_groc_state::varchar(256) as acct_nz_groc_state,
        acct_nz_groc_territory::varchar(256) as acct_nz_groc_territory,
        acct_ssr_country::varchar(256) as acct_ssr_country,
        acct_ssr_state::varchar(256) as acct_ssr_state,
        acct_ssr_team_leader::varchar(256) as acct_ssr_team_leader,
        acct_ssr_territory::varchar(256) as acct_ssr_territory,
        acct_ssr_sub_territory::varchar(256) as acct_ssr_sub_territory,
        acct_ind_groc_country::varchar(256) as acct_ind_groc_country,
        acct_ind_groc_state::varchar(256) as acct_ind_groc_state,
        acct_ind_groc_territory::varchar(256) as acct_ind_groc_territory,
        acct_ind_groc_sub_territory::varchar(256) as acct_ind_groc_sub_territory,
        acct_au_pharma_country::varchar(256) as acct_au_pharma_country,
        acct_au_pharma_state::varchar(256) as acct_au_pharma_state,
        acct_au_pharma_territory::varchar(256) as acct_au_pharma_territory,
        acct_au_pharma_ssr_country::varchar(256) as acct_au_pharma_ssr_country,
        acct_au_pharma_ssr_state::varchar(256) as acct_au_pharma_ssr_state,
        acct_au_pharma_ssr_territory::varchar(256) as acct_au_pharma_ssr_territory,
        acct_tsm::varchar(256) as acct_tsm,
        acct_terriroty::varchar(256) as acct_terriroty,
        acct_store_code::varchar(256) as acct_store_code,
        acct_fax_opt_out::varchar(256) as acct_fax_opt_out,
        acct_email_opt_out::varchar(256) as acct_email_opt_out,
        acct_contact_method::varchar(256) as acct_contact_method,
        ssr_grade::varchar(256) as ssr_grade,
        create_user_key::number(10,0) as create_user_key,
        create_user_name::varchar(100) as create_user_name,
        create_user_desc::varchar(100) as create_user_desc,
        create_user_email_address::varchar(100) as create_user_email_address,
        store_chk_date::timestamp_ntz(9) as store_chk_date,
        prod_grp_key::number(18,0) as prod_grp_key,
        prod_grp_desc::varchar(100) as prod_grp_desc,
        todo_desc::varchar(256) as todo_desc,
        to_do_start_date::timestamp_ntz(9) as to_do_start_date,
        to_do_end_date::timestamp_ntz(9) as to_do_end_date,
        work_item_desc::varchar(255) as work_item_desc,
        work_item_type::varchar(255) as work_item_type,
        work_item_start_date::timestamp_ntz(9) as work_item_start_date,
        work_item_end_date::timestamp_ntz(9) as work_item_end_date,
        survery_notes::varchar(16777216) as survery_notes,
        fail_reason_desc::varchar(256) as fail_reason_desc,
        req_start_date::timestamp_ntz(9) as req_start_date,
        req_end_date::timestamp_ntz(9) as req_end_date,
        oa_acct_key::number(18,0) as oa_acct_key,
        oa_start_date::timestamp_ntz(9) as oa_start_date,
        oa_end_date::timestamp_ntz(9) as oa_end_date,
        oa_points::number(21,3) as oa_points,
        batch_count::number(18,0) as batch_count,
        activated::varchar(10) as activated,
        over_and_above_notes::varchar(255) as over_and_above_notes,
        load_date::date as load_date,
        question_response::varchar(16777216) as question_response,
        dash_start_time::date as dash_start_time,
        dash_end_time::date as dash_end_time,
        cycle_start_date::date as cycle_start_date,
        cycle_end_date::date as cycle_end_date,
        cycle_number::varchar(25) as cycle_number,
        parent_question::varchar(256) as parent_question,
        question_order::number(18,0) as question_order,
        survey_target_type::varchar(25) as survey_target_type,
        survey_target::number(18,0) as survey_target,
        survey_category::varchar(100) as survey_category,
        call_weekly_targets::number(10,0) as call_weekly_targets,
        call_monthly_targets::number(30,20) as call_monthly_targets,
        call_yearly_targets::number(10,0) as call_yearly_targets,
        calls_per_week::number(10,5) as calls_per_week,
        call_description::varchar(255) as call_description,
        assigned_user_key::varchar(50) as assigned_user_key,
        assigned_user_name::varchar(100) as assigned_user_name,
        due_dt::date as due_dt,
        completed_dt::date as completed_dt,
        status::varchar(50) as status,
        diary_session_start_time::timestamp_ntz(9) as diary_session_start_time,
        diary_session_end_time::timestamp_ntz(9) as diary_session_end_time,
        duration::varchar(50) as duration,
        latitude::varchar(50) as latitude,
        longitude::varchar(50) as longitude
    from transformed
)
select * from final
