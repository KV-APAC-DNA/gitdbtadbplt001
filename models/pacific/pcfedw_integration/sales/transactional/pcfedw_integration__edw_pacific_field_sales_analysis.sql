with edw_pacific_field_sales as
(
    select * from snappcfedw_integration.edw_pacific_field_sales
),
edw_time_dim as
(
    select * from snappcfedw_integration.edw_time_dim
),
edw_perenso_account_dim as
(
    select * from snappcfedw_integration.edw_perenso_account_dim
),
itg_perenso_product_group as
(
    select * from snappcfitg_integration.itg_perenso_product_group
),
itg_perenso_users as
(
    select * from snappcfitg_integration.itg_perenso_users
),
itg_pacific_perenso_call_coverage_targets as
(
    select * from snappcfitg_integration.itg_pacific_perenso_call_coverage_targets
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
                then (row_number () over (partition by etd.cal_year order by to_date(etd.cal_date)))/7
                else (row_number () over (partition by etd.cal_year order by to_date(etd.cal_date)))/7 + 1
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
                    when ((row_number () over (partition by etd.cal_year order by to_date(etd.cal_date)))%7::bigint) = 0 
                    then (row_number () over (partition by etd.cal_year order by to_date(etd.cal_date)))/7
                    else (row_number () over (partition by etd.cal_year order by to_date(etd.cal_date)))/7 + 1
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
final as
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
            when upper(epfs.diary_item_category) = 'activity on account' then 'call'
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
            when upper(epad.acct_ind_groc_state) != 'not assigned' then epad.acct_ind_groc_state
            when upper(epad.acct_au_pharma_state) != 'not assigned' then epad.acct_au_pharma_state
            when upper(epad.acct_nz_pharma_state) != 'not assigned' then epad.acct_nz_pharma_state
            when upper(epad.acct_nz_groc_state) != 'not assigned' then epad.acct_nz_groc_state
            when upper(epad.acct_ssr_team_leader) != 'not assigned' then epad.acct_ssr_team_leader
            else 'not assigned'
        end acct_tsm,
        case
            when upper(epad.acct_ind_groc_territory) != 'not assigned' then epad.acct_ind_groc_territory
            when upper(epad.acct_au_pharma_territory) != 'not assigned' then epad.acct_au_pharma_territory
            when upper(epad.acct_nz_pharma_territory) != 'not assigned' then epad.acct_nz_pharma_territory
            when upper(epad.acct_nz_groc_territory) != 'not assigned' then epad.acct_nz_groc_territory
            when upper(epad.acct_ssr_territory) != 'not assigned' then epad.acct_ssr_territory
            when upper(epad.acct_au_pharma_ssr_territory) != 'not assigned' then epad.acct_au_pharma_ssr_territory
            else 'not assigned'
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
            when upper(epfs.work_item_type) = 'check the store' then 'walk the store'
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
            when upper(perenso_source) = 'call' then epad.weekly_targets
            else null
        end call_weekly_targets,
        case
            when upper(perenso_source) = 'call' then epad.monthly_targets /(
                max(jj_mnth_wk) over (partition by etd.jj_year, etd.jj_mnth_id)
            )
            else null
        end call_monthly_targets,
        case
            when upper(perenso_source) = 'call' then epad.yearly_targets
            else null
        end call_yearly_targets,
        case
            when upper(perenso_source) = 'call' then epad.call_per_week
            else null
        end calls_per_week,
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
)
select count(*),'model' from final
union all
select count(*),'snap' from snappcfedw_integration.edw_pacific_field_sales_analysis