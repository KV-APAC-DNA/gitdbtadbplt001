with source as
(
    select * from {{ref('thaitg_integration__itg_th_gt_visit')}}
),
final as
(   
     select 
        cntry_cd,
        crncy_cd,
        id_sale as sales_rep_id,
        sale_name as sales_rep_name,
        id_customer as store_id,
        customer_name as store_name,
        date_plan as visit_date_planned,
        time_plan,
        date_visi as actual_date_visited,
        time_visit_in,
        object as "object",
        visit_end,
        time_visit_out,
        regioncode,
        areacode,
        branchcode,
        saleunit as distributor_id,
        time_survey_in,
        time_survey_out,
        count_survey,
        case
            when (left(trim((id_customer)::text), 1) = '_'::text) then 'N'::text
            else 'Y'::text
        end as valid_flag
    from source
)
select * from final