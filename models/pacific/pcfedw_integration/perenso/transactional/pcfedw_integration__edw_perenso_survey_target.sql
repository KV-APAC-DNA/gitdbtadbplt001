with itg_survey_targets as (
    select * from 
),
itg_survey_product_grp_to_category_map as (
    select * from 
),
itg_survey_type_to_question_map as (
       select * from 
),    
final as (
(select ist.year,

       ist.cycle_start_date,

       ist.cycle_end_date,

       ist.account_type_description,

       ist.target_type,

       ist.category,

       ist.territory_region,

       ist.territory,

       isp.question_product_group,

       istm.perenso_questions,

       ist.target

from itg_survey_targets ist,

     itg_survey_product_grp_to_category_map isp,

     itg_survey_type_to_question_map istm

where upper(ist.category) = upper(isp.category(+))

and   ist.category is not null

and   upper(ist.account_type_description) = upper(istm.account_type_description)

and   upper(ist.target_type) = upper(istm.target_type)

union

select ist.year,

       ist.cycle_start_date,

       ist.cycle_end_date,

       ist.account_type_description,

       ist.target_type,

       ist.category,

       ist.territory_region,

       ist.territory,

       null as question_product_group,

       istm.perenso_questions,

       ist.target

from itg_survey_targets ist,

     itg_survey_type_to_question_map istm

where ist.category is null

and   upper(ist.account_type_description) = upper(istm.account_type_description)

and   upper(ist.target_type) = upper(istm.target_type))
)
select * from final



