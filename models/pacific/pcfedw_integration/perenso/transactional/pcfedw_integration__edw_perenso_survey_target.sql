with itg_survey_targets as (
    select * from {{ ref('pcfitg_integration__itg_survey_targets') }}
),
itg_survey_product_grp_to_category_map as (
    select * from {{ ref('pcfitg_integration__itg_survey_product_grp_to_category_map') }}
),
itg_survey_type_to_question_map as (
    select * from {{ ref('pcfitg_integration__itg_survey_type_to_question_map') }}
),    
transformed as (
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
),
final as (
select
    year::varchar(256) as year,
    cycle_start_date::varchar(256) as cycle_start_date,
    cycle_end_date::varchar(256) as cycle_end_date,
    account_type_description::varchar(256) as account_type_description,
    target_type::varchar(256) as target_type,
    category::varchar(256) as category,
    territory_region::varchar(256) as territory_region,
    territory::varchar(256) as territory,
    question_product_group::varchar(256) as question_product_group,
    perenso_questions::varchar(256) as perenso_questions,
    target::number(20,0) as target
from transformed
)
select * from final



