{{
  config(
    alias="itg_crncy_conv",
    tags= ["daily"]
  )
}}
with
source as
(
  select * from {{ source('snapaspitg_integration', 'itg_crncy_conv') }}
),

final as
(
  select
    *
  from source
)
select * from final
