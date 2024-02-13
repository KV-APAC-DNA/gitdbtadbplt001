with wks_so_inv_133986 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133986') }}
),
wks_so_inv_133980 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133980') }}
),
wks_so_inv_133981 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133981') }}
),
wks_so_inv_133982 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133982') }}
),
wks_so_inv_133983 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133983') }}
),
wks_so_inv_133984 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133984') }}
),
wks_so_inv_133985 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133985') }}
),
wks_so_inv_131164 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_131164') }}
),
wks_so_inv_131165 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_131165') }}
),
wks_so_inv_131166 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_131166') }}
),
wks_so_inv_131167 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_131167') }}
),
wks_so_inv_130516 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130516') }}
),
wks_so_inv_130517 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130517') }}
),
wks_so_inv_130518 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130518') }}
),
wks_so_inv_130519 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130519') }}
),
wks_so_inv_130520 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130520') }}
),
wks_so_inv_130521 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130521') }}
),
wks_so_inv_119024 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_119024') }}
),
wks_so_inv_119025 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_119025') }}
),
wks_so_inv_108129 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_108129') }}
),
wks_so_inv_108273 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_108273') }}
),
wks_so_inv_108565 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_108565') }}
),
wks_so_inv_118477 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_118477') }}
),
wks_so_inv_109772 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_109772') }}
),
wks_so_inv_135976 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_135976') }}
),
wks_so_inv_137021 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_137021') }}
),
final as (
    select * from wks_so_inv_133986
	union all
	select * from wks_so_inv_133985
	union all
	select * from wks_so_inv_133984
	union all
	select * from wks_so_inv_133983
	union all
	select * from wks_so_inv_133982
	union all
	select * from wks_so_inv_133981
	union all
	select * from wks_so_inv_133980
	union all
	select * from wks_so_inv_131167
	union all
	select * from wks_so_inv_131166
	union all
	select * from wks_so_inv_131165
	union all
	select * from wks_so_inv_131164
	union all
	select * from wks_so_inv_130521
	union all
	select * from wks_so_inv_130520
	union all
	select * from wks_so_inv_130519
	union all
	select * from wks_so_inv_130518
	union all
	select * from wks_so_inv_130517
	union all
	select * from wks_so_inv_130516
	union all
	select * from wks_so_inv_119025
	union all
	select * from wks_so_inv_119024
	union all
	select * from wks_so_inv_118477
	union all
	select * from wks_so_inv_109772
	union all
	select * from wks_so_inv_108565
	union all
	select * from wks_so_inv_108273
	union all
	select * from wks_so_inv_108129
    	union all
	select * from wks_so_inv_135976
	union all
	select * from wks_so_inv_137021
)

select * from final