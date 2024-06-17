with source as
(
    select * from {{source('idnsdl_raw','sdl_mds_id_lav_mcs_list')}}
)

select id as "id",
muid as "muid",
versionname as "versionname",
versionnumber as "versionnumber",
version_id as "version_id",
versionflag as "versionflag",
name as "name",
code as "code",
changetrackingmask as "changetrackingmask",
tiering as "tiering",
sku as "sku",
year as "year",
january as "january",
february as "february",
march as "march",
april as "april",
may as "may",
june as "june",
july as "july",
august as "august",
september as "september",
october as "october",
november as "november",
december as "december",
enterdatetime as "enterdatetime",
enterusername as "enterusername",
enterversionnumber as "enterversionnumber",
lastchgdatetime as "lastchgdatetime",
lastchgusername as "lastchgusername",
lastchgversionnumber as "lastchgversionnumber",
validationstatus as "validationstatus",
rownum as "rownum"
from source