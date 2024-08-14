with tbecorderhist
as (
	select *
	from {{ ref('jpndclitg_integration__tbecorderhist') }}
	)

	,transformed
as (
	select max(tbecorderhist.diorderhistid) as diorderhistid
		,tbecorderhist.diorderid
	from tbecorderhist
	where (
			(
				((tbecorderhist.c_dirirekikubun)::text = ('10'::character varying)::text)
				or ((tbecorderhist.c_dirirekikubun)::text = ('20'::character varying)::text)
				)
			and ((tbecorderhist.dielimflg)::text = ((0)::character varying)::text)
			)
	group by tbecorderhist.diorderid
	)
    
select *
from transformed
