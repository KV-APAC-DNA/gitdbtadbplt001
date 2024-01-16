
--Import CTE
with edw_crncy_exch as (
    select * from {{ ref('aspedw_integration__edw_crncy_exch') }}
),

edw_calendar_dim as (
   select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
--Logical CTE

-- Final CTE
final as (
(
	select derived_table1.ex_rt_typ
	,derived_table1.from_crncy
	,derived_table1.to_crncy
	,derived_table1.vld_from
	,derived_table1.ex_rt
	,derived_table1.fisc_per from (
	select a.ex_rt_typ
		,a.from_crncy
		,a.to_crncy
		,calmonthstartdate.vld_from
		,a.ex_rt
		,calmonthstartdate.fisc_per
		,rank() over (
			partition by a.from_crncy
			,a.to_crncy
			,calmonthstartdate.fisc_per order by calmonthstartdate.vld_from desc
			) as latest_ex_rt_by_fisc_per
	FROM 
		(
			select drvd_crncy.ex_rt_typ
				,drvd_crncy.from_crncy
				,drvd_crncy.to_crncy
				,cal.fisc_yr as "year"
				,max(cast((
							case 
								when (
										(cast((drvd_crncy.to_crncy) as text) = cast((cast('USD' as varchar)) as text))
										and (
											(
												(cast((drvd_crncy.from_crncy) as text) = cast((cast('IDR' as varchar)) as text))
												or (cast((drvd_crncy.from_crncy) as text) = cast((cast('KRW' as varchar)) as text))
												)
											or (cast((drvd_crncy.from_crncy) as text) = cast((cast('VND' as varchar)) as text))
											)
										)
									then (drvd_crncy.ex_rt / cast((cast((1000) as decimal)) as decimal(18, 0)))
								when (
										(cast((drvd_crncy.to_crncy) as text) = cast((cast('USD' as varchar)) as text))
										and (cast((drvd_crncy.from_crncy) as text) = cast((cast('JPY' as varchar)) as text))
										)
									then (drvd_crncy.ex_rt / cast((cast((100) as decimal)) as decimal(18, 0)))
								else drvd_crncy.ex_rt
								end
							) as decimal(20, 10))) as ex_rt
			from (
				(
					select distinct edw_crncy_exch.ex_rt_typ
						,edw_crncy_exch.from_crncy
						,edw_crncy_exch.to_crncy
						,to_date(cast((cast(((cast((cast((99999999) as decimal)) as decimal(18, 0)) - cast((cast((edw_crncy_exch.vld_from) as decimal)) as decimal(18, 0)))) as varchar)) as text), cast((cast('YYYYMMDD' as varchar)) as text)) as vld_from
						,edw_crncy_exch.ex_rt
						,edw_crncy_exch.from_ratio
						,edw_crncy_exch.to_ratio
					from edw_crncy_exch as edw_crncy_exch
					where (
							(
								(
									(cast((edw_crncy_exch.ex_rt_typ) as text) = cast((cast('BWAR' as varchar)) as text))
									and (cast((edw_crncy_exch.from_crncy) as text) <> cast((cast('LKR' as varchar)) as text))
									)
								and (cast((edw_crncy_exch.from_crncy) as text) <> cast((cast('BDT' as varchar)) as text))
								)
							and (cast((edw_crncy_exch.from_crncy) as text) <> cast((cast('NZD' as varchar)) as text))
							)
					) as drvd_crncy join edw_calendar_dim as cal
					on (
							(
								(drvd_crncy.vld_from = cal.cal_day)
								and (cast((cal.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
								)
							)
				)
			group by drvd_crncy.ex_rt_typ
				,drvd_crncy.from_crncy
				,drvd_crncy.to_crncy
				,substring(cast((cast((drvd_crncy.vld_from) as varchar)) as text), 1, 4)
				,cal.fisc_yr
			) as a join (
			select edw_calendar_dim.fisc_yr as "year"
				,edw_calendar_dim.fisc_per
				,min(edw_calendar_dim.cal_day) as vld_from
			from edw_calendar_dim
			where (cast((edw_calendar_dim.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
			group by edw_calendar_dim.fisc_yr
				,edw_calendar_dim.fisc_per
			) as calmonthstartdate
			on (
					(
						(a."year" = calmonthstartdate."year")
						and (calmonthstartdate.vld_from <= to_variant(current_date ())::varchar)
						)
					)
		)
	 as derived_table1 where (derived_table1.latest_ex_rt_by_fisc_per = 1)

union all
	
	select distinct edw_crncy_exch.ex_rt_typ
	,edw_crncy_exch.from_crncy
	,edw_crncy_exch.from_crncy as to_crncy
	,calmonthstartdate.vld_from
	,1 as ex_rt
	,calmonthstartdate.fisc_per from (
	edw_crncy_exch as edw_crncy_exch join (
		select edw_calendar_dim.fisc_yr as "year"
			,edw_calendar_dim.fisc_per
			,min(edw_calendar_dim.cal_day) as vld_from
		from edw_calendar_dim
		where (
				(cast((edw_calendar_dim.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
				and (edw_calendar_dim.cal_day <= to_variant(current_date ())::varchar)
				)
		group by edw_calendar_dim.fisc_yr
			,edw_calendar_dim.fisc_per
		) as calmonthstartdate
		on ((1 = 1))
	) where (cast((edw_crncy_exch.ex_rt_typ) as text) = cast((cast('BWAR' as varchar)) as text))
	)

union all

select drvd_crncy.ex_rt_typ
	,drvd_crncy.from_crncy
	,drvd_crncy.to_crncy
	,drvd_crncy.vld_from
	,cast((
			case 
				when (
						(cast((drvd_crncy.to_crncy) as text) = cast((cast('USD' as varchar)) as text))
						and (
							(
								(cast((drvd_crncy.from_crncy) as text) = cast((cast('IDR' as varchar)) as text))
								or (cast((drvd_crncy.from_crncy) as text) = cast((cast('KRW' as varchar)) as text))
								)
							or (cast((drvd_crncy.from_crncy) as text) = cast((cast('VND' as varchar)) as text))
							)
						)
					then (drvd_crncy.ex_rt / cast((cast((1000) as decimal)) as decimal(18, 0)))
				when (
						(cast((drvd_crncy.to_crncy) as text) = cast((cast('USD' as varchar)) as text))
						and (cast((drvd_crncy.from_crncy) as text) = cast((cast('JPY' as varchar)) as text))
						)
					then (drvd_crncy.ex_rt / cast((cast((100) as decimal)) as decimal(18, 0)))
				else drvd_crncy.ex_rt
				end
			) as decimal(20, 10)) as ex_rt
	,drvd_crncy.fisc_per
from (
	select distinct edw_crncy_exch.ex_rt_typ
		,edw_crncy_exch.from_crncy
		,edw_crncy_exch.to_crncy
		,edw_crncy_exch.vld_from
		,edw_crncy_exch.ex_rt
		,edw_crncy_exch.fisc_per
		,rank() over (
			partition by edw_crncy_exch.from_crncy
			,edw_crncy_exch.to_crncy
			,edw_crncy_exch.fisc_per order by edw_crncy_exch.vld_from
			) as latest_ex_rt_by_fisc_per
	from (
		select a.ex_rt_typ
			,a.from_crncy
			,a.to_crncy
			,a.ex_rt
			,a.vld_from
			,b.fisc_per
		from (
			(
				select a.ex_rt_typ
					,a.from_crncy
					,a.to_crncy
					,a.ex_rt
					,to_date(cast((cast(((cast((cast((99999999) as decimal)) as decimal(18, 0)) - cast((cast((a.vld_from) as decimal)) as decimal(18, 0)))) as varchar)) as text), cast((cast('YYYYMMDD' as varchar)) as text)) as vld_from
				from edw_crncy_exch as a
				where (
						(cast((a.ex_rt_typ) as text) = cast((cast('BWAR' as varchar)) as text))
						and (
							(
								(cast((a.from_crncy) as text) = cast((cast('LKR' as varchar)) as text))
								or (cast((a.from_crncy) as text) = cast((cast('BDT' as varchar)) as text))
								)
							or (cast((a.from_crncy) as text) = cast((cast('NZD' as varchar)) as text))
							)
						)
				) as a join edw_calendar_dim as b
				on ((a.vld_from = b.cal_day))
			)
		where (cast((b.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
		
		union all
		
		select ex.ex_rt_typ
			,ex.from_crncy
			,ex.to_crncy
			,ex.ex_rt
			,ex.vld_from
			,c.fisc_per
		from (
			(
				select a.ex_rt_typ
					,a.from_crncy
					,a.to_crncy
					,a.ex_rt
					,a.vld_from
					,b.fisc_per
				from (
					(
						select a.ex_rt_typ
							,a.from_crncy
							,a.to_crncy
							,a.ex_rt
							,a.vld_from
						from (
							(
								select edw_crncy_exch.ex_rt_typ
									,edw_crncy_exch.from_crncy
									,edw_crncy_exch.to_crncy
									,edw_crncy_exch.ex_rt
									,to_date(cast((cast(((cast((cast((99999999) as decimal)) as decimal(18, 0)) - cast((cast((edw_crncy_exch.vld_from) as decimal)) as decimal(18, 0)))) as varchar)) as text), cast((cast('YYYYMMDD' as varchar)) as text)) as vld_from
								from edw_crncy_exch
								where (
										(cast((edw_crncy_exch.ex_rt_typ) as text) = cast((cast('BWAR' as varchar)) as text))
										and (
											(
												(cast((edw_crncy_exch.from_crncy) as text) = cast((cast('LKR' as varchar)) as text))
												or (cast((edw_crncy_exch.from_crncy) as text) = cast((cast('BDT' as varchar)) as text))
												)
											or (cast((edw_crncy_exch.from_crncy) as text) = cast((cast('NZD' as varchar)) as text))
											)
										)
								) as a join (
								select edw_crncy_exch.from_crncy
									,edw_crncy_exch.to_crncy
									,max(to_date(cast((cast(((cast((cast((99999999) as decimal)) as decimal(18, 0)) - cast((cast((edw_crncy_exch.vld_from) as decimal)) as decimal(18, 0)))) as varchar)) as text), cast((cast('YYYYMMDD' as varchar)) as text))) as vld_from
								from edw_crncy_exch
								where (
										(cast((edw_crncy_exch.ex_rt_typ) as text) = cast((cast('BWAR' as varchar)) as text))
										and (
											(
												(cast((edw_crncy_exch.from_crncy) as text) = cast((cast('LKR' as varchar)) as text))
												or (cast((edw_crncy_exch.from_crncy) as text) = cast((cast('BDT' as varchar)) as text))
												)
											or (cast((edw_crncy_exch.from_crncy) as text) = cast((cast('NZD' as varchar)) as text))
											)
										)
								group by edw_crncy_exch.from_crncy
									,edw_crncy_exch.to_crncy
								) as b
								on (
										(
											(
												(cast((a.from_crncy) as text) = cast((b.from_crncy) as text))
												and (cast((a.to_crncy) as text) = cast((b.to_crncy) as text))
												)
											and (a.vld_from = b.vld_from)
											)
										)
							)
						) as a join edw_calendar_dim as b
						on ((a.vld_from = b.cal_day))
					)
				where (cast((b.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
				) as ex join (
				select distinct edw_calendar_dim.fisc_per
				from edw_calendar_dim
				where (
						(cast((edw_calendar_dim.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
						and (edw_calendar_dim.cal_day <= to_variant(current_date ())::varchar)
						)
				) as c
				on ((ex.fisc_per < c.fisc_per))
			)
		) as edw_crncy_exch
	) as drvd_crncy
where (drvd_crncy.latest_ex_rt_by_fisc_per = 1)
)


--final select
select * from final 