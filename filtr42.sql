/* 28. Наличие действующего трудового договора */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
	'filtr42' as filtr, -- наличие действующего трудового договора
	if(count(p28.IIN) > 0, 1, 0) as filtr_value
from
	(select 
		distinct e.IIN as IIN
	from MTSZN_ESUTD.EMPLOYEE as e
		inner join MTSZN_ESUTD.CONTRACT as c on c.EMPLOYEE_ID = e.ID 
	where c.TERMINATION_DATE is null) as p28
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p28.IIN
group by toString(fm.SK_FAMILY_ID)