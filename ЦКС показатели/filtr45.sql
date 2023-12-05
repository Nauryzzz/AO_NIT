/* 31. Информация по детям-сиротам */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr45' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p31.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select 
		distinct vt1.IIN as IIN
	from
		(select -- дети до 18 лет
			distinct gp.IIN as IIN 
		from MU_FL.GBL_PERSON as gp
		where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 0 and 
			date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18 and
			gp.REMOVED = 0 and 
			(gp.EXCLUDE_REASON_ID is null or gp.EXCLUDE_REASON_ID = 1) and
			gp.PERSON_STATUS_ID <> 3 /* признак: не мертв */) as vt1
	inner join -- объединение детей до 18 лет со списком сирот
		(select -- список детей сирот
			distinct st.IIN as IIN
		from MON_NOBD.STUDENT as st
			inner join MON_NOBD.EDUCATION as e on e.STUDENT_ID = st.ID
		where e.IS_ORPHAN is not null /* признак сирота */ and 
			e.IS_ORPHAN <> 0 /* признак сирота */ and 
			st.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
			st.IIN is not null) as vt2
	on vt1.IIN = vt2.IIN) as p31
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p31.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);