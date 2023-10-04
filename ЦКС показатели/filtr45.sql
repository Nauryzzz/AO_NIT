/* 31. Информация по детям-сиротам */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
	'filtr45' as filtr, -- информация по детям-сиротам
	if(count(p31.IIN) > 0, 1, 0) as filtr_value
from
	(select 
		distinct vt1.IIN as IIN
	from
		(select 
			distinct gp.IIN as IIN
		from MU_FL.GBL_PERSON as gp
		where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 0 and 
			  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18 and
			  gp.PERSON_STATUS_ID <> 3) as vt1
	inner join
		(select 
			distinct st.IIN as IIN
		from MON_NOBD.STUDENT as st
			inner join MON_NOBD.EDUCATION as e on e.STUDENT_ID = st.ID
		where e.IS_ORPHAN is not null and e.IS_ORPHAN <> 0 and
			st.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
			st.IIN is not null) as vt2
	on vt1.IIN = vt2.IIN) as p31
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p31.IIN
group by toString(fm.SK_FAMILY_ID)