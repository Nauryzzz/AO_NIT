/* 12. Ребенок-инвалид, ученик */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr37' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p12.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select 
		distinct n54.IIN as IIN
	from
		(select -- дети от 5 до 18 лет
			distinct gp.IIN as IIN
		from MU_FL.GBL_PERSON as gp
		where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 5 and 
			date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18 and
			gp.REMOVED = 0 and 
			(gp.EXCLUDE_REASON_ID is null or gp.EXCLUDE_REASON_ID = 1) and
			 gp.PERSON_STATUS_ID <> 3 /* признак: не мертв */) as n54  
	inner join -- объединение детей от 5 до 18 лет с обучающимися детьми
		(select 
			distinct vt2.IIN as IIN
		from
			(select -- дети обучающиеся в школе
				vt1.IIN, 
				vt1.REG_DATE, vt1.OUT_DATE
			from
				(select 
					st.IIN as IIN,
					e.REG_DATE as REG_DATE, e.OUT_DATE as OUT_DATE, 
					row_number() over (partition by st.IIN order by e.REG_DATE desc) as num
				from MON_NOBD.STUDENT as st
					inner join MON_NOBD.EDUCATION as e on e.STUDENT_ID = st.ID 
					inner join MON_NOBD.SCHOOL as s on s.ID = e.SCHOOL_ID 
					inner join MON_NOBD.SCHOOL_ATTR as sattr on sattr.SCHOOL_ID = s.ID
					inner join MON_NOBD.D_TYPE_SCHOOL as ts on ts.ID = sattr.SCHOOL_TYPE_ID
				where s.DATE_CLOSE1 is null and -- школа еще не закрыта
					ts.ID = 2 and -- Организации среднего образования(начального, основного среднего и общего среднего)
					st.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and
					st.IIN is not null) as vt1
			where vt1.num = 1 /* последняя запись по REG_DATE */) as vt2
		where (vt2.REG_DATE is not null) and (toDate(vt2.OUT_DATE) >= today() or vt2.OUT_DATE is null)) as n55
	on n54.IIN = n55.IIN
	inner join -- объединение обучающихся детей от 5 до 18 лет с детьми имеющими инвалидность
		(select -- дети имеющие инвалидность
			distinct pi.RN as IIN
		from MTSZN_CBDIAPP.PATIENT_INFO_ACTUAL as pi
		where pi.RN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and
			pi.INV_GROUP in (4, /* Ребенок- инвалид */
							 9, /* Ребенок- инвалид */
							 6, /* Ребенок- инвалид 1 группа */
							 7, /* Ребенок- инвалид 2 группа */
							 8  /* Ребенок- инвалид 3 группа */) and 
			(toDate(pi.INV_ENDDATE) >= today() or pi.INV_ENDDATE = '0000-00-00')) as n56
	on n54.IIN = n56.IIN) as p12
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p12.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);
