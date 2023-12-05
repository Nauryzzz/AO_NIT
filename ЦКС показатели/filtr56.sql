/* 35. Информация по выпускникам послевузовского образования */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
	'filtr56' as filtr, -- информация по выпускникам послевузовского образования
	if(count(p35.IIN) > 0, 1, 0) as filtr_value
from
	(select distinct
		n_148.IIN,
		n_149.value
	from
		(select distinct
			gp.IIN as IIN
		from MU_FL.GBL_PERSON as gp
		where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 16 and 
			date_diff(year, toDate(gp.BIRTH_DATE), today()) < 29 and
			gp.REMOVED = 0 and 
			(gp.EXCLUDE_REASON_ID is null or gp.EXCLUDE_REASON_ID = 1) and
			gp.PERSON_STATUS_ID <> 3) as n_148
			  
		inner join
		
		(select distinct
			vt2.IIN as IIN,
			if(date_diff(year, toDate(vt2.OUT_DATE), today()) <= 3, 1, 0) as value
		from
			(select 
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
				where s.DATE_CLOSE1 is null and
					ts.ID in (4, /* Организации высшего образования */
							  5  /* Организации высшего и (или) послевузовского образования */) and 
					st.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and
					st.IIN is not null and 
					e.STUDEDU_LEVEL_ID in (6,   /* Магистратура (Научно-педагогическое направление) */
											7,  /* Магистратура (Профильное направление) */
											8,  /* Резидентура */
											9,  /* Докторантура (Научно-педагогическое направление) */
											10, /* Докторантура (Профильное направление) */
											13, /* Доктор делового администрирования */
											14, /* Педагогическая переподготовка */
											15, /* Магистр делового администрирования */
											16  /* Магистратура (Свидетельство к диплому магистра) */)
					
					) as vt1
				where vt1.num = 1) as vt2
			where vt2.REG_DATE is not null and vt2.OUT_DATE is not null) as n_149
		on n_148.IIN = n_149.IIN) as p35
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p35.IIN
group by toString(fm.SK_FAMILY_ID);