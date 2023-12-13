/* 34. Информация по выпускникам ВУЗОВ */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr55' as filtr, -- информация по выпускникам ВУЗОВ
	if(count(p34.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select distinct
		n_145.IIN,
		n_146.value
	from
		(select -- люди от 16 до 29
			distinct gp.IIN as IIN
		from MU_FL.GBL_PERSON as gp
		where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 16 and 
			date_diff(year, toDate(gp.BIRTH_DATE), today()) < 29 and
			gp.REMOVED = 0 and 
			(gp.EXCLUDE_REASON_ID is null or gp.EXCLUDE_REASON_ID = 1) and
			gp.PERSON_STATUS_ID <> 3) as n_145
			  
		inner join -- объединение людей от 16 до 29 лет с людьми с высшим образованием
		
		(select distinct -- выпускники ВУЗ-ов
			vt2.IIN as IIN,
			if(date_diff(year, toDate(vt2.OUT_DATE), today()) <= 3, 1, 0) as value -- если обучение завершено менее 3 лех назад, то принзак будет 1 иначе 0
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
					e.STUDEDU_LEVEL_ID in (1, /* Первое высшее образование */ 
										   2, /* Первое высшее образование (5 лет)/(специалитет) */ 
										   3, /* Второе высшее образование */ 
										   4, /* Первое высшее сокращенное образование */ 
										   5  /* Интернатура */)
					
					) as vt1
				where vt1.num = 1 /* последняя запись по REG_DATE */) as vt2
			where vt2.REG_DATE is not null and vt2.OUT_DATE is not null) as n_146
		on n_145.IIN = n_146.IIN) as p34
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p34.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);