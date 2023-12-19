/* 64. Информация по выпускникам школ-интернатов */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr64' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p64.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select distinct
		n_148.IIN,
		n_149.value
	from
		(select -- люди от 16 до 29
			distinct gp.IIN as IIN
		from MU_FL.GBL_PERSON as gp
		where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 16 and 
			date_diff(year, toDate(gp.BIRTH_DATE), today()) < 29 and
			gp.REMOVED = 0 and 
			(gp.EXCLUDE_REASON_ID is null or gp.EXCLUDE_REASON_ID = 1) and
			gp.PERSON_STATUS_ID <> 3) as n_148
			  
		inner join -- объединение людей от 16 до 29 лет с людьми с выпускниками школ-интернатов
		
		(select distinct -- выпускники школ-интернатов
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
					inner join MON_NOBD.SCHOOL_SPEC as ss on ss.SCHOOL_ATTR_ID = sattr.ID 
					inner join MON_NOBD.D_SCHOOLSPEC_TYPE as dst on dst.ID = ss.SPEC_TYPE_ID 
				where s.DATE_CLOSE1 is null and
					st.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and
					st.IIN is not null and 
					dst.CODE in ('02.5.1', 	/* школа-интернат */
								'07.04',  	/* специализированная школа-интернат */
								'08.4',  	/* специальная школа-интернат */
								'07.08', 	/* специализированная спортивная школа-интернат (специализированная школа-интернат-колледж олимпийского резерва) */
								'07.05', 	/* специализированная школа-лицей-интернат */
								'07.06', 	/* специализированная школа-гимназия-интернат */
								'02.5.6', 	/* санаторная школа-интернат */
								'07.10', 	/* специализированная военная школа-интернат */
								'02.5.5',	/* школа-интернат для детей из многодетных и малообеспеченных семей */
								'07.07', 	/* специализированная музыкальная школа-интернат */
								'09.4' 		/* Центр поддержки детей, с особыми образовательными потребностями (школа-интернат для детей-сирот и детей, ОБПР, с ООП) */)
					) as vt1
				where vt1.num = 1 /* последняя запись по REG_DATE */) as vt2
			where vt2.REG_DATE is not null and vt2.OUT_DATE is not null) as n_149
		on n_148.IIN = n_149.IIN) as p64
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p64.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);