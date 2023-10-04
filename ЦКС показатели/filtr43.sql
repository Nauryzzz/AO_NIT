/* 9. Ребенок, не охваченный школьным образованием */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
	'filtr43' as filtr, -- ребенок, не охваченный школьным образованием
	if(count(p9.IIN) > 0, 1, 0) as filtr_value
from
	(select 
		distinct gp.IIN as IIN
	from MU_FL.GBL_PERSON as gp
	where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 5 and 
		  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18 and 
		  gp.PERSON_STATUS_ID <> 3

	except

	select 
		distinct vt2.IIN as IIN
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
				ts.ID = 2 and 
				st.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and
				st.IIN is not null) as vt1
		where vt1.num = 1) as vt2
	where (vt2.REG_DATE is not null) and (toDate(vt2.OUT_DATE) >= today() or vt2.OUT_DATE is null)) as p9
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p9.IIN
group by toString(fm.SK_FAMILY_ID)