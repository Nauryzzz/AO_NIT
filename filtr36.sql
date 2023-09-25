/* 11. Ребенок, обеспеченный бесплатным подвозом до школы */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
	'filtr36' as filtr,
	if(count(p11.IIN) > 0, 1, 0) as filtr_value
from
	(select 
		distinct n_51.IIN as IIN
	from
		(select 
			distinct gp.IIN as IIN
		from MU_FL.GBL_PERSON as gp
		where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 5 and 
			  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18) as n_51	  
	inner join
		(select 
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
				where st.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
					st.IIN is not null and
					e.SCHOOL_ID in (select 
										distinct s.ID as ID
									from MON_NOBD.SCHOOL as s
										inner join MON_NOBD.SCHOOL_ATTR as sattr on sattr.SCHOOL_ID = s.ID
										inner join MON_NOBD.D_TYPE_SCHOOL as ts on ts.ID = sattr.SCHOOL_TYPE_ID
										inner join MON_NOBD.EAGENCY as ea on ea.ID = sattr.EAGENCY_ID 
										inner join MON_NOBD.EAGENCY_ATTR as eattr on eattr.EAGENCY_ID = ea.ID
										inner join MON_NOBD.EAGENCY_RURAL_NOEORG as trans on trans.EAGENCY_ATTR_ID = eattr.ID 
									where s.DATE_CLOSE1 is null and ts.ID = 2 and ifNull(trans.DAILYTRANSP_CNT, 0) > 0)
				) as vt1
			where vt1.num = 1) as vt2
		where (vt2.REG_DATE is not null) and (toDate(vt2.OUT_DATE) >= today() or vt2.OUT_DATE is null)) as n52_53
	on n_51.IIN = n52_53.IIN) as p11
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p11.IIN
group by toString(fm.SK_FAMILY_ID)