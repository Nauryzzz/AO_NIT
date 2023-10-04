-- запускать на 5-ом сервере 

DROP TABLE IF EXISTS SK_FAMILY.PROON;

CREATE TABLE SK_FAMILY.PROON
(
    `SK_FAMILY_ID` String COMMENT 'ID семьи',
    `filtr35` Nullable(Int16) COMMENT 'ребенок обеспеченный бесплатным питанием',
    `filtr36` Nullable(Int16) COMMENT 'ребенок, обеспеченный бесплатным подвозом до школы',
    `filtr37` Nullable(Int16) COMMENT 'ребенок-инвалид, ученик',
    `filtr38` Nullable(Int16) COMMENT 'взрослое население, зависимое от ПАВ',
    `filtr39` Nullable(Int16) COMMENT 'семья, один из членов семьи которой зависим от ПАВ с принудительным решением суда',
    `filtr40` Nullable(Int16) COMMENT 'несовершеннолетние дети, зависимые от ПАВ',
    `filtr41` Nullable(Int16) COMMENT 'регистрация акта о несчастном случае на производстве',
    `filtr42` Nullable(Int16) COMMENT 'наличие действующего трудового договора'
)
ENGINE = MergeTree
ORDER BY SK_FAMILY_ID
SETTINGS index_granularity = 8192;

insert into 
	SK_FAMILY.PROON (SK_FAMILY_ID, filtr35, filtr36, filtr37, filtr38, filtr39, filtr40, filtr41, filtr42)
select 
	SK_FAMILY_ID,
	sum(if(filtr = 'filtr35', filtr_value, 0)) as filtr35,
	sum(if(filtr = 'filtr36', filtr_value, 0)) as filtr36,
	sum(if(filtr = 'filtr37', filtr_value, 0)) as filtr37,
	sum(if(filtr = 'filtr38', filtr_value, 0)) as filtr38,
	sum(if(filtr = 'filtr39', filtr_value, 0)) as filtr39,
	sum(if(filtr = 'filtr40', filtr_value, 0)) as filtr40,
	sum(if(filtr = 'filtr41', filtr_value, 0)) as filtr41,
	sum(if(filtr = 'filtr42', filtr_value, 0)) as filtr42
from
	(select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr35' as filtr, -- ребенок обеспеченный бесплатным питанием
		if(count(p10.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			distinct n48.IIN as IIN
		from
			(select 
				distinct gp.IIN as IIN
			from MU_FL.GBL_PERSON as gp
			where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 5 and 
				  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18 and
				  gp.PERSON_STATUS_ID <> 3) as n48
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
						inner join MON_NOBD.EDU_FOODPROVIDE as fp on fp.EDUCATION_ID = e.ID 
						inner join MON_NOBD.SCHOOL as s on s.ID = e.SCHOOL_ID 
						inner join MON_NOBD.SCHOOL_ATTR as sattr on sattr.SCHOOL_ID = s.ID
						inner join MON_NOBD.D_TYPE_SCHOOL as ts on ts.ID = sattr.SCHOOL_TYPE_ID
					where s.DATE_CLOSE1 is null and
						ts.ID = 2 and 
						fp.HOTMEAL_PROVIDE_ID is not null and 
						st.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
						st.IIN is not null) as vt1
				where vt1.num = 1) as vt2
			where (vt2.REG_DATE is not null) and (toDate(vt2.OUT_DATE) >= today() or vt2.OUT_DATE is null)) as n49_50
		on n48.IIN = n49_50.IIN) as p10
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p10.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr36' as filtr, -- ребенок, обеспеченный бесплатным подвозом до школы
		if(count(p11.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			distinct n51.IIN as IIN
		from
			(select 
				distinct gp.IIN as IIN
			from MU_FL.GBL_PERSON as gp
			where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 5 and 
				  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18 and
				  gp.PERSON_STATUS_ID <> 3) as n51	  
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
		on n51.IIN = n52_53.IIN) as p11
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p11.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr37' as filtr, -- ребенок-инвалид, ученик
		if(count(p12.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			distinct n54.IIN as IIN
		from
			(select 
				distinct gp.IIN as IIN
			from MU_FL.GBL_PERSON as gp
			where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 5 and 
				  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18 and
				  gp.PERSON_STATUS_ID <> 3) as n54	  
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
						inner join MON_NOBD.SCHOOL as s on s.ID = e.SCHOOL_ID 
						inner join MON_NOBD.SCHOOL_ATTR as sattr on sattr.SCHOOL_ID = s.ID
						inner join MON_NOBD.D_TYPE_SCHOOL as ts on ts.ID = sattr.SCHOOL_TYPE_ID
					where s.DATE_CLOSE1 is null and
						ts.ID = 2 and 
						st.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and
						st.IIN is not null) as vt1
				where vt1.num = 1) as vt2
			where (vt2.REG_DATE is not null) and (toDate(vt2.OUT_DATE) >= today() or vt2.OUT_DATE is null)) as n55
		on n54.IIN = n55.IIN
		inner join
			(select 
				distinct pi.RN as IIN
			from MTSZN_CBDIAPP.PATIENT_INFO as pi
			where pi.INV_GROUP in (4, 9, 6, 7, 8) and toDate(pi.INV_ENDDATE) >= today()) as n56
		on n54.IIN = n56.IIN) as p12
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p12.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr38' as filtr, -- взрослое население, зависимое от ПАВ
		if(count(p22.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			distinct vt.IIN as IIN
		from
			(select 
				distinct n105.IIN as IIN
			from
				(select 
					distinct gp.IIN as IIN
				from MU_FL.GBL_PERSON as gp
				where 
					date_diff(year, toDate(gp.BIRTH_DATE), today()) > 18 and
					gp.PERSON_STATUS_ID <> 3) as n105
			inner join
				(select 
					distinct h.IIN as IIN
				from MZ_ERDB.HUMAN as h
					inner join MZ_ERDB.HUMAN_DIAG as hd on hd.HUMAN_UID = h.UID
				where hd.ICD10 between 'F10' and 'F19.9') as n106_107
			on n105.IIN = n106_107.IIN
		except
			select 
				distinct cc.defendant as IIN
			from SUPREME_COURT.COURTS_CASES as cc
			where cat = 2 and category = '142080004600000000') as vt) as p22
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p22.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr39' as filtr, -- Семья, один из членов семьи которой зависим от ПАВ с принудительным решением суда
		if(count(p23.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			distinct vt1.IIN as IIN
		from
			(select 
				distinct n109.IIN as IIN
			from
				(select 
					distinct gp.IIN as IIN
				from MU_FL.GBL_PERSON as gp
				where 
					date_diff(year, toDate(gp.BIRTH_DATE), today()) > 18 and
					gp.PERSON_STATUS_ID <> 3) as n109
			inner join
				(select
					distinct p.IIN as IIN
				from MZ_RPN.PERSON p
					inner join MZ_RPN.ATTACHMENTS as att on att.PERSONID = p.ID 
				where att.ENDDATE is null and p.DEATHDATE is null and 
					p.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
					p.IIN is not null) as n_111
			on n109.IIN = n_111.IIN
			inner join
				(select 
					distinct h.IIN as IIN
				from MZ_ERDB.HUMAN as h
					inner join MZ_ERDB.HUMAN_DIAG as hd on hd.HUMAN_UID = h.UID
				where hd.ICD10 between 'F10' and 'F19.9') as n_112
			on n109.IIN = n_112.IIN
				except
			select 
				distinct h.IIN as IIN
			from MZ_REGISTERS_BASE.HUMAN as h 
				inner join MZ_REGISTERS_BASE.BER_KARTA bk on bk.HUMAN_UID = h.UID 
			where h.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and h.IIN is not null) as vt1
		inner join
			(select 
				distinct cc.defendant as IIN
			from SUPREME_COURT.COURTS_CASES as cc
			where cat = 2 and category = '142080004600000000') as n_114
		on vt1.IIN = n_114.IIN
		except
			select 
				distinct p.IIN as IIN
			from MTSZN_CBDIAPP.PATIENT as p
				inner join MTSZN_CBDIAPP.PATIENT_INFO as pi on pi.PATIENT_ID = p.ID 
			where pi.INV_GROUP in (1, 2) and toDate(pi.INV_ENDDATE) >= today()) as p23
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p23.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr40' as filtr, -- несовершеннолетние дети, зависимые от ПАВ
		if(count(p21.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			distinct vt.IIN as IIN
		from
			(select 
				distinct n101.IIN as IIN
			from
				(select 
					distinct gp.IIN as IIN
				from MU_FL.GBL_PERSON as gp
				where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 7 and 
					  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18 and
					  gp.PERSON_STATUS_ID <> 3) as n101
			inner join
				(select 
					distinct h.IIN as IIN
				from MZ_ERDB.HUMAN as h
					inner join MZ_ERDB.HUMAN_DIAG as hd on hd.HUMAN_UID = h.UID
				where hd.ICD10 between 'F10' and 'F19.9') as n102_103
			on n101.IIN = n102_103.IIN) as vt) as p21
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p21.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr41' as filtr, -- регистрация акта о несчастном случае на производстве
		if(count(p27.IIN) > 0, 1, 0) as filtr_value
	from
		(select
			distinct pc.CODE_IIN as IIN
		from MTSZN_LABORPROTECT.PA_CARD as pc
			inner join MTSZN_LABORPROTECT.N1 as n1 on n1.PA_CARD_ID = pc.PA_CARD_ID 
		where n1.D_PHYSIO_STATE_ID in (1, 2, 3)) as p27
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p27.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
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
	group by toString(fm.SK_FAMILY_ID)) as filtr35_42
group by SK_FAMILY_ID;