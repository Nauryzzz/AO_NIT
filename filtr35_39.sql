/* 
	1. на 5-ом сервере запустить. Появляется таблица SK_FAMILY.filtr35_39
	2. экспорт как CSV таблицы SK_FAMILY.filtr35_39
	3. импорт на 17 сервер в таблицу SOC_KARTA.filtr35_39
*/

DROP TABLE IF EXISTS SK_FAMILY.filtr35_39;

CREATE TABLE SK_FAMILY.filtr35_39
(
    `SK_FAMILY_ID` String COMMENT 'ID семьи',
    `filtr35` Nullable(Int16) COMMENT 'ребенок обеспеченный бесплатным питанием',
    `filtr36` Nullable(Int16) COMMENT 'ребенок, обеспеченный бесплатным подвозом до школы',
    `filtr37` Nullable(Int16) COMMENT 'ребенок-инвалид, ученик',
    `filtr38` Nullable(Int16) COMMENT 'взрослое население, зависимое от ПАВ',
    `filtr39` Nullable(Int16) COMMENT 'семья, один из членов семьи которой зависим от ПАВ с принудительным решением суда'
)
ENGINE = MergeTree
ORDER BY SK_FAMILY_ID
SETTINGS index_granularity = 8192;

insert into 
	SK_FAMILY.filtr35_39 (SK_FAMILY_ID, filtr35, filtr36, filtr37, filtr38, filtr39)
select 
	SK_FAMILY_ID,
	sum(if(filtr = 'filtr35', filtr_value, 0)) as filtr35,
	sum(if(filtr = 'filtr36', filtr_value, 0)) as filtr36,
	sum(if(filtr = 'filtr37', filtr_value, 0)) as filtr37,
	sum(if(filtr = 'filtr38', filtr_value, 0)) as filtr38,
	sum(if(filtr = 'filtr39', filtr_value, 0)) as filtr39
from
	(select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr35' as filtr, -- ребенок обеспеченный бесплатным питанием
		if(count(p10.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			distinct n_48.IIN as IIN
		from
			(select 
				distinct gp.IIN as IIN
			from MU_FL.GBL_PERSON as gp
			where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 5 and 
				  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18) as n_48
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
		on n_48.IIN = n49_50.IIN) as p10
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p10.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr36' as filtr, -- ребенок, обеспеченный бесплатным подвозом до школы
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
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr37' as filtr, -- ребенок-инвалид, ученик
		if(count(p12.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			distinct n_54.IIN as IIN
		from
			(select 
				distinct gp.IIN as IIN
			from MU_FL.GBL_PERSON as gp
			where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 5 and 
				  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18) as n_54	  
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
		on n_54.IIN = n55.IIN
		inner join
			(select 
				distinct pi.RN as IIN
			from MTSZN_CBDIAPP.PATIENT_INFO as pi
			where pi.INV_GROUP in (4, 9, 6, 7, 8) and toDate(pi.INV_ENDDATE) >= today()) as n56
		on n_54.IIN = n56.IIN) as p12
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
				where date_diff(year, toDate(gp.BIRTH_DATE), today()) > 18) as n105
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
				distinct n_109.IIN as IIN
			from
				(select 
					distinct gp.IIN as IIN
				from MU_FL.GBL_PERSON as gp
				where date_diff(year, toDate(gp.BIRTH_DATE), today()) > 18) as n_109
			inner join
				(select
					distinct p.IIN as IIN
				from MZ_RPN.PERSON p
					inner join MZ_RPN.ATTACHMENTS as att on att.PERSONID = p.ID 
				where att.ENDDATE is null and p.DEATHDATE is null and 
					p.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
					p.IIN is not null) as n_111
			on n_109.IIN = n_111.IIN
			inner join
				(select 
					distinct h.IIN as IIN
				from MZ_ERDB.HUMAN as h
					inner join MZ_ERDB.HUMAN_DIAG as hd on hd.HUMAN_UID = h.UID
				where hd.ICD10 between 'F10' and 'F19.9') as n_112
			on n_109.IIN = n_112.IIN
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
				distinct pi.RN as IIN
			from MTSZN_CBDIAPP.PATIENT_INFO as pi
			where pi.INV_GROUP in (1, 2) and toDate(pi.INV_ENDDATE) >= today()) as p23
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p23.IIN
	group by toString(fm.SK_FAMILY_ID)) as filtr35_39
group by SK_FAMILY_ID;