-- запускать на 5-ом сервере 

DROP TABLE IF EXISTS SK_FAMILY.PROON;

CREATE TABLE SK_FAMILY.PROON
(
    `SK_FAMILY_ID` String COMMENT 'ID семьи',
    `filtr35` Nullable(Int16) COMMENT 'ребенок, обеспеченный бесплатным питанием',
    `filtr36` Nullable(Int16) COMMENT 'ребенок, обеспеченный бесплатным подвозом до школы',
    `filtr37` Nullable(Int16) COMMENT 'ребенок-инвалид, ученик',
    `filtr38` Nullable(Int16) COMMENT 'взрослое население, зависимое от ПАВ',
    `filtr39` Nullable(Int16) COMMENT 'семья, один из членов семьи которой зависим от ПАВ с принудительным решением суда',
    `filtr40` Nullable(Int16) COMMENT 'несовершеннолетние дети, зависимые от ПАВ',
    `filtr41` Nullable(Int16) COMMENT 'регистрация акта о несчастном случае на производстве',
    `filtr42` Nullable(Int16) COMMENT 'наличие действующего трудового договора'
	`filtr43` Nullable(Int16) COMMENT 'ребенок, не охваченный школьным образованием'
	`filtr44` Nullable(Int16) COMMENT 'жители сельской местности'
	`filtr45` Nullable(Int16) COMMENT 'информация по детям-сиротам'
	`filtr46` Nullable(Int16) COMMENT 'молодежь'
	`filtr47` Nullable(Int16) COMMENT 'наличие членов семьи с количеством дней просрочки выплаты по кредиту 90+ дней'
	`filtr48` Nullable(Int16) COMMENT 'ребенок, отстающий в психофизическом развитии'
	`filtr49` Nullable(Int16) COMMENT 'информация о домашнем хозяйстве (животноводство, птицеводство, рыбоводство, растениеводство), о сельскохозяйственной технике'
	`filtr50` Nullable(Int16) COMMENT 'единая база данных идентификации с/х животных (ИС ИЖС)'
	`filtr51` Nullable(Int16) COMMENT 'информация по малообеспеченным гражданам'
	`filtr52` Nullable(Int16) COMMENT 'многодетная мать (более 4х детей менее 18 лет)'
	`filtr53` Nullable(Int16) COMMENT 'лица предпенсионного возраста (за 2 года до выхода на пенсию)'
	`filtr54` Nullable(Int16) COMMENT 'молодежь NEET (социальный статус не присвоен)'
)
ENGINE = MergeTree
ORDER BY SK_FAMILY_ID
SETTINGS index_granularity = 8192;

insert into 
	SK_FAMILY.PROON (SK_FAMILY_ID, filtr35, filtr36, filtr37, filtr38, filtr39, filtr40, filtr41, filtr42, filtr43, filtr44, filtr45, filtr46, filtr47, filtr48, filtr49, filtr50, filtr51, filtr52, filtr53, filtr54)
select 
	SK_FAMILY_ID,
	sum(if(filtr = 'filtr35', filtr_value, 0)) as filtr35,
	sum(if(filtr = 'filtr36', filtr_value, 0)) as filtr36,
	sum(if(filtr = 'filtr37', filtr_value, 0)) as filtr37,
	sum(if(filtr = 'filtr38', filtr_value, 0)) as filtr38,
	sum(if(filtr = 'filtr39', filtr_value, 0)) as filtr39,
	sum(if(filtr = 'filtr40', filtr_value, 0)) as filtr40,
	sum(if(filtr = 'filtr41', filtr_value, 0)) as filtr41,
	sum(if(filtr = 'filtr42', filtr_value, 0)) as filtr42,
	sum(if(filtr = 'filtr43', filtr_value, 0)) as filtr43,
	sum(if(filtr = 'filtr44', filtr_value, 0)) as filtr44,
	sum(if(filtr = 'filtr45', filtr_value, 0)) as filtr45,
	sum(if(filtr = 'filtr46', filtr_value, 0)) as filtr46,
	sum(if(filtr = 'filtr47', filtr_value, 0)) as filtr47,
	sum(if(filtr = 'filtr48', filtr_value, 0)) as filtr48,
	sum(if(filtr = 'filtr49', filtr_value, 0)) as filtr49,
	sum(if(filtr = 'filtr50', filtr_value, 0)) as filtr50,
	sum(if(filtr = 'filtr51', filtr_value, 0)) as filtr51,
	sum(if(filtr = 'filtr52', filtr_value, 0)) as filtr52,
	sum(if(filtr = 'filtr53', filtr_value, 0)) as filtr53,
	sum(if(filtr = 'filtr54', filtr_value, 0)) as filtr54
from
	(select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr35' as filtr, -- ребенок, обеспеченный бесплатным питанием
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
	group by toString(fm.SK_FAMILY_ID)

	UNION ALL
	
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
		
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, 
		'filtr44' as filtr, -- жители сельской местности
		if(count(p9.IIN) > 0, 1, 0) as filtr_value
	from SK_FAMILY.VILLAGE_IIN as p9
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p9.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
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
		where e.IS_ORPHAN is not null and 
			  e.IS_ORPHAN <> 0 and 
			  st.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
			  st.IIN is not null) as vt2
		on vt1.IIN = vt2.IIN) as p31
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p31.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL

	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr46' as filtr, -- молодежь
		if(count(p36.IIN) > 0, 1, 0) as filtr_value
	from
		(select
			distinct gp.IIN as IIN 
		from MU_FL.GBL_PERSON as gp
	where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 16 and 
		  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 35 and
		  gp.PERSON_STATUS_ID <> 3) as p36
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p36.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr47' as filtr, -- наличие членов семьи с количеством дней просрочки выплаты по кредиту 90+ дней
		if(count(p13.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			distinct n57.IIN as IIN 
		from
			(select
				distinct gp.IIN as IIN
			from MU_FL.GBL_PERSON as gp
			where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 18 and
				  gp.PERSON_STATUS_ID <> 3) as n57
		inner join
			(select
				distinct g.HASH_IIN as IIN 
			from SK_FAMILY.GKB as g
			where g.PAYMENT_DAYS_OVERDUE > 90 and 
				  g.DEBT_PASTDUE_VALUE > 1000) as n58 
		on n57.IIN = n58.IIN) as p13
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p13.IIN
	group by toString(fm.SK_FAMILY_ID)

	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr48' as filtr, -- ребенок, отстающий в психофизическом развитии
		if(count(p8.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			distinct n43.IIN
		from
			(select 
				distinct gp.IIN as IIN
			from MU_FL.GBL_PERSON as gp
			where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 0 and 
				  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18 and
				  gp.PERSON_STATUS_ID <> 3) as n43
			inner join
				(select 
					distinct vt.IIN as IIN 
				from 
					(select
					pers.P_PERSONIN as IIN,
				cast(concat(sick.P_CAT, sick.P_CODE, if(sick.P_SUBCODE is null, '', concat('.', sick.P_SUBCODE))) as String) as MKB
					from MZ_ERSB.MZ_T_INPATIENTS as inp
					inner join MZ_ERSB.MZ_T_PERSON as pers on pers.P_ID = inp.P_ID 
					inner join MZ_ERSB.MZ_T_HS_DIAGNOSES as diag on diag.P_PERSONREGISTER = inp.P_ID
					inner join MZ_ERSB.MZ_T_HSICK as sick on sick.P_ID = diag.P_SICK
					where 
					diag.P_ID = inp.P_ENDDIAG and
					diag.P_DIAGNOSISTYPE = 5 and
					diag.P_DIAGNOSTYPE = 2 and
					sick.P_PARENT <> 0 and
					inp.P_DOCTYPE = 1 and
					pers.P_PERSONIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
					pers.P_PERSONIN is not null) as vt
				where vt.MKB between 'H60' and 'H95.9' or
					  vt.MKB in ('F80.1', 'F80.2') or
					  vt.MKB between 'F80' and 'F89.9'
				) as n44_45
			on n43.IIN = n44_45.IIN) as p8
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p8.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL

	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr49' as filtr, -- информация о домашнем хозяйстве (животноводство, птицеводство, рыбоводство, растениеводство), о сельскохозяйственной технике 
		if(count(p2.IIN) > 0, 1, 0) as filtr_value
	from
		(select
			distinct mz.IIN 
		from DM_ZEROS.MSH_ZEROS as mz 
		where mz.IIN is not null and
			  mz.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
			  mz.cnt_grst_iin > 0) as p2
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p2.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
		'filtr50' as filtr, -- единая база данных идентификации с/х животных (ИС ИЖС)
		if(count(p3.IIN) > 0, 1, 0) as filtr_value
	from
		(select
			distinct lph.OWNER as IIN 
		from MSH_ISZH.LPH_CASE as lph 
		where lph.OWNER is not null and
			  lph.OWNER <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
			  lph.KOLVO > 0 and
			  lph.SUMMA > 0) as p3
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p3.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr51' as filtr, -- информация по малообеспеченным гражданам
		if(count(p30.IIN) > 0, 1, 0) as filtr_value
	from
		(select
			distinct asp.IIN as IIN 
		from SK_FAMILY.MTZSN_FAMILTY_ASP as asp
		where asp.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and
			  asp.IIN is not null and
			  asp.counter = (select max(counter) from SK_FAMILY.MTZSN_FAMILTY_ASP)
		) as p30
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p30.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr52' as filtr, -- многодетная мать (более 4х детей менее 18 лет)
		if(count(p37.IIN) > 0, 1, 0) as filtr_value
	from
		(select
			distinct pers.IIN as IIN
		from 
			(select distinct 
				pers.SICID as SICID,
				pers.IIN as IIN
			from
				(select 
					SICID,
					upper(RN) as IIN,
					row_number() over (partition by RN order by REGDATE desc) as num
				from MTSZN_MAIN.C_SDU_PERSON
				where RN is not null and
					  upper(RN) <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8') as pers
			where pers.num = 1) as pers
			inner join
				(select distinct
					doc.PNCD_ID as SICID,
					doc.RFPM_ID as RFPM_ID
				from MTSZN_SOLIDARY.C_SDU_PNPD_DOCUMENT as doc
				inner join MTSZN_SOLIDARY.SR_SOURCE as sr on sr.CODE = doc.RFPM_ID
				where toYear(toDateTimeOrNull(doc.PNCP_DATE)) = 2023 and 
					  sr.CODE in ('01051101', '01051102', '01051103', '01051104', '01051105', '01051106', '01051107', '01051108')) as doc
			on pers.SICID = doc.SICID) as p37
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p37.IIN
	group by toString(fm.SK_FAMILY_ID)

	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr53' as filtr, -- лица предпенсионного возраста (за 2 года до выхода на пенсию)
		if(count(p41.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			distinct gp.IIN as IIN
		from
			(select 
				IIN,
				BIRTH_DATE,
				date_diff(year, toDateTime64(BIRTH_DATE, 0), today()) as age_year,
				date_diff(month, toDateTime64(BIRTH_DATE, 0), today()) as age_month,
				SEX_ID
			from MU_FL.GBL_PERSON
			where PERSON_STATUS_ID <> 3) as gp
		where
			case 
				when gp.SEX_ID = 1 then if(gp.age_year = 63 - 2, 1, 0)
				when gp.SEX_ID = 2 then 
					case 
						when toYear(today()) = 2023 then if(gp.age_month = (61   * 12) - (2 * 12), 1, 0)
						when toYear(today()) = 2024 then if(gp.age_month = (61.5 * 12) - (2 * 12), 1, 0)
						when toYear(today()) = 2025 then if(gp.age_month = (62   * 12) - (2 * 12), 1, 0)
						when toYear(today()) = 2026 then if(gp.age_month = (62.5 * 12) - (2 * 12), 1, 0)
						when toYear(today()) = 2027 then if(gp.age_month = (63   * 12) - (2 * 12), 1, 0)
					end
			end = 1
		except
				select
					distinct pers.IIN as IIN 
				from 
					(select distinct
						pers.SICID as SICID,
						pers.IIN as IIN
					from
						(select 
							SICID,
							upper(RN) as IIN,
							row_number() over (partition by RN order by REGDATE desc) as num
						from MTSZN_MAIN.C_SDU_PERSON
						where RN is not null and
						upper(RN) <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8') as pers
					where pers.num = 1) as pers
					inner join
							(select distinct
								doc.PNCD_ID as SICID,
								doc.RFPM_ID as RFPM_ID
							from MTSZN_SOLIDARY.C_SDU_PNPD_DOCUMENT as doc
							inner join MTSZN_SOLIDARY.SR_SOURCE as sr on sr.CODE = doc.RFPM_ID
							where toYear(toDateTimeOrNull(doc.PNCP_DATE)) = 2023 and 
							sr.CODE in ('08', '0800', '08000001')) as doc
							on pers.SICID = doc.SICID) as p41	
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p41.IIN
	group by toString(fm.SK_FAMILY_ID)
	
	UNION ALL
	
	select
		toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
		'filtr54' as filtr, -- молодежь NEET (социальный статус не присвоен)
		if(count(p42.IIN) > 0, 1, 0) as filtr_value
	from
		(select 
			neet.IIN as IIN
		from NEET_YOUTH.PEOPLE_NEET as neet
		where neet.PERSON_AGE between 14 and 34 and
			  neet.IS_YOUNG_STUDENT = 1 and 
			  neet.IS_UCHRED = 'Не является учредителем ЮЛ' and 
			  neet.IS_IP = 'Отсутствует ИП' and 
			  neet.IS_GRST = 'Отсутсвует КХ/ФХ' and 
			  neet.IS_OSMS = 'Отсутствует в списке плательщиков ОСМС' and 
			  neet.IS_ESP = 'Не является плательщиком ЕСП' and 
			  neet.IS_OPV_2MONTH = 'Отсутствуют налоговые отчисления ОПВ последние 2 месяца подряд' and 
			  neet.IS_BEZRAB = 'Отсутствует в базе данных официальных безработных') as p42
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p42.IIN
	group by toString(fm.SK_FAMILY_ID)) as filtr35_54
group by SK_FAMILY_ID;
