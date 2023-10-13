/* 23. Семья, один из членов семьи которой зависим от ПАВ с принудительным решением суда */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr39' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p23.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select 
		distinct vt1.IIN as IIN
	from
		(select 
			distinct n109.IIN as IIN
		from
			(select -- люди от 18 лет
				distinct gp.IIN as IIN
			from MU_FL.GBL_PERSON as gp
			where 
				date_diff(year, toDate(gp.BIRTH_DATE), today()) > 18 and
				gp.PERSON_STATUS_ID <> 3 /* признак: не мертв */) as n109 
		inner join -- объединение людей от 18 лет с людьми с прикреплением к поликлинике
			(select -- прикрепленные к поликлинике
				distinct p.IIN as IIN
			from MZ_RPN.PERSON as p
				inner join MZ_RPN.ATTACHMENTS as att on att.PERSONID = p.ID 
			where att.ENDDATE is null and p.DEATHDATE is null and 
				p.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
				p.IIN is not null) as n_111
		on n109.IIN = n_111.IIN
		inner join -- объединение прикрепленных к поликлинике людей от 18 лет с людьми с зависимостью от ПАВ
			(select -- люди с зависимостью от ПАВ
				distinct h.IIN as IIN
			from MZ_ERDB.HUMAN as h
				inner join MZ_ERDB.HUMAN_DIAG as hd on hd.HUMAN_UID = h.UID
			where hd.ICD10 between 'F10' and 'F19.9') as n_112
		on n109.IIN = n_112.IIN
			except -- исключение беременных женщин
		select -- беременные женщины
			distinct h.IIN as IIN
		from MZ_REGISTERS_BASE.HUMAN as h 
			inner join MZ_REGISTERS_BASE.BER_KARTA bk on bk.HUMAN_UID = h.UID 
		where h.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
			h.IIN is not null) as vt1
	inner join -- объденинение с людьми направленными на принудительное лечение
		(select -- люди направленные на принудительное лечение по решению суда
			distinct cc.defendant as IIN
		from SUPREME_COURT.COURTS_CASES as cc
		where cat = 2 and category = '142080004600000000') as n_114
	on vt1.IIN = n_114.IIN
	except -- исключение людей с инвалидностью
		select -- люди с инвалидностью (1-2 группа)
			distinct pi.RN as IIN
		from MTSZN_CBDIAPP.PATIENT_INFO as pi
		where pi.INV_GROUP in (1, 2) and 
			toDate(pi.INV_ENDDATE) >= today()) as p23
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p23.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID)