/* 8. Ребенок, отстающий в психофизическом развитии */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr48' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p8.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select 
		distinct n43.IIN
	from
		(select -- дети до 18 лет
			distinct gp.IIN as IIN
		from MU_FL.GBL_PERSON as gp
		where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 0 and 
			date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18 and
			gp.REMOVED = 0 and 
			(gp.EXCLUDE_REASON_ID is null or gp.EXCLUDE_REASON_ID = 1) and
			gp.PERSON_STATUS_ID <> 3 /* признак: не мертв */) as n43
	inner join -- объединение детей до 18 лет с детьми с диагнозами...
		(select -- список детей с диагнозами...
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
				diag.P_ID = inp.P_ENDDIAG and -- ID записи с заключительным диагнозом
				diag.P_DIAGNOSISTYPE = 5 and -- Заключительный
				diag.P_DIAGNOSTYPE = 2 and -- Основное
				sick.P_PARENT <> 0 and -- Код МКБ не является общей категорией
				inp.P_DOCTYPE = 1 and -- медицинская карта стационарного больного
				pers.P_PERSONIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
				pers.P_PERSONIN is not null) as vt
		where
			vt.MKB between 'H60' and 'H95.9' or -- Тугоухость
			vt.MKB in ('F80.1', 'F80.2') or -- Задержка развития речи
			vt.MKB between 'F80' and 'F89.9' -- Задержка психического развития
		) as n44_45
	on n43.IIN = n44_45.IIN) as p8
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p8.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);