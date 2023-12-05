/* 21. Несовершеннолетние дети, зависимые от ПАВ */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr40' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p21.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select 
		distinct vt.IIN as IIN
	from
		(select 
			distinct n101.IIN as IIN
		from
			(select -- дети от 7 до 18 лет
				distinct gp.IIN as IIN
			from MU_FL.GBL_PERSON as gp
			where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 7 and 
				date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18 and
				gp.REMOVED = 0 and 
				(gp.EXCLUDE_REASON_ID is null or gp.EXCLUDE_REASON_ID = 1) and
				gp.PERSON_STATUS_ID <> 3 /* признак: не мертв */) as n101
		inner join -- объединение детей от 7 до 18 лет с людьми с зависимостью от ПАВ
			(select -- люди с зависимостью от ПАВ
				distinct h.IIN as IIN
			from MZ_ERDB.HUMAN as h
				inner join MZ_ERDB.HUMAN_DIAG as hd on hd.HUMAN_UID = h.UID
			where hd.ICD10 between 'F10' and 'F19.9') as n102_103
		on n101.IIN = n102_103.IIN) as vt) as p21
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p21.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);