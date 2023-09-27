/* 22. Взрослое население, зависимое от ПАВ */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, /* ID семьи */
	'filtr38' as filtr, /* название показателя, необходимо для идентификации значений текущего показателя при объедений */
	if(count(p22.IIN) > 0, 1, 0) as filtr_value /* если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0 */
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
			where date_diff(year, toDate(gp.BIRTH_DATE), today()) > 18) as n105 /* люди от 18 лет */
		inner join /* объединение людей от 18 лет с людьми с зависимостью от ПАВ */
			(select 
				distinct h.IIN as IIN
			from MZ_ERDB.HUMAN as h
				inner join MZ_ERDB.HUMAN_DIAG as hd on hd.HUMAN_UID = h.UID
			where hd.ICD10 between 'F10' and 'F19.9') as n106_107 /* люди с зависимостью от ПАВ */
		on n105.IIN = n106_107.IIN
	except /* исключить из списка людей от 18 лет с зависимостью от ПАВ людей направленных на принудительное лечение */
		select 
			distinct cc.defendant as IIN /* люди направленные на принудительное лечение по решению суда */
		from SUPREME_COURT.COURTS_CASES as cc
		where cat = 2 and category = '142080004600000000') as vt 
	) as p22
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p22.IIN /* определение ID семьи для ИИН */
group by toString(fm.SK_FAMILY_ID)