/* 22. Взрослое население, зависимое от ПАВ */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
	'filtr38' as filtr,
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