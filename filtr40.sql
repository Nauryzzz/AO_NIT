/* 21. Несовершеннолетние дети, зависимые от ПАВ */
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
				  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 18) as n101
		inner join
			(select 
				distinct h.IIN as IIN
			from MZ_ERDB.HUMAN as h
				inner join MZ_ERDB.HUMAN_DIAG as hd on hd.HUMAN_UID = h.UID
			where hd.ICD10 between 'F10' and 'F19.9') as n102_103
		on n101.IIN = n102_103.IIN) as vt) as p21
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p21.IIN
group by toString(fm.SK_FAMILY_ID)