/* 36. Молодежь */
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