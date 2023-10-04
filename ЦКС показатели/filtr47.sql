/* 13. Наличие членов семьи с количеством дней просрочки выплаты по кредиту 90+ дней */
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
		where g.PAYMENT_DAYS_OVERDUE > 90 and g.DEBT_PASTDUE_VALUE > 1000) as n58
	on n57.IIN = n58.IIN) as p13
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p13.IIN
group by toString(fm.SK_FAMILY_ID)