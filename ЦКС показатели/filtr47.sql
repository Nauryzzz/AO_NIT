/* 13. Наличие членов семьи с количеством дней просрочки выплаты по кредиту 90+ дней */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, /* ID семьи */
	'filtr47' as filtr, /* название показателя, необходимо для идентификации значений текущего показателя при объедений */
	if(count(p13.IIN) > 0, 1, 0) as filtr_value /* если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0 */
from
	(select 
		distinct n57.IIN as IIN /* список людей от 18 лет */
	from
		(select 
			distinct gp.IIN as IIN
		from MU_FL.GBL_PERSON as gp
		where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 18 and
			  gp.PERSON_STATUS_ID <> 3 /* признак: не мертв */) as n57
	
	inner join /* объединение людей от 18 лет с людьми с просрочкой по кредиту */
	
		(select
			distinct g.HASH_IIN as IIN /* список людей с просрочкой по кредиту */
		from SK_FAMILY.GKB as g
		where 
			g.PAYMENT_DAYS_OVERDUE > 90 /* просрочка больше 90 дней */ and 
			g.DEBT_PASTDUE_VALUE > 1000 /* сумма просрочки большее 1000 тг. */) as n58 
	on n57.IIN = n58.IIN) as p13
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p13.IIN /* определение ID семьи для ИИН */
group by toString(fm.SK_FAMILY_ID)