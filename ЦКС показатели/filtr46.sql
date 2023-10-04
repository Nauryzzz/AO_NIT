/* 36. Молодежь */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, /* ID семьи */
	'filtr46' as filtr, /* название показателя, необходимо для идентификации значений текущего показателя при объедений */
	if(count(p36.IIN) > 0, 1, 0) as filtr_value /* если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0 */
from
	(select 
		distinct gp.IIN as IIN /* список людей от 16 до 35 лет */
	from MU_FL.GBL_PERSON as gp
	where date_diff(year, toDate(gp.BIRTH_DATE), today()) >= 16 and 
		  date_diff(year, toDate(gp.BIRTH_DATE), today()) <= 35 and
		  gp.PERSON_STATUS_ID <> 3 /* признак: не мертв */) as p36
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p36.IIN /* определение ID семьи для ИИН */
group by toString(fm.SK_FAMILY_ID)