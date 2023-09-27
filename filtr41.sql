/* 27. Регистрация акта о несчастном случае на производстве */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, /* ID семьи */
	'filtr41' as filtr, /* название показателя, необходимо для идентификации значений текущего показателя при объедений */
	if(count(p27.IIN) > 0, 1, 0) as filtr_value /* если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0 */
from
	(select
		distinct pc.CODE_IIN as IIN
	from MTSZN_LABORPROTECT.PA_CARD as pc
		inner join MTSZN_LABORPROTECT.N1 as n1 on n1.PA_CARD_ID = pc.PA_CARD_ID 
	where 
		n1.D_PHYSIO_STATE_ID in (1, /* Легкой степени тяжести */ 
								 2, /* Средней формы тяжести */ 
								 3  /* Тяжелой формы тяжести */)
	) as p27 /* несчастные случаи на производстве */
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p27.IIN /* определение ID семьи для ИИН */
group by toString(fm.SK_FAMILY_ID)
