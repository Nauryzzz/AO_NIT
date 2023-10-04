/* 38. Жители сельской местности */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
	'filtr44' as filtr, -- жители сельской местности
	if(count(p9.IIN) > 0, 1, 0) as filtr_value
from SK_FAMILY.VILLAGE_IIN as p9
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p9.IIN
group by toString(fm.SK_FAMILY_ID)