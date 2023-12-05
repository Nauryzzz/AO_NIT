/* 38. Жители сельской местности */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr44' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p9.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from SK_FAMILY.VILLAGE_IIN as p9 -- витрина со списком людей, которые живут в селе
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p9.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);