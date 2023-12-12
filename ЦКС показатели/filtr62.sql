/* 2. Получение информации по кадастровым номерам и техническим характеристикам земельного участка */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr62' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p1.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select distinct
		ekgn.IIN as IIN
	from DM_ZEROS.DM_MU_RN_PROPERTY_IIN_RNN as ekgn
	where TYPE_OF_PROPERTY = 14175897 /* Земельный участок */) as p1
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p1.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);