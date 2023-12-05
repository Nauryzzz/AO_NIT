/* 30. Информация по малообеспеченным гражданам */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr51' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p30.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select -- список людей, получающих АСП
		distinct asp.IIN as IIN 
	from SK_FAMILY.MTZSN_FAMILTY_ASP as asp
	where 
		asp.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and
		asp.IIN is not null and
		asp.counter = (select max(counter) from SK_FAMILY.MTZSN_FAMILTY_ASP) -- выбор последнего квартала
	) as p30
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p30.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);