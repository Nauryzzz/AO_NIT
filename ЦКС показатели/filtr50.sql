/* 3. Единая база данных идентификации с/х животных (ИС ИЖС) */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr50' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p3.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select -- список людей, имеющих с/х животных
		distinct lph.OWNER as IIN 
	from MSH_ISZH.LPH_CASE as lph 
	where 
		lph.OWNER is not null and
		lph.OWNER <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		lph.KOLVO > 0 /* количество животных */ and
		lph.SUMMA > 0 /* стоимость  животных */) as p3
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p3.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID)