/* 2. Информация о домашнем хозяйстве (животноводство, птицеводство, рыбоводство, растениеводство), о сельскохозяйственной технике */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr49' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p2.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select -- список людей с с/х техникой
		distinct mz.IIN 
	from DM_ZEROS.MSH_ZEROS as mz 
	where 
		mz.IIN is not null and
		mz.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		mz.cnt_grst_iin > 0 /* кол. с/х техники */) as p2
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p2.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);