/* 7. Реестр алиментщиков (добровольные и принудительные) */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr60' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(distinct DEBTOR_IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from AIS_OIP.AIS_OIP_ALIMENTSCHIKI as alim -- список алиментщиков
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = alim.DEBTOR_IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);