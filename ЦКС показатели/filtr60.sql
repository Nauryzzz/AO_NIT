/* 7. Реестр алиментщиков (добровольные и принудительные) */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
	'filtr60' as filtr, -- реестр алиментщиков (добровольные и принудительные)
	if(count(distinct DEBTOR_IIN) > 0, 1, 0) as filtr_value
from AIS_OIP.AIS_OIP_ALIMENTSCHIKI as alim
	inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = alim.DEBTOR_IIN
group by toString(fm.SK_FAMILY_ID);