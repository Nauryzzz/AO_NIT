/* 27. Регистрация акта о несчастном случае на производстве */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
	'filtr41' as filtr, -- регистрация акта о несчастном случае на производстве
	if(count(p27.IIN) > 0, 1, 0) as filtr_value
from
	(select
		distinct pc.CODE_IIN as IIN
	from MTSZN_LABORPROTECT.PA_CARD as pc
		inner join MTSZN_LABORPROTECT.N1 as n1 on n1.PA_CARD_ID = pc.PA_CARD_ID 
	where n1.D_PHYSIO_STATE_ID in (1, 2, 3)) as p27
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p27.IIN
group by toString(fm.SK_FAMILY_ID)
