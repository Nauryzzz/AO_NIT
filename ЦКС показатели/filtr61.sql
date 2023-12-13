/* 25. Информация о зарегистрированных безработных граждан */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr61' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p25.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select -- люди, которые официально зарегистрированы как безработные
		distinct pers.IIN as IIN
	from
		(select distinct
			card.PA_CARD_ID as PA_CARD_ID,
			card.CODE_IIN as IIN
		from
			(select 
				PA_CARD_ID,
				upper(CODE_IIN) as CODE_IIN,
				row_number() over (partition by CODE_IIN order by SDU_LOAD_IN_DT desc) as num
			from MTSZN_EHALYK.C_HSDU_PA_CARD as pc
			where 
				pc.CODE_IIN is not null and
				upper(pc.CODE_IIN) <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8') as card
		where card.num = 1) as pers
	inner join 
		(select  
			distinct PA_CARD_ID
		from MTSZN_EHALYK.C_HSDU_ENROLLMENT
		where enr.DATE_CLOSE = '0000-00-00') as enr
	on pers.PA_CARD_ID = enr.PA_CARD_ID) as p25
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p25.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);