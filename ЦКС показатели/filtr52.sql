/* 37. Многодетная мать (более 4х детей менее 18 лет) */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr52' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p37.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select
		distinct pers.IIN as IIN
	from 
		(select distinct -- основной список людей МТСЗН
			pers.SICID as SICID,
			pers.IIN as IIN
		from
			(select 
				SICID,
				upper(RN) as IIN,
				row_number() over (partition by RN order by REGDATE desc) as num
			from MTSZN_MAIN.C_SDU_PERSON
			where 
				RN is not null and
				upper(RN) <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8') as pers
		where pers.num = 1 /* последняя измененная запись */) as pers
	inner join  -- объедение основного списка людей МТСЗН с людьми получающими пособие
		(select distinct -- список людей, получающих пособие как многодетные матери
			doc.PNCD_ID as SICID,
			doc.RFPM_ID as RFPM_ID
		from MTSZN_SOLIDARY.C_SDU_PNPD_DOCUMENT as doc
			inner join MTSZN_SOLIDARY.SR_SOURCE as sr on sr.CODE = doc.RFPM_ID
		where
			toYear(toDateTimeOrNull(doc.PNCP_DATE)) = toYear(now()) and 
			toMonth(toDateTimeOrNull(doc.PNCP_DATE)) between (toMonth(now() - interval 3 month)) and (toMonth(now() - interval 1 month)) and  
			sr.CODE in ('01051101', /* 10 и более дет.многодет. матер */
						'01051102', /* 9 детей многодетные матери */
						'01051103', /* 8 детей многодетные матери */
						'01051104', /* 7 детей многодетные матери */
						'01051105', /* 6 детей многодетные матери */
						'01051106', /* 5 детей многодетные матери */
						'01051107', /* 4 детей многодетные матери */
						'01051108'  /* 4-х и более детей при неполном стаже многодетные матери */)) as doc
	on pers.SICID = doc.SICID) as p37
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p37.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);