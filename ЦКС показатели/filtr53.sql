/* 41. Лица предпенсионного возраста (за 2 года до выхода на пенсию) */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr53' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p41.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select 
		distinct gp.IIN as IIN -- список людей предпенсионного возраста
	from
		(select 
			IIN,
			BIRTH_DATE,
			date_diff(year, toDateTime64(BIRTH_DATE, 0), today()) as age_year,
			date_diff(month, toDateTime64(BIRTH_DATE, 0), today()) as age_month,
			SEX_ID
		from MU_FL.GBL_PERSON
		where
			REMOVED = 0 and 
			(EXCLUDE_REASON_ID is null or EXCLUDE_REASON_ID = 1) and
			PERSON_STATUS_ID <> 3) as gp
	where
		case 
			when gp.SEX_ID = 1 then if(gp.age_year = 63 - 2, 1, 0)
			when gp.SEX_ID = 2 then 
				case 
					when toYear(today()) = 2023 then if(gp.age_month = (61   * 12) - (2 * 12), 1, 0)
					when toYear(today()) = 2024 then if(gp.age_month = (61.5 * 12) - (2 * 12), 1, 0)
					when toYear(today()) = 2025 then if(gp.age_month = (62   * 12) - (2 * 12), 1, 0)
					when toYear(today()) = 2026 then if(gp.age_month = (62.5 * 12) - (2 * 12), 1, 0)
					when toYear(today()) = 2027 then if(gp.age_month = (63   * 12) - (2 * 12), 1, 0)
				end
		end = 1
	except -- исключаем из списка людей предпенсионного возраста людей уже получающих пенсии
	select -- люди получающие пенсию
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
	inner join
		(select distinct -- список людей, получающих пенсию
			doc.PNCD_ID as SICID,
			doc.RFPM_ID as RFPM_ID
		from MTSZN_SOLIDARY.C_SDU_PNPD_DOCUMENT as doc
			inner join MTSZN_SOLIDARY.SR_SOURCE as sr on sr.CODE = doc.RFPM_ID
		where
			toYear(toDateTimeOrNull(doc.PNCP_DATE)) = toYear(now()) and 
			toMonth(toDateTimeOrNull(doc.PNCP_DATE)) between (toMonth(now() - interval 3 month)) and (toMonth(now() - interval 1 month)) and 
			sr.CODE in ('08',       /* Базовая пенсионная выплата */
						'0800',     /* Базовая пенсионная выплата */
						'08000001', /* Базовая пенсия */)) as doc
	on pers.SICID = doc.SICID) as p41	
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p41.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);