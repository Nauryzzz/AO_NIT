/* 42. Молодежь NEET (социальный статус не присвоен) */
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID, -- ID семьи
	'filtr54' as filtr, -- необходимо для определения значений текущего показателя при UNION ALL
	if(count(p42.IIN) > 0, 1, 0) as filtr_value -- если в семье есть хоть один подходящий ИИН, то признак будет 1 иначе 0
from
	(select -- список молодежи категории NEET
		neet.IIN as IIN
	from 
		NEET_YOUTH.PEOPLE_NEET as neet
	where 
		neet.PERSON_AGE between 14 and 34 and
		neet.IS_YOUNG_STUDENT = 1 and 
		neet.IS_UCHRED = 'Не является учредителем ЮЛ' and 
		neet.IS_IP = 'Отсутствует ИП' and 
		neet.IS_GRST = 'Отсутсвует КХ/ФХ' and 
		neet.IS_OSMS = 'Отсутствует в списке плательщиков ОСМС' and 
		neet.IS_ESP = 'Не является плательщиком ЕСП' and 
		neet.IS_OPV_2MONTH = 'Отсутствуют налоговые отчисления ОПВ последние 2 месяца подряд' and 
		neet.IS_BEZRAB = 'Отсутствует в базе данных официальных безработных') as p42
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p42.IIN -- определение ID семьи для ИИН
group by toString(fm.SK_FAMILY_ID);