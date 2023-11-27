DROP TABLE IF EXISTS DM_ANALYTICS.OPROS_MSH_POKAZATEL2;

CREATE TABLE DM_ANALYTICS.OPROS_MSH_POKAZATEL2
(
    KATO_2 		Nullable(String),
    KATO_2_NAME	Nullable(String),
    KATO_4 		Nullable(String),
    KATO_4_NAME Nullable(String),
    KATO_6 		Nullable(String),
    KATO_6_NAME Nullable(String),
    NUM Int32,
    NUM2 String,
    KATEGORIYA  Nullable(String),
    POKAZATEL 	String,
    VALUE 		Nullable(Float64),
    EDIZM 		Nullable(String),
    VALUE_SDU 	Nullable(Float64),
    RAZNICA 	Nullable(Float64),
    SPECIALIZATION Nullable(String)
)
ENGINE = MergeTree
ORDER BY POKAZATEL
SETTINGS index_granularity = 8192;

insert into 
	DM_ANALYTICS.OPROS_MSH_POKAZATEL2 (KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, KATO_6, KATO_6_NAME, 
									  NUM, NUM2, KATEGORIYA, POKAZATEL, VALUE, EDIZM, 
									  VALUE_SDU, RAZNICA, SPECIALIZATION)
select 
	ifNull(p.KATO_2, 	  '(нет данных)'),
	ifNull(p.KATO_2_NAME, '(нет данных)'),
	ifNull(p.KATO_4, 	  '(нет данных)'),
	ifNull(p.KATO_4_NAME, '(нет данных)'),
	ifNull(p.KATO_6, 	  '(нет данных)'), 
	ifNull(p.KATO_6_NAME, '(нет данных)'),
	p.NUM,
	p.NUM2,
	p.KATEGORIYA,
	p.POKAZATEL,
	p.VALUE,
	p.EDIZM,
	s.VALUE as VALUE_SDU,
	round(case 
			when p.VALUE <= s.VALUE and p.VALUE > 0 then (s.VALUE - p.VALUE) / p.VALUE * 100
			when p.VALUE > s.VALUE and p.VALUE > 0 then (p.VALUE - s.VALUE) / p.VALUE * 100
			when p.VALUE = 0 and s.VALUE > 0 then 100
			when s.VALUE = 0 and p.VALUE > 0 then 100
			else 0
		end, 2) as RAZNICA,
	SPECIALIZATION
from 
	(select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		1 as NUM,
		'1' as NUM2,
		'Демография, занятость и уровень жизни населения' as KATEGORIYA,
		'Численность населения' as POKAZATEL,
		labour_population as VALUE,
		'человек' as EDIZM,
		null as SPECIALIZATION
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		2,
		'1.1',
		'Демография, занятость и уровень жизни населения' as KATEGORIYA,
		'из них постоянно проживающих' as POKAZATEL,
		labour_constant_population as VALUE,
		'человек' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		3,
		'2',
		'Демография, занятость и уровень жизни населения' as KATEGORIYA,
		'Рабочая сила' as POKAZATEL,
		labour_labour as VALUE,
		'человек' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		4,
		'3',
		'Демография, занятость и уровень жизни населения' as KATEGORIYA,
		'Занятое население в бюджетной сфере' as POKAZATEL,
		labour_goverment_workers as VALUE,
		'человек' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		5,
		'4',
		'Демография, занятость и уровень жизни населения' as KATEGORIYA,
		'Занятое население в частном секторе' as POKAZATEL,
		labour_private_labour as VALUE,
		'человек' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		6,
		'4.1',
		'Демография, занятость и уровень жизни населения' as KATEGORIYA,
		'из них занятое население в личном подсобном хозяйстве' as POKAZATEL,
		labour_private_ogorod as VALUE,
		'человек' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		7,
		'5',
		'Демография, занятость и уровень жизни населения' as KATEGORIYA,
		'лица, не входящие в состав рабочей силы' as POKAZATEL,
		labour_total_econ_inactive_population as VALUE,
		'человек' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		8,
		'6',
		'Демография, занятость и уровень жизни населения' as KATEGORIYA,
		'Безработные' as POKAZATEL,
		labour_unemployed as VALUE,
		'человек' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		9,
		'7',
		'Демография, занятость и уровень жизни населения' as KATEGORIYA,
		'Средний размер домашних хозяйств' as POKAZATEL,
		labour_household_size as VALUE,
		'человек' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		10,
		'8',
		'Демография, занятость и уровень жизни населения' as KATEGORIYA,
		'Средний доход на одну семью, в месяц' as POKAZATEL,
		labour_average_income_family as VALUE,
		'тенге/месяц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
---------------------------------------------------------------------------
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		11,
		'1',
		'Жилищные условия' as KATEGORIYA,
		'Общее количество дворов (частные дома, квартиры в многоквартирном доме, точки чабана, иное)' as POKAZATEL,
		house_total_dvor as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		12,
		'1.1',
		'Жилищные условия' as KATEGORIYA,
		'из них количество заселенных дворов' as POKAZATEL,
		house_zaselen_dvor as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
---------------------------------------------------------------------------		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		13,
		'1',
		'Растениеводство' as KATEGORIYA,
		'Количество домашних хозяйств' as POKAZATEL,
		dh_count as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		14,
		'2',
		'Растениеводство' as KATEGORIYA,
		'Количество домашних хозяйств имеющих участки (огороды, сады, приусадебные участки)' as POKAZATEL,
		dx_number_ogorodov as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		15,
		'3',
		'Растениеводство' as KATEGORIYA,
		'Сельскохозяйственные угодья домашних хозяйств' as POKAZATEL,
		dx_cx_land as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		16,
		'3.1',
		'Растениеводство' as KATEGORIYA,
		'пашня' as POKAZATEL,
		dx_pashnya as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		17,
		'3.2',
		'Растениеводство' as KATEGORIYA,
		'многолетние насаждения' as POKAZATEL,
		dx_mnogoletnie as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		18,
		'3.3',
		'Растениеводство' as KATEGORIYA,
		'залежь' as POKAZATEL,
		dx_zelej as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		19,
		'3.4',
		'Растениеводство' as KATEGORIYA,
		'пастбища' as POKAZATEL,
		dx_pastbishe as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		20,
		'3.5',
		'Растениеводство' as KATEGORIYA,
		'сенокосы' as POKAZATEL,
		dx_senokosy as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		21,
		'3.6',
		'Растениеводство' as KATEGORIYA,
		'огороды' as POKAZATEL,
		dx_ogorody as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		22,
		'3.7',
		'Растениеводство' as KATEGORIYA,
		'сады' as POKAZATEL,
		dx_sad as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		23,
		'4',
		'Растениеводство' as KATEGORIYA,
		'Объем урожая сельскохозяйственных культур в домашних хозяйствах, всего' as POKAZATEL,
		dx_urojai as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		24,
		'4.1',
		'Растениеводство' as KATEGORIYA,
		'огурцы' as POKAZATEL,
		dx_cucumber as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		25,
		'4.2',
		'Растениеводство' as KATEGORIYA,
		'томаты' as POKAZATEL,
		dx_tomato as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		26,
		'4.3',
		'Растениеводство' as KATEGORIYA,
		'картофель' as POKAZATEL,
		dx_potato as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		27,
		'4.4',
		'Растениеводство' as KATEGORIYA,
		'капуста' as POKAZATEL,
		dx_kapusta as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		28,
		'4.5',
		'Растениеводство' as KATEGORIYA,
		'морковь' as POKAZATEL,
		dx_carrot as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		29,
		'4.6',
		'Растениеводство' as KATEGORIYA,
		'свекла' as POKAZATEL,
		dx_svekla as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		30,
		'4.7',
		'Растениеводство' as KATEGORIYA,
		'сладкий перец' as POKAZATEL,
		dx_sweet_peper as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		31,
		'4.8',
		'Растениеводство' as KATEGORIYA,
		'баклажаны' as POKAZATEL,
		dx_baklajan as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		32,
		'4.9',
		'Растениеводство' as KATEGORIYA,
		'кабачки' as POKAZATEL,
		dx_kabachek as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		33,
		'4.10',
		'Растениеводство' as KATEGORIYA,
		'лук' as POKAZATEL,
		dx_onion as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		34,
		'4.11',
		'Растениеводство' as KATEGORIYA,
		'чеснок' as POKAZATEL,
		dx_chesnok as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		35,
		'4.12',
		'Растениеводство' as KATEGORIYA,
		'редис' as POKAZATEL,
		dx_redis as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		36,
		'4.13',
		'Растениеводство' as KATEGORIYA,
		'кормовые культуры' as POKAZATEL,
		dx_korm as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
				
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		37,
		'4.14',
		'Растениеводство' as KATEGORIYA,
		'культуры многолетние (плодовые, ягодные насаждения)' as POKAZATEL,
		dx_fruits as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
				
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		38,
		'5',
		'Растениеводство' as KATEGORIYA,
		'Количество крестьянских хозяйств' as POKAZATEL,
		kx_amount as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		39,
		'6',
		'Растениеводство' as KATEGORIYA,
		'Сельскохозяйственные угодья крестьянских хозяйств' as POKAZATEL,
		kz_cx_land as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		40,
		'6.1',
		'Растениеводство' as KATEGORIYA,
		'пашня' as POKAZATEL,
		kx_pansya as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		41,
		'6.2',
		'Растениеводство' as KATEGORIYA,
		'многолетние насаждения' as POKAZATEL,
		kx_mnogoletnie as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		42,
		'6.3',
		'Растениеводство' as KATEGORIYA,
		'залежь' as POKAZATEL,
		kx_zelej as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		43,
		'6.4',
		'Растениеводство' as KATEGORIYA,
		'пастбища' as POKAZATEL,
		kx_pastbishe as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		44,
		'6.5',
		'Растениеводство' as KATEGORIYA,
		'сенокосы' as POKAZATEL,
		kx_senokosy as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		45,
		'7',
		'Растениеводство' as KATEGORIYA,
		'Объем урожая сельскохозяйственных культур в крестьянских хозяйствах, всего' as POKAZATEL,
		kx_urojai as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		46,
		'7.1',
		'Растениеводство' as KATEGORIYA,
		'зерновые и бобовые всех видов' as POKAZATEL,
		kx_zerno as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		47,
		'7.2',
		'Растениеводство' as KATEGORIYA,
		'рис' as POKAZATEL,
		kx_rice as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		48,
		'7.3',
		'Растениеводство' as KATEGORIYA,
		'масличные всех видов' as POKAZATEL,
		kx_maslichnye as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		49,
		'7.4',
		'Растениеводство' as KATEGORIYA,
		'кормовые культуры' as POKAZATEL,
		kx_korm as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		50,
		'7.5',
		'Растениеводство' as KATEGORIYA,
		'культуры многолетние (плодовые деревья, кустарники)' as POKAZATEL,
		kx_mnogoletnie_derevo as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		51,
		'8',
		'Растениеводство' as KATEGORIYA,
		'Резерв увеличения земельных угодий за счет фонда неиспользуемых и/или изъятых земель' as POKAZATEL,
		kz_cx_land_reserve as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		52,
		'8.1',
		'Растениеводство' as KATEGORIYA,
		'пашня (резерв)' as POKAZATEL,
		kx_pansya_reserve as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		53,
		'8.2',
		'Растениеводство' as KATEGORIYA,
		'многолетние насаждения (резерв)' as POKAZATEL,
		kx_mnogoletnie_reserve as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		54,
		'8.3',
		'Растениеводство' as KATEGORIYA,
		'залежь (резерв)' as POKAZATEL,
		kx_zelej_reserve as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		55,
		'8.4',
		'Растениеводство' as KATEGORIYA,
		'пастбища (резерв)' as POKAZATEL,
		kx_pastbishe_reserve as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		56,
		'8.5',
		'Растениеводство' as KATEGORIYA,
		'сенокосы (резерв)' as POKAZATEL,
		kx_senokosy_reserve as VALUE,
		'гектар' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		57,
		'9',
		'Растениеводство' as KATEGORIYA,
		'Обеспеченность водой для полива' as POKAZATEL,
		infrastructure_polivochnaya_sistema_ as VALUE,
		'наличие' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		58,
		'10',
		'Растениеводство' as KATEGORIYA,
		'Обеспеченность водой для полива' as POKAZATEL,
		infrastructure_polivochnaya_sistema_isused as VALUE,
		'используется' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		59,
		'11',
		'Растениеводство' as KATEGORIYA,
		'Процент покрытия поливом посевных площадей, %' as POKAZATEL,
		infrastructure_polivy as VALUE,
		'%' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		60,
		'12',
		'Растениеводство' as KATEGORIYA,
		'Здание МТМ (машино-тракторная мастерская)' as POKAZATEL,
		infrastructure_mtm as VALUE,
		'наличие' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		61,
		'13',
		'Растениеводство' as KATEGORIYA,
		'Здание МТМ (машино-тракторная мастерская)' as POKAZATEL,
		infrastructure_mtm_isused as VALUE,
		'используется' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		62,
		'14',
		'Растениеводство' as KATEGORIYA,
		'Склады для хранения сырья и готовой продукции' as POKAZATEL,
		infrastructure_slad as VALUE,
		'наличие' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		63,
		'15',
		'Растениеводство' as KATEGORIYA,
		'Склады для хранения сырья и готовой продукции' as POKAZATEL,
		infrastructure_slad_isused as VALUE,
		'используется' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		64,
		'16',
		'Растениеводство' as KATEGORIYA,
		'Гаражи, ангары для хранения с/х техники и автотранспорта' as POKAZATEL,
		infrastructure_garage as VALUE,
		'наличие' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		65,
		'17',
		'Растениеводство' as KATEGORIYA,
		'Гаражи, ангары для хранения с/х техники и автотранспорта' as POKAZATEL,
		infrastructure_garage_isused as VALUE,
		'используется' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		66,
		'18',
		'Растениеводство' as KATEGORIYA,
		'Цистерны для хранения ГСМ (горюче-смазочные материалы)' as POKAZATEL,
		infrastructure_cycsterny as VALUE,
		'наличие' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		67,
		'19',
		'Растениеводство' as KATEGORIYA,
		'Цистерны для хранения ГСМ (горюче-смазочные материалы)' as POKAZATEL,
		infrastructure_cycsterny_isused as VALUE,
		'используется' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		68,
		'20',
		'Растениеводство' as KATEGORIYA,
		'Трансформаторная электро-подстанция' as POKAZATEL,
		infrastructure_transformator as VALUE,
		'наличие' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		69,
		'21',
		'Растениеводство' as KATEGORIYA,
		'Трансформаторная электро-подстанция' as POKAZATEL,
		infrastructure_transformator_isused as VALUE,
		'используется' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		70,
		'22',
		'Растениеводство' as KATEGORIYA,
		'Специализация в растениеводстве' as POKAZATEL,
		0 as VALUE,
		'' as EDIZM,
		specialization_rastenivodstvo
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
---------------------------------------------------------------------------	
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		71,
		'1',
		'Животноводство' as KATEGORIYA,
		'Общее число дворов' as POKAZATEL,
		animal_dvor as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		72,
		'1.1',
		'Животноводство' as KATEGORIYA,
		'из них имеет скот и птицу' as POKAZATEL,
		animal_skot_bird as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		73,
		'2',
		'Животноводство' as KATEGORIYA,
		'Сельскохозяйственные угодья домашних хозяйств' as POKAZATEL,
		animal_cx_land as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		74,
		'2.1',
		'Животноводство' as KATEGORIYA,
		'пашня' as POKAZATEL,
		animal_pashnya as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		75,
		'2.2',
		'Животноводство' as KATEGORIYA,
		'многолетние насаждения' as POKAZATEL,
		animal_mnogolet as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		76,
		'2.3',
		'Животноводство' as KATEGORIYA,
		'залежь' as POKAZATEL,
		animal_zalej as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		77,
		'2.4',
		'Животноводство' as KATEGORIYA,
		'пастбища' as POKAZATEL,
		animal_pastbisha as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		78,
		'2.5',
		'Животноводство' as KATEGORIYA,
		'сенокосы' as POKAZATEL,
		animal_senokos as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		79,
		'2.6',
		'Животноводство' as KATEGORIYA,
		'огороды' as POKAZATEL,
		animal_ogorod as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		80,
		'2.7',
		'Животноводство' as KATEGORIYA,
		'сады' as POKAZATEL,
		animal_sad as VALUE,
		'кв.метров' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		81,
		'3',
		'Животноводство' as KATEGORIYA,
		'КРС молочный' as POKAZATEL,
		animal_krs_milk as VALUE,
		'голов' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		82,
		'4',
		'Животноводство' as KATEGORIYA,
		'КРС мясной' as POKAZATEL,
		animal_krs_meat as VALUE,
		'голов' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		83,
		'5',
		'Животноводство' as KATEGORIYA,
		'Овцы, бараны' as POKAZATEL,
		animal_sheep as VALUE,
		'голов' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		84,
		'6',
		'Животноводство' as KATEGORIYA,
		'Козы, козлы' as POKAZATEL,
		animal_kozel as VALUE,
		'голов' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		85,
		'7',
		'Животноводство' as KATEGORIYA,
		'Лошади' as POKAZATEL,
		animal_horse as VALUE,
		'голов' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		86,
		'8',
		'Животноводство' as KATEGORIYA,
		'Верблюды' as POKAZATEL,
		animal_camel as VALUE,
		'голов' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		87,
		'9',
		'Животноводство' as KATEGORIYA,
		'Свиньи' as POKAZATEL,
		animal_pig as VALUE,
		'голов' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		88,
		'10',
		'Животноводство' as KATEGORIYA,
		'Куры' as POKAZATEL,
		animal_chicken as VALUE,
		'голов' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		89,
		'11',
		'Животноводство' as KATEGORIYA,
		'Гуси' as POKAZATEL,
		animal_gusi as VALUE,
		'голов' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		90,
		'12',
		'Животноводство' as KATEGORIYA,
		'Утки' as POKAZATEL,
		animal_duck as VALUE,
		'голов' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		91,
		'13',
		'Животноводство' as KATEGORIYA,
		'Индюки' as POKAZATEL,
		animal_induk as VALUE,
		'голов' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		92,
		'14',
		'Животноводство' as KATEGORIYA,
		'Валовой надой молока, тонн в год, всего' as POKAZATEL,
		animal_mik_total as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		93,
		'14.1',
		'Животноводство' as KATEGORIYA,
		'валовой надой коровьего молока' as POKAZATEL,
		animal_milk_cow as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		94,
		'14.2',
		'Животноводство' as KATEGORIYA,
		'доля коровьего молока' as POKAZATEL,
		animal_milkrate_cow as VALUE,
		'%' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		95,
		'14.3',
		'Животноводство' as KATEGORIYA,
		'валовой надой козьего молока' as POKAZATEL,
		animal_mil_kozel as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		96,
		'14.4',
		'Животноводство' as KATEGORIYA,
		'доля козьего молока' as POKAZATEL,
		animal_milrate_kozel as VALUE,
		'%' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		97,
		'14.5',
		'Животноводство' as KATEGORIYA,
		'валовой надой кобыльего молока' as POKAZATEL,
		animal_milk_horse as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		98,
		'14.6',
		'Животноводство' as KATEGORIYA,
		'доля молока кобыльего молока' as POKAZATEL,
		animal_milkrate_horse as VALUE,
		'%' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		99,
		'14.7',
		'Животноводство' as KATEGORIYA,
		'валовой надой верблюжьего молока' as POKAZATEL,
		animal_milk_camel as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		100,
		'14.8',
		'Животноводство' as KATEGORIYA,
		'доля верблюжьего молока' as POKAZATEL,
		animal_milkrate_camel as VALUE,
		'%' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		101,
		'15',
		'Животноводство' as KATEGORIYA,
		'Валовый сбор мяса, тонн в год, всего' as POKAZATEL,
		animal_meat_total as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		102,
		'15.1',
		'Животноводство' as KATEGORIYA,
		'говядина' as POKAZATEL,
		animal_meat_cow as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		103,
		'15.2',
		'Животноводство' as KATEGORIYA,
		'баранина' as POKAZATEL,
		animal_meat_sheep as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		104,
		'15.3',
		'Животноводство' as KATEGORIYA,
		'конина' as POKAZATEL,
		animal_meat_horse as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		105,
		'15.4',
		'Животноводство' as KATEGORIYA,
		'свинина' as POKAZATEL,
		animal_meat_pig as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		106,
		'15.5',
		'Животноводство' as KATEGORIYA,
		'верблюжатина' as POKAZATEL,
		animal_meat_camel as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		107,
		'15.6',
		'Животноводство' as KATEGORIYA,
		'куринное' as POKAZATEL,
		animal_meat_chicken as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		108,
		'15.7',
		'Животноводство' as KATEGORIYA,
		'утиное' as POKAZATEL,
		animal_meat_duck as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		109,
		'15.8',
		'Животноводство' as KATEGORIYA,
		'гусиное' as POKAZATEL,
		animal_meat_gusi as VALUE,
		'тонн/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		110,
		'16',
		'Животноводство' as KATEGORIYA,
		'Валовый сбор яиц' as POKAZATEL,
		animal_egg_total as VALUE,
		'тыс. штук в год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		111,
		'16.1',
		'Животноводство' as KATEGORIYA,
		'яйца куриные' as POKAZATEL,
		animal_egg_chicken as VALUE,
		'штук/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op		
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		112,
		'16.2',
		'Животноводство' as KATEGORIYA,
		'яйца гусиные' as POKAZATEL,
		animal_egg_gusi as VALUE,
		'штук/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		113,
		'16.3',
		'Животноводство' as KATEGORIYA,
		'яйца перепелиновые' as POKAZATEL,
		animal_egg_perepel as VALUE,
		'штук/год' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		114,
		'17',
		'Животноводство' as KATEGORIYA,
		'Трансформаторная электро-подстанция' as POKAZATEL,
		animal_transformator as VALUE,
		'наличие' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		115,
		'18',
		'Животноводство' as KATEGORIYA,
		'Трансформаторная электро-подстанция' as POKAZATEL,
		animal_transformator_isused as VALUE,
		'используется' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		116,
		'19',
		'Животноводство' as KATEGORIYA,
		'Специализация в животноводстве' as POKAZATEL,
		0 as VALUE,
		'' as EDIZM,
		specialization_animal
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
---------------------------------------------------------------------------
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		117,
		'1',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Торговля и услуги' as POKAZATEL,
		0 as VALUE,
		'' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		118,
		'1.1',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Автосервис (СТО, шиномонтаж, замена автозапчастей и т.д.)' as POKAZATEL,
		noncx_sto as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		119,
		'1.2',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Автосервис (СТО, шиномонтаж, замена автозапчастей и т.д.)' as POKAZATEL,
		noncx_sto_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		120,
		'1.3',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Детские центры развития, репетиторские услуги, языковые курсы' as POKAZATEL,
		noncx_kindergarden as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		121,
		'1.4',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Детские центры развития, репетиторские услуги, языковые курсы' as POKAZATEL,
		noncx_kindergarden_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		122,
		'1.5',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Изготовление сувениров, украшений из различных материалов' as POKAZATEL,
		noncx_souvenier as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		123,
		'1.6',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Изготовление сувениров, украшений из различных материалов' as POKAZATEL,
		noncx_souvenier_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op	
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		124,
		'1.7',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Компьютерные услуги' as POKAZATEL,
		noncx_pc_service as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		125,
		'1.8',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Компьютерные услуги' as POKAZATEL,
		noncx_pc_service_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		126,
		'1.9',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Магазин (минимаркет, строительных материалов, автозапчастей, одежды и обуви, орг.техники, сотовых телефонов и акссесуаров и др.)' as POKAZATEL,
		noncx_store as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		127,
		'1.10',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Магазин (минимаркет, строительных материалов, автозапчастей, одежды и обуви, орг.техники, сотовых телефонов и акссесуаров и др.)' as POKAZATEL,
		noncx_store_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		128,
		'1.11',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Мастерская, услуги по ремонту бытовой техники, орг.техники, инструментов, замена картриджей и т.д.' as POKAZATEL,
		noncx_remont_bytovoi_tech as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		129,
		'1.12',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Мастерская, услуги по ремонту бытовой техники, орг.техники, инструментов, замена картриджей и т.д.' as POKAZATEL,
		noncx_remont_bytovoi_tech_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		130,
		'1.13',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Металлопластиковые изделия' as POKAZATEL,
		noncx_metal as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		131,
		'1.14',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Металлопластиковые изделия' as POKAZATEL,
		noncx_metal_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		132,
		'1.15',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Оказание профессиональных услуг - бухгалтерские, юридические, налоговые, маркетинг, реклама и т.д.' as POKAZATEL,
		noncx_accounting as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		133,
		'1.16',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Оказание профессиональных услуг - бухгалтерские, юридические, налоговые, маркетинг, реклама и т.д.' as POKAZATEL,
		noncx_accounting_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		134,
		'1.17',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Полиграфические услуги, фотосалон, услуги фото-видео съемки' as POKAZATEL,
		noncx_photo as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		135,
		'1.18',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Полиграфические услуги, фотосалон, услуги фото-видео съемки' as POKAZATEL,
		noncx_photo_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		136,
		'1.19',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Туризм (гостиницы, хостелы, кемпинги, турбазы)' as POKAZATEL,
		noncx_turism as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		137,
		'1.20',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Туризм (гостиницы, хостелы, кемпинги, турбазы)' as POKAZATEL,
		noncx_turism_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		138,
		'1.21',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги аренды (автотранспортных средств, оборудования, инструментов)' as POKAZATEL,
		noncx_rent as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		139,
		'1.22',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги аренды (автотранспортных средств, оборудования, инструментов)' as POKAZATEL,
		noncx_rent_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		140,
		'1.23',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги грузовых авто' as POKAZATEL,
		noncx_cargo as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		141,
		'1.24',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги грузовых авто' as POKAZATEL,
		noncx_cargo_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		142,
		'1.25',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги массажа, косметических, лечебных и оздоровительных процедур' as POKAZATEL,
		noncx_massage as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		143,
		'1.26',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги массажа, косметических, лечебных и оздоровительных процедур' as POKAZATEL,
		noncx_massage_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		144,
		'1.27',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги общепита (кафе, фаст-фуд, бистро, кофейни и т.д.)' as POKAZATEL,
		noncx_foodcourt as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		145,
		'1.28',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги общепита (кафе, фаст-фуд, бистро, кофейни и т.д.)' as POKAZATEL,
		noncx_foodcourt_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		146,
		'1.29',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги по уборке, озеленению, клининговые услуги и т.д.' as POKAZATEL,
		noncx_cleaning as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		147,
		'1.30',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги по уборке, озеленению, клининговые услуги и т.д.' as POKAZATEL,
		noncx_cleaning_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		148,
		'1.31',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги салонов красоты (парикмахерская, ногтевой сервис, маникюр, макияж)' as POKAZATEL,
		noncx_beuty as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		149,
		'1.32',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Услуги салонов красоты (парикмахерская, ногтевой сервис, маникюр, макияж)' as POKAZATEL,
		noncx_beuty_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		150,
		'1.33',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Химчистка одежды, авто, мойка ковров и т.д.' as POKAZATEL,
		noncx_carwash as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		151,
		'1.34',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Химчистка одежды, авто, мойка ковров и т.д.' as POKAZATEL,
		noncx_carwash_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		152,
		'1.35',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Швейный цех, ателье, вязальный цех, пошив и ремонт одежды, национальной одежды, головных уборов, кыз жасау, предметов быта' as POKAZATEL,
		noncx_atelie as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		153,
		'1.36',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Швейный цех, ателье, вязальный цех, пошив и ремонт одежды, национальной одежды, головных уборов, кыз жасау, предметов быта' as POKAZATEL,
		noncx_atelie_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		154,
		'1.37',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Прочие виды' as POKAZATEL,
		noncx_others as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		155,
		'1.38',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Прочие виды' as POKAZATEL,
		noncx_others_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		156,
		'2',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Строительство' as POKAZATEL,
		0 as VALUE,
		'' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		157,
		'2.1',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Строительные услуги' as POKAZATEL,
		noncx_stroika as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		158,
		'2.2',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Строительные услуги' as POKAZATEL,
		noncx_stroika_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		159,
		'3',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Промышленность' as POKAZATEL,
		0 as VALUE,
		'' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		160,
		'3.1',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Производство мебели' as POKAZATEL,
		noncx_mebel as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		161,
		'3.2',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Производство мебели' as POKAZATEL,
		noncx_mebel_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		162,
		'3.3',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Производство строительных материалов' as POKAZATEL,
		noncx_stroi_material as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		163,
		'3.4',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Производство строительных материалов' as POKAZATEL,
		noncx_stroi_material_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		164,
		'3.5',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Сварочный цех' as POKAZATEL,
		noncx_svarka as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
			
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		165,
		'3.6',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Сварочный цех' as POKAZATEL,
		noncx_svarka_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		166,
		'3.7',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Деревообработка' as POKAZATEL,
		noncx_woodworking as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		167,
		'3.8',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Деревообработка' as POKAZATEL,
		noncx_woodworking_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		168,
		'3.9',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Прочие' as POKAZATEL,
		noncx_others_uslugi as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		169,
		'3.10',
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'Прочие' as POKAZATEL,
		noncx_others_needs_uslugi as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
---------------------------------------------------------------------------	
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		170,
		'1',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Переработка молока (предприятия)' as POKAZATEL,
		manufacture_milk as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		171,
		'2',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Переработка молока (предприятия)' as POKAZATEL,
		manufacture_milk_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		172,
		'3',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Переработка мяса (предприятия)' as POKAZATEL,
		manufacture_meat as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		173,
		'4',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Переработка мяса (предприятия)' as POKAZATEL,
		manufacture_meat_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		174,
		'5',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Переработка плодов, ягод, овощей, картофеля, дикорастущего сырья' as POKAZATEL,
		manufacture_vegirables as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		175,
		'6',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Переработка плодов, ягод, овощей, картофеля, дикорастущего сырья' as POKAZATEL,
		manufacture_vegirables_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		176,
		'7',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство майонеза, растительных масел' as POKAZATEL,
		manufacture_mayo as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		177,
		'8',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство майонеза, растительных масел' as POKAZATEL,
		manufacture_mayo_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		178,
		'9',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Переработка рыбы' as POKAZATEL,
		manufacture_fish as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		179,
		'10',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Переработка рыбы' as POKAZATEL,
		manufacture_fish_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		180,
		'11',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство кондитерских изделий' as POKAZATEL,
		manufacture_choco as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		181,
		'12',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство кондитерских изделий' as POKAZATEL,
		manufacture_choco_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		182,
		'13',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство пива и безалкогольных напитков' as POKAZATEL,
		manufacture_beer as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		183,
		'14',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство пива и безалкогольных напитков' as POKAZATEL,
		manufacture_beer_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		184,
		'15',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство ликеро-водочных изделий' as POKAZATEL,
		manufacture_vodka as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		185,
		'16',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство ликеро-водочных изделий' as POKAZATEL,
		manufacture_vodka_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		186,
		'17',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Продукция из меда' as POKAZATEL,
		manufacture_honey as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		187,
		'18',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Продукция из меда' as POKAZATEL,
		manufacture_honey_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		188,
		'19',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство полуфабрикатов (пельмени, манты, вареники, замороженные продукты и пр.)' as POKAZATEL,
		manufacture_polufabricat as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		189,
		'20',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство полуфабрикатов (пельмени, манты, вареники, замороженные продукты и пр.)' as POKAZATEL,
		manufacture_polufabricat_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		190,
		'21',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство хлебобулочных изделий' as POKAZATEL,
		manufacture_bread as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		191,
		'22',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Производство хлебобулочных изделий' as POKAZATEL,
		manufacture_bread_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op

	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		192,
		'23',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Прочее' as POKAZATEL,
		manufacture_others as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		193,
		'24',
		'Переработка сельхозпродукции и пищевая промышленность' as KATEGORIYA,
		'Прочее' as POKAZATEL,
		manufacture_others_needs as VALUE,
		'потребность' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
---------------------------------------------------------------------------		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		194,
		'1',
		'Потребность в кредитах' as KATEGORIYA,
		'Количество заявок по направлениям кредита' as POKAZATEL,
		credit_amount as VALUE,
		'единиц' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		195,
		'2',
		'Потребность в кредитах' as KATEGORIYA,
		'Итого общая потребность в кредитах' as POKAZATEL,
		credit_total as VALUE,
		'тенге' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		196,
		'3',
		'Потребность в кредитах' as KATEGORIYA,
		'Средний чек по кредиту' as POKAZATEL,
		credit_average_total as VALUE,
		'тенге' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		197,
		'4',
		'Потребность в кредитах' as KATEGORIYA,
		'Количество обеспеченных залогом участников' as POKAZATEL,
		credit_zalog as VALUE,
		'%' as EDIZM,
		null
	from 
		DM_ANALYTICS.OPROS_MSH_KATO_3 as op) as p
	left join DM_ANALYTICS.OPROS_SDU_2 as s on 
		lowerUTF8(trim(s.POKAZATEL)) = lowerUTF8(trim(p.POKAZATEL)) and 
		lowerUTF8(s.EDIZM) = lowerUTF8(p.EDIZM) and 
		p.KATO_2 = s.KATO_2 and 
		p.KATO_4 = s.KATO_4 and 
		p.KATO_6 = s.KATO_6
	where p.KATO_2 is not null and p.KATO_4 is not null and p.KATO_6 is not null
	order by p.NUM