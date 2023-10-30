DROP TABLE IF EXISTS DM_ANALYTICS.OPROS_MSH_POKAZATEL;

CREATE TABLE DM_ANALYTICS.OPROS_MSH_POKAZATEL
(
    KATO_2 		Nullable(String),
    KATO_2_NAME	Nullable(String),
    KATO_4 		Nullable(String),
    KATO_4_NAME Nullable(String),
    KATO_6 		Nullable(String),
    KATO_6_NAME Nullable(String),
    NUM Int32,
    POKAZATEL 	String,
    VALUE 		Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY POKAZATEL
SETTINGS index_granularity = 8192;

insert into 
	DM_ANALYTICS.OPROS_MSH_POKAZATEL (KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, KATO_6, KATO_6_NAME, NUM, POKAZATEL, VALUE)
select 
	ifNull(p.KATO_2, 	  '(нет данных)'),
	ifNull(p.KATO_2_NAME, '(нет данных)'),
	ifNull(p.KATO_4, 	  '(нет данных)'),
	ifNull(p.KATO_4_NAME, '(нет данных)'),
	ifNull(p.KATO_6, 	  '(нет данных)'), 
	ifNull(p.KATO_6_NAME, '(нет данных)'),
	p.NUM,
	p.POKAZATEL,
	p.VALUE
from 
	(select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		1 as NUM,
		'численность населения' as POKAZATEL,
		labour_population as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		2,
		'итого экономически активного населения' as POKAZATEL,
		labour_labour as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		3,
		'из них занятые в личном подсобном хозяйстве (личное подворье)' as POKAZATEL,
		labour_private_ogorod as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		4,
		'итого экономически неактивного населения' as POKAZATEL,
		labour_total_econ_inactive_population as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		5,
		'безработный' as POKAZATEL,
		labour_unemployed as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op	
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		6,
		'средний доход на одну семью, в месяц' as POKAZATEL,
		labour_average_income_family as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		7,
		'количество заселенных дворов' as POKAZATEL,
		house_zaselen_dvor as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		8,
		'кол-во домашних хозяйств имеющих участки (огороды, сады, приусадебные участки)' as POKAZATEL,
		dx_number_ogorodov as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		9,
		'пашня, кв.метров' as POKAZATEL,
		dx_pashnya as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		10,
		'многолетние насаждения, кв.метров' as POKAZATEL,
		dx_mnogoletnie as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		11,
		'пастбища, кв.метров' as POKAZATEL,
		dx_pastbishe as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		12,
		'сенокосы, кв.метров' as POKAZATEL,
		dx_senokosy as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		13,
		'кол-во крестьянских хозяйств' as POKAZATEL,
		kx_amount as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		14,
		'пашня' as POKAZATEL,
		kx_pansya as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		15,
		'многолетние насаждения' as POKAZATEL,
		kx_mnogoletnie as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		16,
		'пастбища' as POKAZATEL,
		kx_pastbishe as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		17,
		'сенокосы' as POKAZATEL,
		kx_senokosy as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		18,
		'общее число дворов' as POKAZATEL,
		animal_dvor as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		19,
		'из них имеют скот и птицу' as POKAZATEL,
		animal_skot_bird as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		20,
		'КРС' as POKAZATEL,
		animal_krs as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		21,
		'овцы, бараны' as POKAZATEL,
		animal_sheep as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		22,
		'козы, козлы' as POKAZATEL,
		animal_kozel as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		23,
		'лошади' as POKAZATEL,
		animal_horse as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		24,
		'верблюды' as POKAZATEL,
		animal_camel as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		25,
		'свиньи' as POKAZATEL,
		animal_pig as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		26,
		'автосервис (СТО, шиномонтаж, замена автозапчастей и т.д.)' as POKAZATEL,
		`noncx_sto ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		27,
		'деревообработка' as POKAZATEL,
		`noncx_woodworking ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		28,
		'детские центры развития, репетиторские услуги, языковые курсы' as POKAZATEL,
		`noncx_kindergarden ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		29,
		'изготовление сувениров, украшений из различных материалов' as POKAZATEL,
		`noncx_souvenier ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		30,
		'компьютерные услуги' as POKAZATEL,
		`noncx_pc_service ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		31,
		'магазин (мини-маркет, строительных материалов, автозапчастей, одежды и обуви, орг.техники, сотовых телефонов и акссесуаров и др.)' as POKAZATEL,
		`noncx_store ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		32,
		'мастерская, услуги по ремонту бытовой техники, орг.техники, инструментов, замена картриджей и т.д.' as POKAZATEL,
		`noncx_remont_bytovoi_tech ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		33,
		'металлопластиковые изделия' as POKAZATEL,
		`noncx_metal ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		34,
		'оказание профессиональных услуг - бухгалтерские, юридические, налоговые, маркетинг, реклама и т.д.' as POKAZATEL,
		`noncx_accounting  ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		35,
		'полиграфические услуги, фотосалон, услуги фото-видео съемки' as POKAZATEL,
		`noncx_photo ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		36,
		'производство мебели' as POKAZATEL,
		`noncx_mebel ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		37,
		'производство строительных материалов' as POKAZATEL,
		`noncx_stroi_material ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		38,
		'строительные услуги' as POKAZATEL,
		`noncx_stroika ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		39,
		'туризм (гостиницы, хостелы, кемпинги, турбазы)' as POKAZATEL,
		`noncx_turism  ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		40,
		'услуги аренды ( автотранспортных средств, оборудования, инструментов)' as POKAZATEL,
		`noncx_rent ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		41,
		'услуги грузовых авто' as POKAZATEL,
		`noncx_cargo ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		42,
		'услуги массажа, косметических, лечебных и оздоровительных процедур' as POKAZATEL,
		`noncx_massage ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		43,
		'услуги общепита (кафе, фаст-фуд, бистро, кофейни и т.д.)' as POKAZATEL,
		`noncx_foodcourt ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		44,
		'услуги по уборке, озеленению, клининговые услуги и т.д.' as POKAZATEL,
		`noncx_cleaning ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		45,
		'услуги салонов красоты (парикмахерская, ногтевой сервис, маникюр, макияж)' as POKAZATEL,
		`noncx_beuty ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		46,
		'химчистка одежды, авто, мойка ковров и т.д.' as POKAZATEL,
		`noncx_carwash ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op
		
	UNION ALL
	
	select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		47,
		'швейный цех, ателье, вязальный цех, пошив и ремонт одежды, национальной одежды, головных уборов, кыз жасау, предметов быта' as POKAZATEL,
		`noncx_atelie ` as VALUE
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op) as p
	order by p.NUM