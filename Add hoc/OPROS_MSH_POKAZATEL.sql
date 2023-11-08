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
    KATEGORIYA  Nullable(String),
    POKAZATEL 	String,
    VALUE 		Nullable(Float64),
    EDIZM 		Nullable(String),
    VALUE_SDU 	Nullable(Float64),
    RAZNICA 	Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY POKAZATEL
SETTINGS index_granularity = 8192;

insert into 
	DM_ANALYTICS.OPROS_MSH_POKAZATEL (KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, KATO_6, KATO_6_NAME, 
									  NUM, KATEGORIYA, POKAZATEL, VALUE, EDIZM, 
									  VALUE_SDU, RAZNICA)
select 
	ifNull(p.KATO_2, 	  '(нет данных)'),
	ifNull(p.KATO_2_NAME, '(нет данных)'),
	ifNull(p.KATO_4, 	  '(нет данных)'),
	ifNull(p.KATO_4_NAME, '(нет данных)'),
	ifNull(p.KATO_6, 	  '(нет данных)'), 
	ifNull(p.KATO_6_NAME, '(нет данных)'),
	p.NUM,
	p.KATEGORIYA,
	p.POKAZATEL,
	p.VALUE,
	p.EDIZM,
	s.VALUE as VALUE_SDU,
	round(case 
			when p.VALUE <= s.VALUE and p.VALUE > 0 then (s.VALUE-p.VALUE)/p.VALUE*100
			when p.VALUE > s.VALUE and p.VALUE > 0 then (p.VALUE-s.VALUE)/p.VALUE*100
			when p.VALUE = 0 and s.VALUE > 0 then 100
			when s.VALUE = 0 and p.VALUE > 0 then 100
			else 0
		end, 2) as RAZNICA
from 
	(select 
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6, 
		KATO_6_NAME,
		1 as NUM,
		'Занятость' as KATEGORIYA,
		'численность населения' as POKAZATEL,
		labour_population as VALUE,
		'человек' as EDIZM
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
		'Занятость' as KATEGORIYA,
		'итого экономически активного населения' as POKAZATEL,
		labour_labour as VALUE,
		'человек' as EDIZM
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
		'Занятость' as KATEGORIYA,
		'из них занятые в личном подсобном хозяйстве (личное подворье)' as POKAZATEL,
		labour_private_ogorod as VALUE,
		'человек' as EDIZM
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
		'Занятость' as KATEGORIYA,
		'итого экономически неактивного населения' as POKAZATEL,
		labour_total_econ_inactive_population as VALUE,
		'человек' as EDIZM
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
		'Занятость' as KATEGORIYA,
		'безработный' as POKAZATEL,
		labour_unemployed as VALUE,
		'человек' as EDIZM
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
		'Занятость' as KATEGORIYA,
		'средний доход на одну семью, в месяц' as POKAZATEL,
		labour_average_income_family as VALUE,
		'тенге/месяц' as EDIZM
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
		'Жилищные условия' as KATEGORIYA,
		'количество заселенных дворов' as POKAZATEL,
		house_zaselen_dvor as VALUE,
		'единиц' as EDIZM
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
		'Наличие земельных угодий и посевных площадей в домашних хозяйствах' as KATEGORIYA,
		'кол-во домашних хозяйств имеющих участки (огороды, сады, приусадебные участки)' as POKAZATEL,
		dx_number_ogorodov as VALUE,
		'единиц' as EDIZM
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
		'Наличие земельных угодий и посевных площадей в домашних хозяйствах' as KATEGORIYA,
		'пашня' as POKAZATEL,
		dx_pashnya as VALUE,
		'площадь' as EDIZM
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
		'Наличие земельных угодий и посевных площадей в домашних хозяйствах' as KATEGORIYA,
		'многолетние насаждения' as POKAZATEL,
		dx_mnogoletnie as VALUE,
		'площадь' as EDIZM
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
		'Наличие земельных угодий и посевных площадей в домашних хозяйствах' as KATEGORIYA,
		'пастбища' as POKAZATEL,
		dx_pastbishe as VALUE,
		'площадь' as EDIZM
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
		'Наличие земельных угодий и посевных площадей в домашних хозяйствах' as KATEGORIYA,
		'сенокосы' as POKAZATEL,
		dx_senokosy as VALUE,
		'площадь' as EDIZM
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
		'Наличие земельных угодий и посевных площадей в крестьянских хозяйствах' as KATEGORIYA,
		'кол-во крестьянских хозяйств' as POKAZATEL,
		kx_amount as VALUE,
		'единиц' as EDIZM
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
		'Наличие земельных угодий и посевных площадей в крестьянских хозяйствах' as KATEGORIYA,
		'пашня' as POKAZATEL,
		kx_pansya as VALUE,
		'гектар' as EDIZM
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
		'Наличие земельных угодий и посевных площадей в крестьянских хозяйствах' as KATEGORIYA,
		'многолетние насаждения' as POKAZATEL,
		kx_mnogoletnie as VALUE,
		'гектар' as EDIZM
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
		'Наличие земельных угодий и посевных площадей в крестьянских хозяйствах' as KATEGORIYA,
		'пастбища' as POKAZATEL,
		kx_pastbishe as VALUE,
		'гектар' as EDIZM
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
		'Наличие земельных угодий и посевных площадей в крестьянских хозяйствах' as KATEGORIYA,
		'сенокосы' as POKAZATEL,
		kx_senokosy as VALUE,
		'гектар' as EDIZM
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
		'Животноводство' as KATEGORIYA,
		'общее число дворов' as POKAZATEL,
		animal_dvor as VALUE,
		'единиц' as EDIZM
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
		'Животноводство' as KATEGORIYA,
		'кол. дворов имеющих скот' as POKAZATEL,
		animal_skot_bird as VALUE,
		'единиц' as EDIZM
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
		'Животноводство' as KATEGORIYA,
		'КРС' as POKAZATEL,
		animal_krs as VALUE,
		'голов' as EDIZM
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
		'Животноводство' as KATEGORIYA,
		'овцы, бараны' as POKAZATEL,
		animal_sheep as VALUE,
		'голов' as EDIZM
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
		'Животноводство' as KATEGORIYA,
		'козы, козлы' as POKAZATEL,
		animal_kozel as VALUE,
		'голов' as EDIZM
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
		'Животноводство' as KATEGORIYA,
		'лошади' as POKAZATEL,
		animal_horse as VALUE,
		'голов' as EDIZM
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
		'Животноводство' as KATEGORIYA,
		'верблюды' as POKAZATEL,
		animal_camel as VALUE,
		'голов' as EDIZM
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
		'Животноводство' as KATEGORIYA,
		'свиньи' as POKAZATEL,
		animal_pig as VALUE,
		'голов' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'автосервис (СТО, шиномонтаж, замена автозапчастей и т.д.)' as POKAZATEL,
		`noncx_sto ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'деревообработка' as POKAZATEL,
		`noncx_woodworking ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'детские центры развития, репетиторские услуги, языковые курсы' as POKAZATEL,
		`noncx_kindergarden ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'изготовление сувениров, украшений из различных материалов' as POKAZATEL,
		`noncx_souvenier ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'компьютерные услуги' as POKAZATEL,
		`noncx_pc_service ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'магазин (мини-маркет, строительных материалов, автозапчастей, одежды и обуви, орг.техники, сотовых телефонов и акссесуаров и др.)' as POKAZATEL,
		`noncx_store ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'мастерская, услуги по ремонту бытовой техники, орг.техники, инструментов, замена картриджей и т.д.' as POKAZATEL,
		`noncx_remont_bytovoi_tech ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'металлопластиковые изделия' as POKAZATEL,
		`noncx_metal ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'оказание профессиональных услуг - бухгалтерские, юридические, налоговые, маркетинг, реклама и т.д.' as POKAZATEL,
		`noncx_accounting  ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'полиграфические услуги, фотосалон, услуги фото-видео съемки' as POKAZATEL,
		`noncx_photo ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'производство мебели' as POKAZATEL,
		`noncx_mebel ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'производство строительных материалов' as POKAZATEL,
		`noncx_stroi_material ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'строительные услуги' as POKAZATEL,
		`noncx_stroika ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'туризм (гостиницы, хостелы, кемпинги, турбазы)' as POKAZATEL,
		`noncx_turism  ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'услуги аренды ( автотранспортных средств, оборудования, инструментов)' as POKAZATEL,
		`noncx_rent ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'услуги грузовых авто' as POKAZATEL,
		`noncx_cargo ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'услуги массажа, косметических, лечебных и оздоровительных процедур' as POKAZATEL,
		`noncx_massage ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'услуги общепита (кафе, фаст-фуд, бистро, кофейни и т.д.)' as POKAZATEL,
		`noncx_foodcourt ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'услуги по уборке, озеленению, клининговые услуги и т.д.' as POKAZATEL,
		`noncx_cleaning ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'услуги салонов красоты (парикмахерская, ногтевой сервис, маникюр, макияж)' as POKAZATEL,
		`noncx_beuty ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'химчистка одежды, авто, мойка ковров и т.д.' as POKAZATEL,
		`noncx_carwash ` as VALUE,
		'единиц' as EDIZM
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
		'Несельскохозяйственные виды бизнеса' as KATEGORIYA,
		'швейный цех, ателье, вязальный цех, пошив и ремонт одежды, национальной одежды, головных уборов, кыз жасау, предметов быта' as POKAZATEL,
		`noncx_atelie ` as VALUE,
		'единиц' as EDIZM
	from 
		DM_ANALYTICS.OPROS_MSH_KATO as op) as p
	left join DM_ANALYTICS.OPROS_SDU as s on 
		lowerUTF8(s.POKAZATEL) = lowerUTF8(p.POKAZATEL) and 
		p.KATO_2 = s.KATO_2 and 
		p.KATO_4 = s.KATO_4 and 
		p.KATO_6 = s.KATO_6
	where p.KATO_2 is not null and p.KATO_4 is not null and p.KATO_6 is not null
	order by p.NUM