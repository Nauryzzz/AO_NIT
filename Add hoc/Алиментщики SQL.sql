-- запустить на 17 сервере. Загрузить данные с TEST.alimenty (17 сервер) в таблицу AIS_OIP.POKAZATELI (5 сервер)
DROP TABLE IF EXISTS AIS_OIP.POKAZATELI;

CREATE TABLE AIS_OIP.POKAZATELI
(
    `IIN` String,
    `pokazatel` String
)
ENGINE = MergeTree
ORDER BY IIN
SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS AIS_OIP.AIS_OIP_CASE_3;

CREATE TABLE AIS_OIP.AIS_OIP_CASE_3
(
    `IIN` String,
    `SEX_NAME` Nullable(String),
    `PERSON_AGE` Nullable(Int16),
    `IS_CREDIT` Nullable(Int16),
    `IS_LPH` Nullable(Int16),
    `IS_GRST` Nullable(Int16),
    `IS_DUCHET` Nullable(Int16),
    `KATO_2` Nullable(Int16),
    `KATO_2_NAME` Nullable(String),
    `IS_OBRASHENIE` Nullable(Int16),
    `IS_ALIVE` Nullable(Int16),
    `IS_PSIHUCHET` Nullable(Int16),
    `IS_NARKOUCHET` Nullable(Int16),
    `IS_ONKOUCHET` Nullable(Int16),
    `IS_DEESPOSOBNOST` Nullable(Int16),
    `IS_ZAKLUCHENIE` Nullable(Int16),
    `IS_HIGHSCHOOL` Nullable(Int16),
	`IS_NEDV` Nullable(Int16)
)
ENGINE = MergeTree
ORDER BY IIN
SETTINGS index_granularity = 8192;

insert into 
	AIS_OIP.AIS_OIP_CASE_3 (IIN, SEX_NAME, PERSON_AGE, 
							IS_CREDIT, IS_LPH, IS_GRST, 
							IS_DUCHET, KATO_2, KATO_2_NAME, IS_OBRASHENIE,
							IS_ALIVE, IS_PSIHUCHET, IS_NARKOUCHET,
							IS_ONKOUCHET, IS_DEESPOSOBNOST, IS_ZAKLUCHENIE,
							IS_HIGHSCHOOL, IS_NEDV)
select distinct
	a.IIN, /* ИИН */
	f.SEX_NAME, /* Пол */
	f.PERSON_AGE, /* Возраст */
	if(f.COUNT_CREDIT > 0, 1, 0) as IS_CREDIT, /* Наличие кредита */
	if(f.INCOME_LPH_IIN > 0, 1, 0) as IS_LPH, /* Наличие ЛПХ */
	if(f.CNT_GRST_IIN > 0, 1, 0) as IS_GRST, /* Наличие ГРСТ */
	if(f.DUCHET = 'D-UCHET', 1, 0) as IS_DUCHET, /* Д-учет */
	if(empty(f.KATO_2) = 1, '0', f.KATO_2) as KATO_2, /* Код региона */ 
	f.KATO_2_NAME as KATO_2_NAME, /* Название региона */
	if(e.iin_bin is not null, 1, 0) as IS_OBRASHENIE, /* Наличие обращения */
	if(z.IIN_H2 is null, 1, 0) as IS_ALIVE, /* Жив/мертв */
	set1.IS_PSIHUCHET, /* Псих. учет */
	set1.IS_NARKOUCHET, /* Нарко. учет */
	set1.IS_ONKOUCHET, /* Онко. учет */
	set1.IS_DEESPOSOBNOST, /* Дееспособность */
	set1.IS_ZAKLUCHENIE, /* Находится в заключении */
	if(f.EDU_HIGHSCHOOL > 0, 1, 0) as IS_HIGHSCHOOL, /* Наличие высшего образования */
	if(f.CNT_NEDV_IIN > 0, 1, 0) as IS_NEDV /* Наличие недвижимости */
from AIS_OIP.AIS_OIP_ALIMENTSCHIKI as a
	left join SOC_KARTA.SK_FAMILY_QUALITY_IIN3 as f on f.IIN = a.IIN
	left join MCRIAP_EOBR.main_sec_2 as e on e.iin_bin = a.IIN
	left join DM_MU.ZAGS_NUMBER_CURRENT_BIRTH_DEATH_1 as z on z.IIN_H2 = a.IIN and z.OBJECT_OF_ZAGS = 'DEATH'
	left join 
		(select 
			vt.IIN as IIN,
			sum(is_psihuchet) as IS_PSIHUCHET,
			sum(is_narkouchet) as IS_NARKOUCHET,
			sum(is_onkouchet) as IS_ONKOUCHET,
			sum(is_deesposobnost) as IS_DEESPOSOBNOST,
			sum(is_zakluchenie) as IS_ZAKLUCHENIE
		from
			(select 
				p.IIN,
				if(p.pokazatel = 'psih_uchet', 1, 0) as is_psihuchet,
				if(p.pokazatel = 'narko_uchet', 1, 0) as is_narkouchet,
				if(p.pokazatel = 'onko_uchet', 1, 0) as is_onkouchet,
				if(p.pokazatel = 'deesposobnost', 1, 0) as is_deesposobnost,
				if(p.pokazatel = 'zakluchenie', 1, 0) as is_zakluchenie
			from AIS_OIP.POKAZATELI as p) as vt
		group by vt.IIN) as set1 on set1.IIN = a.IIN;

-- запустить на 5 сервере
DROP TABLE IF EXISTS TEST.alimenty;

CREATE TABLE TEST.alimenty
(
    `IIN` String,
    `pokazatel` String
)
ENGINE = MergeTree
ORDER BY IIN
SETTINGS index_granularity = 8192;

insert into 
	TEST.alimenty (IIN, pokazatel)
select 
	IIN, pokazatel
from
	(select 
		distinct h.IIN as IIN,
		'psih_uchet' as pokazatel
	from MZ_ERDB.HUMAN as h 
		inner join MZ_ERDB.HUMAN_DIAG as hd on hd.HUMAN_UID = h.UID
	where (hd.ICD10 between 'F00' and 'F09.9' or hd.ICD10 between 'F20' and 'F99.9') and 
		h.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		h.IIN is not null
UNION ALL
	select 
		distinct h.IIN as IIN,
		'narko_uchet' as pokazatel
	from MZ_ERDB.HUMAN as h 
		inner join MZ_ERDB.HUMAN_DIAG as hd on hd.HUMAN_UID = h.UID
	where hd.ICD10 between 'F10' and 'F19.9' and 
		h.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		h.IIN is not null
UNION ALL
	select 
		distinct op.RPN_IIN as IIN,
		'onko_uchet' as pokazatel
	from MZ_EROB.ONCOMED_PERSON as op
	where op.RPN_IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		op.RPN_IIN is not null
UNION ALL
	select 
		distinct fl.IIN as IIN,
		'deesposobnost' as pokazatel
	from MU_FL.GBL_PERSON as fl
	where fl.CAPABLE_STATUS_ID in (1, 2) and 
		fl.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		fl.IIN is not null
UNION ALL
	select 
		distinct fl.IIN as IIN,
		'zakluchenie' as pokazatel
	from MU_FL.GBL_PERSON as fl
	where fl.IMPRISONED_STATUS_ID is not null and 
		fl.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		fl.IIN is not null) as vt