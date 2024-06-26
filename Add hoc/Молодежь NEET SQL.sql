DROP TABLE IF EXISTS DM_ANALYTICS.YOUTH_NEET;

CREATE TABLE DM_ANALYTICS.YOUTH_NEET
(
    IIN String,
    SEX_NAME Nullable(String),
    PERSON_AGE Nullable(Int64),
    AGE_CATEGORY Nullable(String),
    ZAGS_STATUS Nullable(String),
    IS_UCHRED String,
    IS_IP String,
    IS_GRST String,
    IS_OSMS String,
    IS_ESP String,
    IS_OPV_2MONTH String,
    IS_YOUNG_STUDENT UInt8,
    HAS_STUDY_DOC String,
    IS_OLD_STUDENT UInt8,
    HAS_CHILD_POSOB String,
    HAS_CHILD String,
    IS_BEZRAB String,
    HAS_SOC_OTCHISL String,
    HAS_SRED_OBR String,
    HAS_UNFINISH_SRED_OBR String,
    HAS_TIPO_OBR String,
    HAS_UNFINISH_TIPO_OBR String,
    HAS_VYSWEE_OBR String,
    HAS_UNFINISH_VYSWEE_OBR String,
    HAS_MASTER_DEGREE_OBR String,
    HAS_UNFINISH_MASTER_DEGREE_OBR String,
	
    IS_DISABLED Nullable(Int32),
    IS_IMPRISONED Nullable(Int32),
    IS_STUDENT Nullable(Int32),
    IS_PAV Nullable(Int32),
    IS_PREGNANT Nullable(Int32),
    IS_IN_DEBT Nullable(Int32),
    HAS_TRANSPORT Nullable(Int32),
    HAS_ALIMONY_DEBT Nullable(Int32),
    
    KATO_2 Nullable(String),
    KATO_2_NAME Nullable(String),
    KATO_4 Nullable(String),
    KATO_4_NAME Nullable(String),
    KATO_6 Nullable(String),
    KATO_6_NAME Nullable(String),
    KATO Nullable(String),
    
    IS_NEET UInt8,
    NEET_CATEGORY Nullable(String),
    SDU_LOAD_IN_DT DateTime
)
ENGINE = MergeTree
ORDER BY IIN
SETTINGS index_granularity = 8192;

insert into DM_ANALYTICS.YOUTH_NEET 
	(IIN,
	SEX_NAME,
	PERSON_AGE,
	AGE_CATEGORY,
	ZAGS_STATUS,
	IS_UCHRED,
	IS_IP,
	IS_GRST,
	IS_OSMS,
	IS_ESP,
	IS_OPV_2MONTH,
	IS_YOUNG_STUDENT,
	HAS_STUDY_DOC,
	IS_OLD_STUDENT,
	HAS_CHILD_POSOB,
	HAS_CHILD,
	IS_BEZRAB,
	HAS_SOC_OTCHISL,
	HAS_SRED_OBR,
	HAS_UNFINISH_SRED_OBR,
	HAS_TIPO_OBR,
	HAS_UNFINISH_TIPO_OBR,
	HAS_VYSWEE_OBR,
	HAS_UNFINISH_VYSWEE_OBR,
	HAS_MASTER_DEGREE_OBR,
	HAS_UNFINISH_MASTER_DEGREE_OBR,
  	IS_DISABLED,
  	IS_IMPRISONED,
  	IS_STUDENT,
    IS_PAV,
    IS_PREGNANT,
    IS_IN_DEBT,
    HAS_TRANSPORT,
    HAS_ALIMONY_DEBT,
	KATO_2, KATO_2_NAME,
	KATO_4, KATO_4_NAME,
	KATO_6, KATO_6_NAME,
	KATO,
	IS_NEET,
	NEET_CATEGORY,
	SDU_LOAD_IN_DT)
select
	IIN,
  	SEX_NAME,
  	PERSON_AGE,
  	AGE_CATEGORY,
  	ifNull(neet.ZAGS_STATUS, '(нет данных)') as ZAGS_STATUS,
  	IS_UCHRED,
  	IS_IP,
  	IS_GRST,
  	IS_OSMS,
  	IS_ESP,
  	IS_OPV_2MONTH,
  	IS_YOUNG_STUDENT,
  	HAS_STUDY_DOC,
  	IS_OLD_STUDENT,
  	HAS_CHILD_POSOB,
  	HAS_CHILD,
  	IS_BEZRAB,
  	HAS_SOC_OTCHISL,
  	HAS_SRED_OBR,
  	HAS_UNFINISH_SRED_OBR,
  	HAS_TIPO_OBR,
  	HAS_UNFINISH_TIPO_OBR,
  	HAS_VYSWEE_OBR,
  	HAS_UNFINISH_VYSWEE_OBR,
  	HAS_MASTER_DEGREE_OBR,
  	HAS_UNFINISH_MASTER_DEGREE_OBR,
	
  	IS_DISABLED,
  	IS_IMPRISONED,
  	IS_STUDENT,
    IS_PAV,
    IS_PREGNANT,
    IS_IN_DEBT,
    HAS_TRANSPORT,
    HAS_ALIMONY_DEBT,
	
	k.KATO_2  as KATO_2, 
	k.KATO_2_NAME as KATO_2_NAME,

	k.KATO_4 as KATO_4, 
	k.KATO_4_NAME as KATO_4_NAME,

	k.KATO_6 as KATO_6, 
	k.KATO_6_NAME as KATO_6_NAME,
	KATO,
	
	case 
		when 
			neet.IS_UCHRED = 'Не является учредителем ЮЛ' and 
			neet.IS_IP = 'Отсутствует ИП' and 
			neet.IS_GRST = 'Отсутсвует КХ/ФХ' and 
			neet.IS_OSMS = 'Отсутствует в списке плательщиков ОСМС' and 
			neet.IS_ESP = 'Не является плательщиком ЕСП' and 
			neet.IS_OPV_2MONTH = 'Отсутствуют налоговые отчисления ОПВ последние 2 месяца подряд' and 
			neet.IS_BEZRAB = 'Отсутствует в базе данных официальных безработных' and 
			neet.IS_STUDENT = 0 and 
			neet.IS_IMPRISONED = 0 and 
			neet.IS_DISABLED = 0
		then 1 
		else 0
	end as IS_NEET,
	
	case
		when 
			IS_NEET = 1 and
			neet.IS_YOUNG_STUDENT = 1 and 
			neet.HAS_STUDY_DOC = 'Зарегистрирован диплом/аттестат об образовании' and 
			neet.HAS_SOC_OTCHISL = 'Никогда не отчисляли социальных отчислений и платежей'
		then 'Молодой выпускник'
		
		when 
			IS_NEET = 1 and 
			IS_OLD_STUDENT = 1
		then 'Не работающий'
		
		when 
			IS_NEET = 1 and
			neet.HAS_CHILD_POSOB = 'Не получает пособия по уходу за ребенком' and 
			neet.HAS_CHILD = 'Наличие ребенка (детей не более 3-х)'
		then 'Молодой родитель'
		
		else '(нет данных)'
	end as NEET_CATEGORY,
	
	today() as SDU_LOAD_IN_DT
from 
	DM_ANALYTICS.PEOPLE_NEET as neet
	left join
		(select 
			IIN, 
			KATO_2, KATO_2_NAME,
			
			if(KATO_2_NAME in ('г. Астана', 'г.Алматы', 'г.Шымкент'), null, KATO_4) as KATO_4, 
			if(KATO_2_NAME in ('г. Астана', 'г.Алматы', 'г.Шымкент'), null, KATO_4_NAME) as KATO_4_NAME,
			
			KATO_6, FULL_KATO_NAME as KATO_6_NAME
		from SOC_KARTA.KATO_FOR_FAMILY
		where KATO_2 <> '' and KATO_2_NAME <> '') as k on k.IIN = neet.IIN
where 
	neet.PERSON_AGE between 14 and 34;

/*
CREATE TABLE DM_ANALYTICS.YOUTH_NEET_HIST
(
	NEET_CNT Nullable(Int64),
	YOUNG_GRDAUATED Nullable(Int64),
	NERAB Nullable(Int64),
	YOUNG_PARENT Nullable(Int64),
	YOUTH_CNT Nullable(Int64),
    SDU_LOAD_IN_DT DateTime
)
ENGINE = MergeTree
order by SDU_LOAD_IN_DT
SETTINGS index_granularity = 8192;
*/

insert into DM_ANALYTICS.YOUTH_NEET_HIST 
	(NEET_CNT, 
	YOUNG_GRDAUATED,
	NERAB,
	YOUNG_PARENT,
	YOUTH_CNT, 
	SDU_LOAD_IN_DT)
select 
	sumIf(yn.IS_NEET = 1, 1) as NEET_CNT,
	sumIf(yn.NEET_CATEGORY = 'Молодой выпускник', 1) as YOUNG_GRDAUATED,
	sumIf(yn.NEET_CATEGORY = 'Не работающий', 1) as NERAB,
	sumIf(yn.NEET_CATEGORY = 'Молодой родитель', 1) as YOUNG_PARENT,
	COUNT(yn.IIN) as YOUTH_CNT,
	MAX(SDU_LOAD_IN_DT) as SDU_LOAD_IN_DT
from DM_ANALYTICS.YOUTH_NEET as yn

/*
NEET_CNT	YOUTH_CNT	SDU_LOAD_IN_DT
346968	5853905	2023-11-14 00:00:00
346968	5853905	2023-11-14 00:00:00
346968	5853905	2023-11-15 00:00:00
175003	5910066	2024-01-04 00:00:00
3605497	5907205	2024-01-30 00:00:00
3603359	5913692	2024-02-01 07:26:20
3604788	5915312	2024-02-02 07:24:32
3604786	5915312	2024-02-03 07:28:03
3607446	5918060	2024-02-04 07:24:40
3608354	5918683	2024-02-05 07:22:09
3610074	5920937	2024-02-06 07:34:21
3632463	5922992	2024-02-07 07:28:55
*/