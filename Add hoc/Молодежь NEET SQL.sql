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
    
    KATO_2 Nullable(String),
    KATO_2_NAME Nullable(String),
    KATO_4 Nullable(String),
    KATO_4_NAME Nullable(String),
    KATO_6 Nullable(String),
    KATO_6_NAME Nullable(String),
    KATO Nullable(String),
    
    IS_NEET UInt8,
    NEET_CATEGORY Nullable(String)
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
	KATO_2, KATO_2_NAME,
	KATO_4, KATO_4_NAME,
	KATO_6, KATO_6_NAME,
	KATO,
	IS_NEET,
	NEET_CATEGORY)
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
	
	if(k.KATO_2 = '' or k.KATO_2 is null, '(без прописки)', k.KATO_2) as KATO_2, 
	if(k.KATO_2_NAME = '' or k.KATO_2_NAME is null, '(без прописки)', k.KATO_2_NAME) as KATO_2_NAME,

	if(k.KATO_4 = '' or k.KATO_4 is null, '(без прописки)', k.KATO_4) as KATO_4, 
	if(k.KATO_4_NAME = '' or k.KATO_4_NAME is null, '(без прописки)', k.KATO_4_NAME) as KATO_4_NAME,

	if(k.KATO_6 = '' or k.KATO_6 is null, '(без прописки)', k.KATO_6) as KATO_6, 
	if(k.KATO_6_NAME = '' or k.KATO_6_NAME is null, '(без прописки)', k.KATO_6_NAME) as KATO_6_NAME,
	KATO,
	
	case 
		when 
			neet.IS_YOUNG_STUDENT = 1 and 
			neet.IS_UCHRED = 'Не является учредителем ЮЛ' and 
			neet.IS_IP = 'Отсутствует ИП' and 
			neet.IS_GRST = 'Отсутсвует КХ/ФХ' and 
			neet.IS_OSMS = 'Отсутствует в списке плательщиков ОСМС' and 
			neet.IS_ESP = 'Не является плательщиком ЕСП' and 
			neet.IS_OPV_2MONTH = 'Отсутствуют налоговые отчисления ОПВ последние 2 месяца подряд' and 
			neet.IS_BEZRAB = 'Отсутствует в базе данных официальных безработных'
		then 1 
		else 0
	end as IS_NEET,
	
	case
		when 
			IS_NEET = 1 and
			neet.HAS_STUDY_DOC = 'Зарегистрирован диплом/аттестат об образовании' and 
			neet.HAS_SOC_OTCHISL = 'Никогда не отчисляли социальных отчислений и платежей'
		then 'Молодой выпускник'
		when 
			IS_NEET = 1 and
			neet.HAS_CHILD_POSOB = 'Не получает пособия по уходу за ребенком' and 
			neet.HAS_CHILD = 'Наличие ребенка (детей не более 3-х)'
		then 'Молодой родитель'
		else 
			if(IS_NEET = 1, 'Не работающий', '(нет данных)')
	end as NEET_CATEGORY
from 
	DM_ANALYTICS.PEOPLE_NEET as neet
	left join
		(select 
			IIN, 
			KATO_2, KATO_2_NAME,
			KATO_4, KATO_4_NAME,
			KATO_6, FULL_KATO_NAME as KATO_6_NAME
		from SOC_KARTA.KATO_FOR_FAMILY
		where KATO_2 <> '' and KATO_2_NAME <> '') as k on k.IIN = neet.IIN
where 
	neet.PERSON_AGE between 14 and 34