drop table if exists MGOV_SURVEYS.answers_dashboard;
CREATE TABLE MGOV_SURVEYS.answers_dashboard
(
    IIN String,
    question_type String,
    question_id String,
    question String,
    response String,
    response_1 String,
    response_2 String,
    response_3 String,
    NATIONALITY Nullable(String),
    FAMILY_CAT_NEW Nullable(String),
    uroven_dohod Nullable(String),
    obrazovanie Nullable(String),
    gorod_selo Nullable(String),
    PERSON_AGE Nullable(String),
    SEX_NAME Nullable(String),
    rab_nerab Nullable(String),
    FULL_KATO_NAME Nullable(String),
    KATO_2 Nullable(String),	
    KATO_2_NAME Nullable(String),	
    KATO_4 Nullable(String),
    KATO_4_NAME Nullable(String),	
    KATO_6 Nullable(String)
)
ENGINE = MergeTree
ORDER BY IIN
SETTINGS index_granularity = 8192
AS
select 
	p.uin as IIN,
	ans.question_type,
	ans.question_id,
	ans.question,
	ans.response,
	trim(splitByChar(',', ans.response)[1]) as response_1,
	trim(splitByChar(',', ans.response)[2]) as response_2,
	trim(splitByChar(',', ans.response)[3]) as response_3,
	cks.NATIONALITY,
	cks.FAMILY_CAT_NEW,
	cks.filtr1 as uroven_dohod,
	cks.filtr27 as obrazovanie,
	if(iin3.IS_VILLAGE_IIN = 1, 'Село', 'Город') as gorod_selo,
	iin3.PERSON_AGE,
	iin3.SEX_NAME,
	if(CNT_EMPLOYABLE_IIN3 = 1, 'Да', 'Нет') as rab_nerab,
	iin3.FULL_KATO_NAME,
	iin3.KATO_2,
	iin3.KATO_2_NAME,
	iin3.KATO_4,
	iin3.KATO_4_NAME,
	iin3.KATO_6
from MGOV_SURVEYS.parsed_answers as ans 
left join MGOV_SURVEYS.profiles as p on ans.profile_id = p.id
left join SOC_KARTA.NATIONALITY_SEGMENTATION_FAMILY_MEMBER as cks on cks.IIN = p.uin
left join SOC_KARTA.SK_FAMILY_QUALITY_IIN3 as iin3 on iin3.IIN = p.uin

-- Версия 2.0 14.03.2024
drop table if exists MGOV_SURVEYS.answers_dashboard;
CREATE TABLE MGOV_SURVEYS.answers_dashboard
(
    IIN String,
    question_type String,
    question_id String,
    question String,
    response String,
    response_1 String,
    response_2 String,
    response_3 String,
    NATIONALITY Nullable(String),
    FAMILY_CAT_NEW Nullable(String),
    uroven_dohod Nullable(String),
    obrazovanie Nullable(String),
    gorod_selo Nullable(String),
    PERSON_AGE Nullable(String),
    SEX_NAME Nullable(String),
    rab_nerab Nullable(String),
    FULL_KATO_NAME Nullable(String),
    KATO_2 Nullable(String),	
    KATO_2_NAME Nullable(String),	
    KATO_4 Nullable(String),
    KATO_4_NAME Nullable(String),	
    KATO_6 Nullable(String)
)
ENGINE = MergeTree
ORDER BY IIN
SETTINGS index_granularity = 8192
AS
with v1 as 
	(select 
		p.uin as IIN,
		ans.question_type as question_type,
		ans.question_id as question_id,
		ans.question as question,
		ans.response as response,
		splitByChar(',', ans.response) as response_arr,
		cks.NATIONALITY as NATIONALITY,
		cks.FAMILY_CAT_NEW as FAMILY_CAT_NEW,
		cks.filtr1 as uroven_dohod,
		cks.filtr27 as obrazovanie,
		if(iin3.IS_VILLAGE_IIN = 1, 'Село', 'Город') as gorod_selo,
		iin3.PERSON_AGE as PERSON_AGE,
		iin3.SEX_NAME as SEX_NAME,
		if(CNT_EMPLOYABLE_IIN3 = 1, 'Да', 'Нет') as rab_nerab,
		iin3.FULL_KATO_NAME as FULL_KATO_NAME,
		iin3.KATO_2 as KATO_2,
		iin3.KATO_2_NAME as KATO_2_NAME,
		iin3.KATO_4 as KATO_4,
		iin3.KATO_4_NAME as KATO_4_NAME, 
		iin3.KATO_6 as KATO_6
	from MGOV_SURVEYS.parsed_answers as ans 
	left join MGOV_SURVEYS.profiles as p on ans.profile_id = p.id
	left join SOC_KARTA.NATIONALITY_SEGMENTATION_FAMILY_MEMBER as cks on cks.IIN = p.uin
	left join SOC_KARTA.SK_FAMILY_QUALITY_IIN3 as iin3 on iin3.IIN = p.uin)
select
	IIN,
	question_type,
	question_id,
	question,
	response,
	trimBoth(response_arr) as response_1,
	'',
	'',
	NATIONALITY,
	FAMILY_CAT_NEW,
	uroven_dohod,
	obrazovanie,
	gorod_selo,
	PERSON_AGE,
	SEX_NAME,
	rab_nerab,
	FULL_KATO_NAME,
	KATO_2,
	KATO_2_NAME,
	KATO_4,
	KATO_4_NAME,
	KATO_6
from v1 array join v1.response_arr
where trimBoth(response_arr) <> '' and trimBoth(response_arr) is not null
