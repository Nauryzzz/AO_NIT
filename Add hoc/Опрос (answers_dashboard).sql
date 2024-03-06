drop table if exists test.answers_dashboard;
CREATE TABLE test.answers_dashboard
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
	p.iin as IIN,
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
from test.parsed_answers as ans 
left join test.profiles as p on ans.profile_id = p.id
left join SOC_KARTA.NATIONALITY_SEGMENTATION_FAMILY_MEMBER as cks on cks.IIN = p.iin
left join SOC_KARTA.SK_FAMILY_QUALITY_IIN3 as iin3 on iin3.IIN = p.iin