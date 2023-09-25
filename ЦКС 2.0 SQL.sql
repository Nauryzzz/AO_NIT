/* 1. Свидетельство о рождении */
-- Могут быть значение NULL в ИИН у родителей
-- Один и тот же NUMBER_AKT, но разные ИИН у детей
-- Есть ИИН детей равных ИИН родителей (817 записей)
select 
	birth.NUMBER_AKT,
	birth.CHILD_IIN,
	birth.MOTHER_IIN,
	birth.FATHER_IIN,
	birth.CHILD_BIRTH_DATE
from
	(select 
		trim(z.NUMBER_AKT) as NUMBER_AKT, 
		z.CHILD_IIN, 
		if(z.MOTHER_IIN = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, z.MOTHER_IIN) as MOTHER_IIN,
		if(z.FATHER_IIN = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, z.FATHER_IIN) as FATHER_IIN,
		z.CHILD_BIRTH_DATE,
		row_number() over (partition by z.CHILD_IIN order by z.CHANGE_DATE desc) as CHANGE_DATE_NUM
	from MU_ZAGS.ZAGS_BIRTH_ARCHIVE as z
	where 
		z.STATUS_ID = 7 and /* Регистрация завершена */
		z.BIRTH_STATUS_ID = 1 and /* Живорожденный */
		z.CHILD_IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and /* Не NULL */
		z.CHILD_IIN is not null and
		z.NUMBER_AKT is not null) as birth
where birth.CHANGE_DATE_NUM = 1;

/* 2. Свидетельство о браке */
-- кол ИИН мужчин с несколькими номерами акта 321 493
-- кол ИИН женщин с несколькими номерами акта 198 478
-- кол записей с NULL ИИН для мужчины и женщины 4 308 057
-- кол записей с одинаковыми ИИН для мужчины и женщины 255
select
	marriage.NUMBER_AKT,
	marriage.MAN_IIN,
	marriage.WOMAN_IIN,
	marriage.REG_DATE
from
	(select 
		trim(m.NUMBER_AKT) as NUMBER_AKT,
		if(m.MAN_IIN = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, m.MAN_IIN) as MAN_IIN,
		if(m.WOMAN_IIN = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, m.WOMAN_IIN) as WOMAN_IIN,
		m.REG_DATE,
		row_number() over (partition by m.NUMBER_AKT order by m.CHANGE_DATE desc) as CHANGE_DATE_NUM
	from MU_ZAGS.ZAGS_MARRIAGE_ARCHIVE as m 
	where 
		m.STATUS_ID = 7 and /* Регистрация завершена */
		m.DIVORCE_AKT_NUMBER is null and /* Брак не расторгнут */
		m.NUMBER_AKT is not null) as marriage
where marriage.CHANGE_DATE_NUM = 1;

/* 3. ГБД ФЛ */
select
	fl.IIN,
	fl.BIRTH_DATE,
	fl.IS_LIVE,
	fl.DEATH_DATE,
	fl.AR_CODE
from
	(select
		p.IIN,
		p.BIRTH_DATE,
		if(p.PERSON_STATUS_ID = 3, 0, 1) as IS_LIVE, /* PERSON_STATUS_ID = 3 Умерший */
		if(p.DEATH_DATE = '0000-00-00 00:00:00', null, p.DEATH_DATE) as DEATH_DATE,
		if(p.AR_CODE = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, p.AR_CODE) as AR_CODE,
		row_number() over (partition by p.IIN order by p.CHANGE_TIME desc) as CHANGE_TIME_NUM
	from MU_FL.GBL_PERSON as p
	where 
		p.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		p.IIN is not null) as fl
where fl.CHANGE_TIME_NUM = 1;