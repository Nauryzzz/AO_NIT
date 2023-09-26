/* 1. Свидетельство о рождении */
-- Могут быть значение NULL в ИИН у родителей
-- Один и тот же NUMBER_AKT, но разные ИИН у детей
-- Есть ИИН детей равных ИИН родителей (817 записей)
select 
	birth.ID,
	birth.CHILD_IIN,
	birth.MOTHER_IIN,
	birth.FATHER_IIN,
	birth.CHILD_BIRTH_DATE,
	birth.CHANGE_DATE
from
	(select 
		z.ID,
		trim(z.NUMBER_AKT) as NUMBER_AKT, 
		z.CHILD_IIN, 
		if(z.MOTHER_IIN = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, z.MOTHER_IIN) as MOTHER_IIN,
		if(z.FATHER_IIN = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, z.FATHER_IIN) as FATHER_IIN,
		z.CHILD_BIRTH_DATE,
		z.CHANGE_DATE,
		row_number() over (partition by z.CHILD_IIN order by z.CHANGE_DATE desc) as CHANGE_DATE_NUM
	from MU_ZAGS.ZAGS_BIRTH_ARCHIVE as z
	where 
		z.STATUS_ID = 7 and /* Регистрация завершена */
		z.BIRTH_STATUS_ID = 1 and /* Живорожденный */
		z.CHILD_IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and /* Не NULL */
		z.CHILD_IIN is not null and
		z.NUMBER_AKT is not null and
		z.ANNULATED = 0 and
		z.DELETED = 0 and
		z.DELETED_AS_DUPLICATE = 0) as birth
where birth.CHANGE_DATE_NUM = 1 and (birth.MOTHER_IIN is not null and birth.FATHER_IIN is not null);

/* 2. Свидетельство о браке */
-- количество ИИН мужчин с несколькими номерами акта 321 493
-- количество ИИН женщин с несколькими номерами акта 198 478
-- количество записей с NULL ИИН для мужчины и женщины 4 308 057
-- количество записей с одинаковыми ИИН для мужчины и женщины 255
select
	marriage.ID,
	marriage.MAN_IIN,
	marriage.WOMAN_IIN,
	marriage.REG_DATE,
	marriage.CHANGE_DATE
from
	(select 
		m.ID,
		trim(m.NUMBER_AKT) as NUMBER_AKT,
		if(m.MAN_IIN = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, m.MAN_IIN) as MAN_IIN,
		if(m.WOMAN_IIN = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, m.WOMAN_IIN) as WOMAN_IIN,
		m.REG_DATE,
		m.CHANGE_DATE,
		row_number() over (partition by m.NUMBER_AKT order by m.CHANGE_DATE desc) as CHANGE_DATE_NUM
	from MU_ZAGS.ZAGS_MARRIAGE_ARCHIVE as m 
	where 
		m.STATUS_ID = 7 and /* Регистрация завершена */
		m.DIVORCE_AKT_NUMBER is null and /* Брак не расторгнут */
		m.NUMBER_AKT is not null and
		m.ANNULATED = 0 and
		m.DELETED = 0 and
		m.DELETED_AS_DUPLICATE = 0) as marriage
where marriage.CHANGE_DATE_NUM = 1 and (marriage.MAN_IIN is not null and marriage.WOMAN_IIN is not null);

/* 3. ГБД ФЛ */
-- DEATH_DATE может быть NULL при статусе "мертв"
select
	fl.ID,
	fl.IIN,
	fl.BIRTH_DATE,
	fl.IS_LIVE,
	fl.DEATH_DATE,
	fl.AR_CODE,
	fl.CHANGE_TIME
from
	(select
		p.ID,
		p.IIN,
		p.BIRTH_DATE,
		if(p.PERSON_STATUS_ID = 3, 0, 1) as IS_LIVE, /* PERSON_STATUS_ID = 3 Умерший */
		if(p.DEATH_DATE = '0000-00-00 00:00:00', null, p.DEATH_DATE) as DEATH_DATE,
		if(p.AR_CODE = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, p.AR_CODE) as AR_CODE,
		p.CHANGE_TIME,
		row_number() over (partition by p.IIN order by p.CHANGE_TIME desc) as CHANGE_TIME_NUM
	from MU_FL.GBL_PERSON as p
	where 
		p.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		p.IIN is not null) as fl
where fl.CHANGE_TIME_NUM = 1;