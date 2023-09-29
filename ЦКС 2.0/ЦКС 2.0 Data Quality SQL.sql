--CREATE TABLE TEST.DATA_QUALITY
--(
--    `ID` String,
--    `CHECK_DATE` DateTime,
--    `RULE_ID` Int8,
--    `CURRENT_VALUE` String,
--    `DETAILS` String
--)
--ENGINE = MergeTree
--ORDER BY RULE_ID
--SETTINGS index_granularity = 8192;

-- RULE_ID = 1
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select
	fl.IIN,			-- ID
	today(),		-- CHECK_DATE
	1,				-- RULE_ID
	fl.BIRTH_DATE,	-- CURRENT_VALUE
	''				-- DETAILS
from
	(select
		p.ID,
		p.IIN,
		p.BIRTH_DATE,
		if(p.PERSON_STATUS_ID = 3, 0, 1) as IS_LIVE, /* PERSON_STATUS_ID = 3 Умерший */
		if(p.DEATH_DATE = '0000-00-00 00:00:00', null, p.DEATH_DATE) as DEATH_DATE,
		if(p.AR_CODE = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, p.AR_CODE) as AR_CODE,
		p.CHANGE_TIME as CHANGE_DATE,
		row_number() over (partition by p.IIN order by p.CHANGE_TIME desc) as CHANGE_TIME_NUM
	from MU_FL.GBL_PERSON as p
	where 
		p.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		p.IIN is not null) as fl
where fl.CHANGE_TIME_NUM = 1 and (cast(SUBSTRING(fl.BIRTH_DATE, 1, 4) as Int32) < 1800 or cast(SUBSTRING(fl.BIRTH_DATE, 1, 4) as Int32) > 2024);

-- RULE_ID = 2
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select
	fl.IIN,			-- ID
	today(),		-- CHECK_DATE
	2,				-- RULE_ID
	'',				-- CURRENT_VALUE
	''				-- DETAILS
from
	(select
		p.ID,
		p.IIN,
		p.BIRTH_DATE,
		if(p.PERSON_STATUS_ID = 3, 0, 1) as IS_LIVE, /* PERSON_STATUS_ID = 3 Умерший */
		if(p.DEATH_DATE = '0000-00-00 00:00:00', null, p.DEATH_DATE) as DEATH_DATE,
		if(p.AR_CODE = '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8', null, p.AR_CODE) as AR_CODE,
		p.CHANGE_TIME as CHANGE_DATE,
		row_number() over (partition by p.IIN order by p.CHANGE_TIME desc) as CHANGE_TIME_NUM
	from MU_FL.GBL_PERSON as p
	where 
		p.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		p.IIN is not null) as fl
where fl.CHANGE_TIME_NUM = 1 and fl.AR_CODE is null

-- RULE_ID = 3
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select 
	birth.ID,		-- ID
	today(),		-- CHECK_DATE
	3,				-- RULE_ID
	'',				-- CURRENT_VALUE
	''				-- DETAILS
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
where birth.CHANGE_DATE_NUM = 1 and birth.MOTHER_IIN is null;

-- RULE_ID = 4
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select 
	birth.ID,		-- ID
	today(),		-- CHECK_DATE
	4,				-- RULE_ID
	'',				-- CURRENT_VALUE
	''				-- DETAILS
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
where birth.CHANGE_DATE_NUM = 1 and birth.FATHER_IIN is null;

-- RULE_ID = 5
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select 
	birth.ID,		-- ID
	today(),		-- CHECK_DATE
	5,				-- RULE_ID
	birth.CHILD_IIN,-- CURRENT_VALUE
	''				-- DETAILS
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
where birth.CHANGE_DATE_NUM = 1 and birth.MOTHER_IIN = birth.CHILD_IIN;

-- RULE_ID = 6
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select 
	birth.ID,		-- ID
	today(),		-- CHECK_DATE
	6,				-- RULE_ID
	birth.CHILD_IIN,-- CURRENT_VALUE
	''				-- DETAILS
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
where birth.CHANGE_DATE_NUM = 1 and birth.FATHER_IIN = birth.CHILD_IIN;

-- RULE_ID = 7
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select 
	birth.ID,		-- ID
	today(),		-- CHECK_DATE
	7,				-- RULE_ID
	'',				-- CURRENT_VALUE
	''				-- DETAILS
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
where birth.CHANGE_DATE_NUM = 1 and (birth.FATHER_IIN is null and birth.MOTHER_IIN is null);

-- RULE_ID = 8
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select 
	birth.ID,		-- ID
	today(),		-- CHECK_DATE
	8,				-- RULE_ID
	'',				-- CURRENT_VALUE
	''				-- DETAILS
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
where birth.CHANGE_DATE_NUM = 1 and birth.CHILD_IIN is null;

-- RULE_ID = 9
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select 
	birth.ID,			-- ID
	today(),			-- CHECK_DATE
	9,					-- RULE_ID
	birth.MOTHER_IIN,	-- CURRENT_VALUE
	''					-- DETAILS
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
where birth.CHANGE_DATE_NUM = 1 and birth.MOTHER_IIN = birth.FATHER_IIN;

-- RULE_ID = 10
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select 
	birth.CHILD_IIN,						-- ID
	today(),								-- CHECK_DATE
	10,										-- RULE_ID
	count(birth.ID),						-- CURRENT_VALUE		
	groupArray(cast(birth.ID as String))	-- DETAILS
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
where birth.CHANGE_DATE_NUM = 1
group by birth.CHILD_IIN
having count(birth.ID) > 1;

-- RULE_ID = 11
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select 
	birth.CHILD_IIN,								-- ID
	today(),										-- CHECK_DATE
	11,												-- RULE_ID
	count(distinct birth.MOTHER_IIN) + 
	count(distinct birth.FATHER_IIN),				-- CURRENT_VALUE
	groupArray(cast(birth.ID as String))			-- DETAILS
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
where birth.CHANGE_DATE_NUM = 1
group by birth.CHILD_IIN
having count(distinct birth.MOTHER_IIN) > 1 or count(distinct birth.FATHER_IIN) > 1

-- RULE_ID = 12
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select
	marriage.ID,	-- ID
	today(),		-- CHECK_DATE
	12,				-- RULE_ID
	'',				-- CURRENT_VALUE
	''				-- DETAILS
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
where marriage.CHANGE_DATE_NUM = 1 and marriage.WOMAN_IIN is null;

-- RULE_ID = 13
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select
	marriage.ID,	-- ID
	today(),		-- CHECK_DATE
	13,				-- RULE_ID
	'',				-- CURRENT_VALUE
	''				-- DETAILS
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
where marriage.CHANGE_DATE_NUM = 1 and marriage.MAN_IIN is null;

-- RULE_ID = 14
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select
	marriage.ID,	-- ID
	today(),		-- CHECK_DATE
	14,				-- RULE_ID
	'',				-- CURRENT_VALUE
	''				-- DETAILS
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
where marriage.CHANGE_DATE_NUM = 1 and (marriage.MAN_IIN is null and marriage.WOMAN_IIN is null);

-- RULE_ID = 15
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select
	marriage.ID,		-- ID
	today(),			-- CHECK_DATE
	15,					-- RULE_ID
	marriage.MAN_IIN,	-- CURRENT_VALUE
	''					-- DETAILS
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
where marriage.CHANGE_DATE_NUM = 1 and (marriage.MAN_IIN = marriage.WOMAN_IIN);

-- RULE_ID = 16
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select
	marriage.MAN_IIN,						-- ID
	today(),								-- CHECK_DATE
	16,										-- RULE_ID
	count(marriage.ID),						-- CURRENT_VALUE		
	groupArray(cast(marriage.ID as String))	-- DETAILS
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
where marriage.CHANGE_DATE_NUM = 1 and marriage.MAN_IIN is not null
group by marriage.MAN_IIN
having count(marriage.ID) > 1;

-- RULE_ID = 17
insert into
	TEST.DATA_QUALITY(ID, CHECK_DATE, RULE_ID, CURRENT_VALUE, DETAILS)
select
	marriage.WOMAN_IIN,						-- ID
	today(),								-- CHECK_DATE
	17,										-- RULE_ID
	count(marriage.ID),						-- CURRENT_VALUE		
	groupArray(cast(marriage.ID as String))	-- DETAILS
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
where marriage.CHANGE_DATE_NUM = 1 and marriage.WOMAN_IIN is not null
group by marriage.WOMAN_IIN
having count(marriage.ID) > 1;