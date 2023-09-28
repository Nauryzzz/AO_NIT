-- 1. На 5 сервере. Создание промежуточной витрины для переноса на 17 сервере
drop table if exists TEST.FIRST_CLASSES;
create table TEST.FIRST_CLASSES
(
    GOD Int32,
    IIN String,
    POL Nullable(String),
    BIRTH_DATE Nullable(String),
    BIRTH_YEAR Int32,
    AGE Int32
)
engine = MergeTree
order by IIN
settings index_granularity = 8192;

insert into 
	TEST.FIRST_CLASSES (GOD, IIN, POL, BIRTH_DATE, BIRTH_YEAR, AGE)
select distinct
	fc.GOD,
	fc.IIN,
	fc.POL,
	fc.BIRTH_DATE,
	fc.BIRTH_YEAR,
	fc.GOD - fc.BIRTH_YEAR as AGE
from
(select
	case 
		when (vt.BIRTH_YEAR in (2014, 2015) and vt.REG_YEAR = 2021) then 2021
		when (vt.BIRTH_YEAR in (2015, 2016) and vt.REG_YEAR = 2022) then 2022
		when (vt.BIRTH_YEAR in (2016, 2017) and vt.REG_YEAR = 2023) then 2023
		when (vt.BIRTH_YEAR in (2018)) then 2024
		when (vt.BIRTH_YEAR in (2019)) then 2025
		when (vt.BIRTH_YEAR in (2020)) then 2026
	end as GOD,
	vt.IIN,
	vt.POL,
	vt.BIRTH_DATE,
	vt.BIRTH_YEAR,
	date_diff(year, toDate(vt.BIRTH_DATE), ifNull(toDate(vt.REG_DATE), today())) as AGE
from
	(select distinct
		gp.IIN as IIN, 
		if(gp.SEX_ID = 1, 'мужской', 'женский') as POL, 
		gp.BIRTH_DATE as BIRTH_DATE,
		cast(substring(gp.BIRTH_DATE, 1, 4) as int) as BIRTH_YEAR,
		s.REG_DATE as REG_DATE,
		cast(substring(if(s.REG_DATE = '' or s.REG_DATE is null, '0000', s.REG_DATE), 1, 4) as int) as REG_YEAR
	from MU_FL.GBL_PERSON as gp
		left join
			(select 
				st.IIN as IIN,
				e.REG_DATE as REG_DATE
			from MON_NOBD.STUDENT as st
				inner join MON_NOBD.EDUCATION as e on e.STUDENT_ID = st.ID
				inner join MON_NOBD.SCHOOL as s on s.ID = e.SCHOOL_ID 
				inner join MON_NOBD.SCHOOL_ATTR as sattr on sattr.SCHOOL_ID = s.ID
				inner join MON_NOBD.D_TYPE_SCHOOL as ts on ts.ID = sattr.SCHOOL_TYPE_ID
			where s.DATE_CLOSE1 is null and 
				ts.ID = 2 and 
				st.IIN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
				st.IIN is not null) as s on s.IIN = gp.IIN
	where cast(substring(gp.BIRTH_DATE, 1, 4) as int) >= 2014) as vt) as fc
where fc.GOD is not null

-- 2. Загрузка на 17 сервер витрины TEST.FIRST_CLASSES с 5 сервера.
drop table if exists MON_NOBD.FIRST_CLASSES;

create table MON_NOBD.FIRST_CLASSES
(
    GOD Int32,
    IIN String,
    POL Nullable(String),
    BIRTH_DATE Nullable(String),
    BIRTH_YEAR Int32,
    AGE Int32
)
engine = MergeTree
order by IIN
settings index_granularity = 8192;

-- 3. Создание витрины на 17 сервере MON_NOBD.FIRST_CLASSES_2
drop table if exists MON_NOBD.FIRST_CLASSES_2;

create table MON_NOBD.FIRST_CLASSES_2
(
 	GOD Int32,
    IIN String,
    POL Nullable(String),
    BIRTH_DATE Nullable(String),
    BIRTH_YEAR Int32,
    AGE Nullable(Int32),
    KATO_2 Nullable(String),
    KATO_2_NAME Nullable(String),
    KATO_4 Nullable(String),
    KATO_4_NAME Nullable(String),
    KATO_6 Nullable(String),
    KATO_6_NAME Nullable(String)
)
engine = MergeTree
order by IIN
settings index_granularity = 8192;

insert into 
	MON_NOBD.FIRST_CLASSES_2 (GOD, IIN, POL, BIRTH_DATE, BIRTH_YEAR, AGE, KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, KATO_6, KATO_6_NAME)
select
	fc.GOD, 
	fc.IIN, 
	fc.POL, 
	fc.BIRTH_DATE, 
	fc.BIRTH_YEAR, 
	fc.AGE,
	
	if(k.KATO_2 = '' or k.KATO_2 is null, '(без прописки)', k.KATO_2) as KATO_2, 
	if(k.KATO_2_NAME = '' or k.KATO_2_NAME is null, '(без прописки)', k.KATO_2_NAME) as KATO_2_NAME,

	/*if(k.KATO_4 = '' or k.KATO_4 is null, '(без прописки)', k.KATO_4) as KATO_4,*/
	if(pt.KATO_4 in ('', 'null') or pt.KATO_4 is null, substring(pt.KATO_6, 1, 4), 
		if(pt.KATO_4 <> substring(pt.KATO_6, 1, 4), substring(pt.KATO_6, 1, 4), pt.KATO_4)) as KATO_4, 	
	if(k.KATO_4_NAME = '' or k.KATO_4_NAME is null, '(без прописки)', k.KATO_4_NAME) as KATO_4_NAME,

	if(k.KATO_6 = '' or k.KATO_6 is null, '(без прописки)', k.KATO_6) as KATO_6, 
	if(k.KATO_6_NAME = '' or k.KATO_6_NAME is null, '(без прописки)', k.KATO_6_NAME) as KATO_6_NAME
from
	(select distinct
		f.GOD as GOD, 
		f.IIN as IIN, 
		f.POL as POL, 
		f.BIRTH_DATE as BIRTH_DATE, 
		f.BIRTH_YEAR as BIRTH_YEAR, 
		f.AGE as AGE
	from MON_NOBD.FIRST_CLASSES as f) as fc
left join
	(select 
		IIN, 
		KATO_2, KATO_2_NAME,
		KATO_4, KATO_4_NAME,
		KATO_6, FULL_KATO_NAME as KATO_6_NAME
	from SOC_KARTA.KATO_FOR_FAMILY
	where KATO_2 <> '' and KATO_2_NAME <> '') as k on fc.IIN = k.IIN
	
-- 4. На 17 сервере. Подготовка витрины
drop table if exists MON_NOBD.FIRST_CLASSES_2_SCHOOL;

create table MON_NOBD.FIRST_CLASSES_2_SCHOOL
(
    SCHOOL_ID String,
    KATO_2 Nullable(String),
    KATO_2_NAME Nullable(String),
    KATO_4 Nullable(String),
    KATO_4_NAME Nullable(String),
    KATO_6 Nullable(String),
    KATO_6_NAME Nullable(String),
    BIN Nullable(String),
    RU_NAME Nullable(String),
    SHIFTS_CNT Nullable(Int32),
    PLACES_CNT Nullable(Int32),
    DATE_OPEN Nullable(String),
    DATE_CLOSE Nullable(String)
)
engine = MergeTree
order by SCHOOL_ID
settings index_granularity = 8192;

-- 5. На 5 сервере. Надо загрузить на 17 сервер через CSV файл
select 
	SCHOOL_ID,
	if(KATO_2 = '' or KATO_2 is null, '(без прописки)', KATO_2) as KATO_2, 
	if(KATO_2_NAME = '' or KATO_2_NAME is null, '(без прописки)', KATO_2_NAME) as KATO_2_NAME,
	if(KATO_4 = '' or KATO_4 is null, '(без прописки)', KATO_4) as KATO_4, 
	if(KATO_4_NAME = '' or KATO_4_NAME is null, '(без прописки)', KATO_4_NAME) as KATO_4_NAME,
	if(KATO_6 = '' or KATO_6 is null, '(без прописки)', KATO_6) as KATO_6, 
	if(KATO_6_NAME = '' or KATO_6_NAME is null, '(без прописки)', KATO_6_NAME) as KATO_6_NAME,
	BIN,
	RU_NAME,
	SHIFTS_CNT,
	PLACES_CNT,
	DATE_OPEN,
	DATE_CLOSE
from
	(SELECT DISTINCT
	    S.ID as SCHOOL_ID,
	    S.BIN as BIN,
	    substring(AREA.CODE, 1, 2) as KATO_2,
	    upperUTF8(AREA.RNAME) as KATO_2_NAME,
	    substring(REGION.CODE, 1, 4) as KATO_4,
	    upperUTF8(REGION.RNAME) as KATO_4_NAME,
	    substring(LOCALITY.CODE, 1, 6) as KATO_6,
	    upperUTF8(LOCALITY.RNAME) as KATO_6_NAME,
	    S.RU_NAME as RU_NAME,
	    SA.SHIFT_ID as SHIFTS_CNT,
	    SB.SEAT_CNT as PLACES_CNT,
	    SA.DATE_OPEN as DATE_OPEN,
	    SA.DATE_CLOSE as DATE_CLOSE,
	    row_number() over (partition by SCHOOL_ID order by PLACES_CNT desc) as num
	FROM MON_NOBD.SCHOOL AS S
	INNER JOIN MON_NOBD.SCHOOL_ATTR AS SA ON S.ID = SA.SCHOOL_ID AND SA.EDU_PERIOD_ID = 0
	INNER JOIN MON_NOBD.D_REGION AS LOCALITY ON SA.REGION_ID = LOCALITY.ID
	LEFT JOIN MON_NOBD.D_REGION AS AREA ON AREA.ID = LOCALITY.AREA_ID
	LEFT JOIN MON_NOBD.D_REGION AS REGION ON REGION.ID = LOCALITY.DISTRICT_ID
	LEFT JOIN MON_NOBD.SCHOOL_BUILD AS SB ON SA.ID = SB.SCHOOL_ATTR_ID AND SB.BUILD_EDUUSE_ID = 1 AND SB.BUILD_STATE_ID != 4
	LEFT JOIN MON_NOBD.SCHOOL_SPEC AS SS ON SA.ID = SS.SCHOOL_ATTR_ID
	LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE AS SST ON SST.ID = SS.SPEC_TYPE_ID
	WHERE (SST.CODE LIKE '02.1.%'
	    OR SST.CODE LIKE '02.2.%'
	    OR SST.CODE LIKE '02.3.%'
	    OR SST.CODE LIKE '02.4.%'
	    OR SST.CODE LIKE '02.5.%'
	    OR SST.CODE LIKE '07.%'
	    OR SST.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4'))) as sc
where sc.num = 1

-- 6. На 17 сервере. Витрина по возрастам.
drop table if exists MON_NOBD.FIRST_CLASSES_2_AGES;

create table MON_NOBD.FIRST_CLASSES_2_AGES
(
	GOD Int16,
	AGE Nullable(Int16),
	COUNT_IIN Nullable(Int32),
    KATO_2 Nullable(String),
    KATO_2_NAME Nullable(String),
    KATO_4 Nullable(String),
    KATO_4_NAME Nullable(String),
    KATO_6 Nullable(String),
    KATO_6_NAME Nullable(String)
)
ENGINE = MergeTree
order by GOD
settings index_granularity = 8192;

insert into 
	MON_NOBD.FIRST_CLASSES_2_AGES (GOD, AGE, COUNT_IIN, KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, KATO_6, KATO_6_NAME)
select
	GOD,
	datediff(year, toDate(pt.BIRTH_DATE), toDate(concat(toString(GOD), '-12-31'))) as AGE,
	count(pt.IIN) as COUNT_IIN,
	
	if(pt.KATO_2 = '' or pt.KATO_2 is null, '(без прописки)', pt.KATO_2) as KATO_2, 
	if(pt.KATO_2_NAME = '' or pt.KATO_2_NAME is null, '(без прописки)', pt.KATO_2_NAME) as KATO_2_NAME,

	if(pt.KATO_4 = '' or pt.KATO_4 is null, '(без прописки)', pt.KATO_4) as KATO_4, 
	if(pt.KATO_4_NAME = '' or pt.KATO_4_NAME is null, '(без прописки)', pt.KATO_4_NAME) as KATO_4_NAME,

	if(pt.KATO_6 = '' or pt.KATO_6 is null, '(без прописки)', pt.KATO_6) as KATO_6, 
	if(pt.KATO_6_NAME = '' or pt.KATO_6_NAME is null, '(без прописки)', pt.KATO_6_NAME) as KATO_6_NAME
from
	(select distinct
		fl.IIN,
		fl.BIRTH_DATE as BIRTH_DATE,
		k.KATO_2,
		k.KATO_2_NAME,
		k.KATO_4,
		k.KATO_4_NAME,
		k.KATO_6,
		k.KATO_6_NAME
	from MU_FL.GBL_PERSON as fl
		left join 
			(select 
				IIN, 
				KATO_2, KATO_2_NAME, 
				KATO_4, KATO_4_NAME, 
				KATO_6, FULL_KATO_NAME as KATO_6_NAME
			from SOC_KARTA.KATO_FOR_FAMILY
			where KATO_2 <> '' and KATO_2_NAME <> '') as k on k.IIN = fl.IIN) as pt
array join range(2021, 2027, 1) as GOD
group by 
	KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, KATO_6, KATO_6_NAME, 
	GOD, 
	datediff(year, toDate(pt.BIRTH_DATE), toDate(concat(toString(GOD), '-12-31')))
having AGE > 0 and AGE < 21
order by GOD, AGE;

-- 7. На 17 сервере. Витрина по произв. мощности.
drop table if exists MON_NOBD.FIRST_CLASSES_2_PROIZV;

create table MON_NOBD.FIRST_CLASSES_2_PROIZV
(
	GOD Int16,
	COUNT_IIN Nullable(Int32),
	PLACES_CNT Nullable(Int32),
    KATO_2 Nullable(String),
    KATO_2_NAME Nullable(String),
    KATO_4 Nullable(String),
    KATO_4_NAME Nullable(String),
    KATO_6 Nullable(String),
    KATO_6_NAME Nullable(String)
)
ENGINE = MergeTree
ORDER BY GOD
SETTINGS index_granularity = 8192;

insert into 
	MON_NOBD.FIRST_CLASSES_2_PROIZV (GOD, COUNT_IIN, PLACES_CNT, KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, KATO_6, KATO_6_NAME)
select
	a.GOD,
	a.COUNT_IIN,
	p.PLACES_CNT,
	p.KATO_2,
	p.KATO_2_NAME,
	p.KATO_4,
	p.KATO_4_NAME,
	p.KATO_6,
	p.KATO_6_NAME
from 
	(select 
		GOD,
		sum(COUNT_IIN) as COUNT_IIN,
		KATO_2,
		KATO_2_NAME,
		KATO_4,
		KATO_4_NAME,
		KATO_6,
		KATO_6_NAME
	from MON_NOBD.FIRST_CLASSES_2_AGES
	where AGE >= 6 and AGE <= 17
	group by GOD, KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, KATO_6, KATO_6_NAME) as a
left join
	(select
		sc.GOD,
		sumIf(sc.PLACES_CNT, sc.IS_COUNT = 1) as PLACES_CNT,
		sc.KATO_2 as KATO_2,
		sc.KATO_2_NAME as KATO_2_NAME,
		sc.KATO_4 as KATO_4,
		sc.KATO_4_NAME as KATO_4_NAME,
		sc.KATO_6 as KATO_6,
		sc.KATO_6_NAME as KATO_6_NAME		
	from
		(select
			*,
			GOD,
			cast(substring(DATE_OPEN, 1, 4) as int) YEAR_OPEN,
			cast(substring(if(DATE_CLOSE = '', '0000', DATE_CLOSE), 1, 4) as int) YEAR_CLOSE,
			if((YEAR_CLOSE > GOD or YEAR_CLOSE = 0) and YEAR_OPEN <= GOD, 1, 0) as IS_COUNT
		from MON_NOBD.FIRST_CLASSES_2_SCHOOL
		array join range(2021, 2027, 1) as GOD) as sc
	group by sc.GOD, 
		sc.KATO_2, sc.KATO_2_NAME, 
		sc.KATO_4, sc.KATO_4_NAME, 
		sc.KATO_6, sc.KATO_6_NAME) as p on 
			a.GOD = p.GOD and
			a.KATO_2 = p.KATO_2 and
			a.KATO_2_NAME = p.KATO_2_NAME and
			a.KATO_4 = p.KATO_4 and
			/*a.KATO_4_NAME = p.KATO_4_NAME and*/
			a.KATO_6 = p.KATO_6 and 
			a.KATO_6_NAME = p.KATO_6_NAME;