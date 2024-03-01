select DISTINCT ON (sf.IIN) 
bo.NAME_ORG as NAME_ORG,
bo.BIN as BIN,
sf.IIN as IIN,
sf.BIRTH_DATE_IIN as BIRTH_DATE, 
sf.AGE as AGE, 
sfm.SEX_ID as SEX_ID,
sf.INCOME_ASP_IIN as ASP, 
sf.INCOME_ESP_IIN as ESP, 
sf.INCOME_OOP_3_IIN as ZP_3, 
sf.INCOME_OOP_12_IIN as  ZP_12, 
sf.INCOME_LPH_IIN as INCOME_LPH, 
sf.CNT_LPH_IIN as  CNT_LPH, 
sf.CNT_DV_IIN as CNT_AUTO, 
sf.CNT_DV_COM_IIN as CNT_COM_AUTO, 
sf.CNT_NEDV_ZU_IIN as  CNT_ZEM, 
sf.PLOSH_ZEM as PLOSH_ZEM, 
sf.CNT_NEDV_IIN as CNT_NEDV,
sf.CNT_NEDV_COM_IIN as CNT_COM_NEDV, 
sf.CNT_IP_IIN as IP, 
sf.CNT_UL_IIN as TOO, 
sf.NEED_NEDV_IIN as NEED_NEDV, 
sf.IS_VILLAGE_IIN as VILLAGE_CITY,
sf.SQ_MET_IIN as SQ_MET, 
sf.SQ_MET_COM_IIN as SQ_MET_COM,
sf.IS_STUDENT_IIN as STUDENT,
sf.IS_INVALID_IIN as INVAILD,
sf.IS_PREGNANT_IIN as PREGNANT, 
sf.GKB_COUNT_CREDIT as CNT_CREDIT, 
sf.GKB_AMOUNT as AMOUNT_CREDIT, 
sf.GKB_DEBT_VALUE as DEBT_VALUE,
sf.GKB_DEBT_PASTDUE_VALUE as DEBT_PASTDUE_VALUE,
sf.GKB_PAYMENT_DAYS_OVERDUE as PAYMENT_DAYS_OVERDUE
from (select DISTINCT ON (BIN) BIN, NAME_ORG from SOC_KARTA.BIN_ORGANIZATION) bo 
join SOC_KARTA.OPV_ZP_12 oz on bo.BIN = oz.BIN
join SOC_KARTA.SK_FAMILY_QUALITY_IIN sf on oz.RNN = sf.IIN
left join SOC_KARTA.SK_FAMILY_MEMBER sfm on sf.IIN = sfm.IIN

-- 0 витрина для доходов нашего БИН
DROP TABLE IF EXISTS TEST.OPV_IIN_BIN;
CREATE TABLE TEST.OPV_IIN_BIN
	(IIN String,
	BIN Nullable(String),
	PERIOD Nullable(String),
	ZP Nullable(Float64))
ENGINE = MergeTree
ORDER BY IIN
SETTINGS index_granularity = 8192
	AS
SELECT 
  DISTINCT l.RNN as IIN, 
  d.P_RNN as BIN,
  COALESCE(l.PERIOD, d.PERIOD) AS PERIOD, 
  l.AMOUNT * 10 as AMOUNT
FROM 
  (
    SELECT 
      RNN, 
      PERIOD, 
      concat(substring(PERIOD, 3, 6), '-', substring(PERIOD, 1, 2), '-01') as PERIOD_01, 
      AMOUNT, 
      MH 
    FROM 
      MTSZN_PAYSYS.C_SDU_PAY_LIST 
    WHERE 
      PERIOD_01 >= '2020-10-01' and PERIOD_01 <= '2024-02-28' -- период не забыть указать!!!
      and ERROR is null
  ) l 
  INNER JOIN (
    SELECT 
      SUBSTRING(PAY_DATE, 1, 10) as PAY_DATE, 
      MH, 
      P_RNN, 
      KNP, 
      PERIOD 
    FROM 
      MTSZN_PAYSYS.C_SDU_PAY_DOC 
    where 
      PAY_DATE >= '2020-10-01' and PAY_DATE <= '2024-02-28' -- период не забыть указать!!!
      and KNP = '010' 
      and ASSIGN not like '%ESP%' 
      and P_ACCOUNT not in (
        '150A1F36881C6E513A1C0A2400C6B8531330393617FAA3DE0D58F3001217AE99', 
        'B168FB00624D8CAEFC9631007E7502363A03D4A76A3D3BD251601DE3777BCB81', 
        'A718560E2DE5A61EF8F8A5CBC015E9D89604C165C395A7A1A3297E9A9950771C', 
        '1A98C49B4AC371F2C6A0CB3A3FEA8CC745B658E8B2CD29E62F279E0A182CF4E3') 
      and MH in (SELECT ID FROM MTSZN_PAYSYS.MT_HEAD where STATE = 5)
  ) d on d.MH = l.MH 
where (COALESCE(PERIOD, d.PERIOD) like '%2021' or 
		COALESCE(PERIOD, d.PERIOD) like '%2022' or 
		COALESCE(PERIOD, d.PERIOD) like '%2023') and 
		d.P_RNN = '278B77D976AA62570C0EB4A4827F865C7A533AADF5AF402E4C04D472C0796C43'
GROUP BY RNN, PERIOD, AMOUNT, MH, BIN

-- 0 витрина для наших ИИНок с нашего БИН, которые получалаи доход с других БИН
DROP TABLE IF EXISTS TEST.OPV_IIN_NOT_BIN;
CREATE TABLE TEST.OPV_IIN_NOT_BIN
	(IIN String,
	ZP Nullable(Float64),
	BIN Nullable(String),
	PERIOD Nullable(String),
	RN Nullable(Int32))
ENGINE = MergeTree
ORDER BY IIN
SETTINGS index_granularity = 8192
	AS
SELECT 
	l.RNN as IIN,
	l.AMOUNT * 10 as ZP,
	d.P_RNN as BIN,
	COALESCE(l.PERIOD, d.PERIOD) AS PERIOD, 
	ROW_NUMBER() OVER (partition by l.RNN order by toInt32(PERIOD) desc) as RN
FROM 
  (SELECT 
      RNN, PERIOD, 
      concat(substring(PERIOD, 3, 6), '-', substring(PERIOD, 1, 2), '-01') as PERIOD_01, 
      AMOUNT, MH 
    FROM 
      MTSZN_PAYSYS.C_SDU_PAY_LIST 
    WHERE 
      PERIOD_01 >= '2020-10-01' and PERIOD_01 <= '2024-02-28' -- период не забыть указать!!!
      and ERROR is null
  ) l 
  INNER JOIN (
    SELECT 
      SUBSTRING(PAY_DATE, 1, 10) as PAY_DATE, 
      MH, P_RNN, 
      KNP, PERIOD 
    FROM 
      MTSZN_PAYSYS.C_SDU_PAY_DOC 
    where 
      PAY_DATE >= '2020-10-01' and PAY_DATE <= '2024-02-28' -- период не забыть указать!!!
      and KNP = '010' 
      and ASSIGN not like '%ESP%' 
      and P_ACCOUNT not in (
        '150A1F36881C6E513A1C0A2400C6B8531330393617FAA3DE0D58F3001217AE99', 
        'B168FB00624D8CAEFC9631007E7502363A03D4A76A3D3BD251601DE3777BCB81', 
        'A718560E2DE5A61EF8F8A5CBC015E9D89604C165C395A7A1A3297E9A9950771C', 
        '1A98C49B4AC371F2C6A0CB3A3FEA8CC745B658E8B2CD29E62F279E0A182CF4E3') 
      and MH in (SELECT ID FROM MTSZN_PAYSYS.MT_HEAD where STATE = 5)
  ) d on d.MH = l.MH 
where l.RNN in (SELECT distinct ozp.IIN
				FROM TEST.OPV_IIN_BIN as ozp
				WHERE ozp.PERIOD like '%2021' and ozp.PERIOD like '%2022' or ozp.PERIOD like '%2023') and -- Люди из нашего нашего БИН!!!
	d.P_RNN <> '278B77D976AA62570C0EB4A4827F865C7A533AADF5AF402E4C04D472C0796C43' and -- Не наш БИН!!!
	(COALESCE(PERIOD, d.PERIOD) like '%2021' or 
	 COALESCE(PERIOD, d.PERIOD) like '%2022' or 
	 COALESCE(PERIOD, d.PERIOD) like '%2023') 
GROUP BY RNN, PERIOD, AMOUNT, MH, BIN

-- 1 вкладка
SELECT 
	zp.IIN, 
	zp.ZP as ZP_SUM,
	zp.PERIOD
FROM TEST.OPV_IIN_BIN as zp
WHERE zp.PERIOD like '%2021' or zp.PERIOD like '%2022' or zp.PERIOD like '%2023'
order by zp.IIN, toInt32(SUBSTRING(zp.PERIOD, 3, 4)), toInt32(SUBSTRING(zp.PERIOD, 1, 2))

SELECT 
	zp.RNN as IIN, 
	zp.AMOUNT * 10 as ZP_SUM,
	zp.PERIOD
FROM SK_FAMILY.OPV_IIN_YEAR_NEW as zp
WHERE zp.BIN = '278B77D976AA62570C0EB4A4827F865C7A533AADF5AF402E4C04D472C0796C43' and (zp.PERIOD like '%2022' or zp.PERIOD like '%2023')
order by zp.RNN, toInt32(zp.PERIOD)

-- Вкладка Распределение доходов
select 
	COUNT(IIN) as CNT_IIN,
	SUM(ZP_200) as ZP_200,
	SUM(ZP_500) as ZP_500,
	SUM(ZP_MLN) as ZP_MLN,
	SUM(ZP_B_MLN) as ZP_B_MLN
from 
	(select 
		z.IIN,
		CASE WHEN z.ZP_SUM < 200000 THEN 1 ELSE 0 END as ZP_200,
		CASE WHEN z.ZP_SUM > 200000 and z.ZP_SUM <= 500000 THEN 1 ELSE 0 END as ZP_500,
		CASE WHEN z.ZP_SUM > 500000 and z.ZP_SUM <= 1000000 THEN 1 ELSE 0 END as ZP_MLN,
		CASE WHEN z.ZP_SUM > 1000000 THEN 1 ELSE 0 END as ZP_B_MLN
	from
		(SELECT 
			RNN as IIN, 
			SUM(AMOUNT * 10) as ZP_SUM
		FROM SK_FAMILY.OPV_IIN_YEAR_NEW as zp
		WHERE zp.BIN = '278B77D976AA62570C0EB4A4827F865C7A533AADF5AF402E4C04D472C0796C43' and (PERIOD like '%2022' or PERIOD like '%2023')
		GROUP BY RNN) as z) as vt

-- 2 вариант		
SELECT 
	ozp.PERIOD,
	COUNT(ozp.IIN) as CNT, 
	SUM(CASE WHEN ozp.ZP < 200000 THEN 1 ELSE 0 END) as ZP_200,
	SUM(CASE WHEN ozp.ZP > 200000 and ozp.ZP <= 500000 THEN 1 ELSE 0 END) as ZP_500,
	SUM(CASE WHEN ozp.ZP > 500000 and ozp.ZP <= 1000000 THEN 1 ELSE 0 END) as ZP_MLN,
	SUM(CASE WHEN ozp.ZP > 1000000 THEN 1 ELSE 0 END) as ZP_B_MLN
FROM TEST.OPV_IIN_BIN as ozp
WHERE ozp.PERIOD like '%2023'
GROUP BY ozp.PERIOD

-- вкладка Дубляж работ
with 
IIN_BIN as 
	(SELECT distinct
		zp.RNN as IIN
	FROM SK_FAMILY.OPV_IIN_YEAR_NEW as zp
	WHERE zp.BIN = '278B77D976AA62570C0EB4A4827F865C7A533AADF5AF402E4C04D472C0796C43' and 
		(zp.PERIOD like '%2022' or zp.PERIOD like '%2023')),
OPV as 
	(select 
		i.IIN,
		opv.AMOUNT * 10 as  ZP,
		opv.BIN,
		ROW_NUMBER() OVER (partition by opv.RNN order by toInt32(opv.PERIOD) desc) as RN
	from IIN_BIN as i
	join SK_FAMILY.OPV_IIN_YEAR_NEW as opv on 
		opv.RNN = i.IIN and 
		opv.BIN <> '278B77D976AA62570C0EB4A4827F865C7A533AADF5AF402E4C04D472C0796C43'
	where opv.PERIOD like '%2022' or opv.PERIOD like '%2023'),
BIN_R as 
	(select 
		IIN,
		SUM(ZP) as SUM_ZP,
		AVG(ZP) as AVG_ZP,
		COUNT(*) as CNT,
		MAX(ZP) as MAX_ZP,
		MIN(ZP) as MIN_ZP,
		COUNT(distinct BIN) as CNT_BIN
	from OPV
	group by IIN)
select 
	bin.IIN,
	vt.BIN,
	bin.CNT_BIN,
	bin.SUM_ZP,
	bin.AVG_ZP,
	bin.CNT,
	bin.MAX_ZP,
	bin.MIN_ZP
from BIN_R as bin  
join (select IIN, BIN from OPV as op where op.RN = 1) as vt on vt.IIN = bin.IIN

-- 2 вариант
with 
IIN_BIN as 
	(SELECT distinct ozp.IIN
	FROM TEST.OPV_IIN_BIN as ozp
	WHERE ozp.PERIOD like '%2021' and ozp.PERIOD like '%2022' or ozp.PERIOD like '%2023'),
OPV as 
	(SELECT 
		IIN,
		ZP,
		BIN,
		PERIOD, 
		RN
	FROM TEST.OPV_IIN_NOT_BIN),
BIN_R as 
	(select 
		IIN,
		SUM(ZP) as SUM_ZP,
		AVG(ZP) as AVG_ZP,
		COUNT(*) as CNT,
		MAX(ZP) as MAX_ZP,
		MIN(ZP) as MIN_ZP,
		COUNT(distinct BIN) as CNT_BIN
	from OPV
	group by IIN)
select 
	bin.IIN,
	vt.BIN,
	bin.CNT_BIN,
	bin.SUM_ZP,
	bin.AVG_ZP,
	bin.CNT,
	bin.MAX_ZP,
	bin.MIN_ZP
from BIN_R as bin  
join (select IIN, BIN from OPV as op where op.RN = 1) as vt on vt.IIN = bin.IIN

-- вкладка Статусы
with 
IIN_BIN as 
	(SELECT distinct
		zp.RNN as IIN
	FROM SK_FAMILY.OPV_IIN_YEAR_NEW as zp
	WHERE zp.BIN = '278B77D976AA62570C0EB4A4827F865C7A533AADF5AF402E4C04D472C0796C43' and 
		(zp.PERIOD like '%2022' or zp.PERIOD like '%2023')),
SRS as 
	(select * from
	   (select IIN,
			   STATUS_ID,
			   CATEGORY_ID,
			   ROW_NUMBER() OVER (PARTITION BY IIN
								  ORDER BY ID DESC) as RN
		from SK_FAMILY.SR_PERSON_SOURCE
		where STATUS_ID in (16,
							238,
							32,
							318,
							315,
							1,
							2,
							314,
							11,
							12,
							240,
							15)
		  and CATEGORY_ID in (99,
							  100,
							  125,
							  107))
	 where RN = 1)
SELECT i.IIN,
	 case
		 when STATUS_ID = 16 then 1
		 else 0
	 end AS beremennye,
	 case
		 when STATUS_ID = 238 then 1
		 else 0
	 end AS inostr_grajd,
	 case
		 when STATUS_ID = 32 then 1
		 else 0
	 end AS naem_rabotniki,
	 case
		 when STATUS_ID = 318 then 1
		 else 0
	 end AS bezhency,
	 case
		 when STATUS_ID = 315 then 1
		 else 0
	 end AS poluch_posobii,
	 case
		 when STATUS_ID = 1 then 1
		 else 0
	 end AS pernsioner,
	 case
		 when STATUS_ID = 2 then 1
		 else 0
	 end AS veteran_vov,
	 case
		 when STATUS_ID = 314 then 1
		 else 0
	 end AS umer_ispolnenii,
	 case
		 when STATUS_ID = 11 then 1
		 else 0
	 end AS invalidnost,
	 case
		 when STATUS_ID = 12 then 1
		 else 0
	 end AS uhod_inv,
	 case
		 when CATEGORY_ID in (125, 107) then 1
		 else 0
	 end AS det_bez_popech,
	 case
		 when STATUS_ID = 240 then 1
		 else 0
	 end AS mnogodet,
	 case
		 when CATEGORY_ID in (99, 100) then 1
		 else 0
	 end AS poluch_asp,
	 case
		 when STATUS_ID = 15 then 1
		 else 0
	 end AS kandas
  FROM IIN_BIN as i 
  left join SRS as s on s.IIN = i.IIN
 
-- 2 вариант
with 
IIN_BIN as 
	(SELECT DISTINCT zp.IIN
	FROM TEST.OPV_IIN_BIN as zp
	WHERE (zp.PERIOD like '%2021') or (zp.PERIOD like '%2022') or (zp.PERIOD like '%2023') ),
SRS as 
	(select * from
	   (select IIN,
			   STATUS_ID,
			   CATEGORY_ID,
			   ROW_NUMBER() OVER (PARTITION BY IIN, STATUS_ID, CATEGORY_ID
								  ORDER BY ID DESC) as RN
		from SK_FAMILY.SR_PERSON_SOURCE
		where STATUS_ID in (16,
							238,
							32,
							318,
							315,
							1,
							2,
							314,
							11,
							12,
							240,
							15)
		  OR CATEGORY_ID in (99,
							  100,
							  125,
							  107))
	 where RN = 1),
SRS_R as 
	(SELECT DISTINCT 
		i.IIN,
		 case
			 when STATUS_ID = 16 then 1
			 else 0
		 end AS beremennye,
		 case
			 when STATUS_ID = 238 then 1
			 else 0
		 end AS inostr_grajd,
		 case
			 when STATUS_ID = 32 then 1
			 else 0
		 end AS naem_rabotniki,
		 case
			 when STATUS_ID = 318 then 1
			 else 0
		 end AS bezhency,
		 case
			 when STATUS_ID = 315 then 1
			 else 0
		 end AS poluch_posobii,
		 case
			 when STATUS_ID = 1 then 1
			 else 0
		 end AS pernsioner,
		 case
			 when STATUS_ID = 2 then 1
			 else 0
		 end AS veteran_vov,
		 case
			 when STATUS_ID = 314 then 1
			 else 0
		 end AS umer_ispolnenii,
		 case
			 when STATUS_ID = 11 then 1
			 else 0
		 end AS invalidnost,
		 case
			 when STATUS_ID = 12 then 1
			 else 0
		 end AS uhod_inv,
		 case
			 when CATEGORY_ID in (125, 107) then 1
			 else 0
		 end AS det_bez_popech,
		 case
			 when STATUS_ID = 240 then 1
			 else 0
		 end AS mnogodet,
		 case
			 when CATEGORY_ID in (99, 100) then 1
			 else 0
		 end AS poluch_asp,
		 case
			 when STATUS_ID = 15 then 1
			 else 0
		 end AS kandas
	  FROM IIN_BIN as i 
	  left join SRS as s on s.IIN = i.IIN)
select 
	IIN,
	sum(beremennye) as beremennye,
	sum(inostr_grajd) as inostr_grajd,
	sum(naem_rabotniki) as naem_rabotniki,
	sum(bezhency) as bezhency,
	sum(poluch_posobii) as poluch_posobii,
	sum(pernsioner) as pernsioner,
	sum(veteran_vov) as veteran_vov,
	--sum(umer_ispolnenii) as umer_ispolnenii,
	sum(invalidnost) as invalidnost,
	sum(uhod_inv) as uhod_inv,
	--sum(det_bez_popech) as det_bez_popech,
	sum(mnogodet) as mnogodet,
	sum(poluch_asp) as poluch_asp,
	sum(kandas) as kandas
from SRS_R as s 
group by s.IIN
  
-- динамика ЗП по месяцам
with zp as
	(SELECT 
		zp.RNN as IIN, 
		zp.AMOUNT * 10 as ZP,
		SUBSTRING(zp.PERIOD, 1, 2) as MES
	FROM SK_FAMILY.OPV_IIN_YEAR_NEW as zp
	WHERE zp.BIN = '278B77D976AA62570C0EB4A4827F865C7A533AADF5AF402E4C04D472C0796C43' and (zp.PERIOD like '%2023'))
select 
	IIN,
	sum(case when MES = '01' then ZP else 0 end) as m_01,
	sum(case when MES = '02' then ZP else 0 end) as m_02,
	sum(case when MES = '03' then ZP else 0 end) as m_03,
	sum(case when MES = '04' then ZP else 0 end) as m_04,
	sum(case when MES = '05' then ZP else 0 end) as m_05,
	sum(case when MES = '06' then ZP else 0 end) as m_06,
	sum(case when MES = '07' then ZP else 0 end) as m_07,
	sum(case when MES = '08' then ZP else 0 end) as m_08,
	sum(case when MES = '09' then ZP else 0 end) as m_09,
	sum(case when MES = '10' then ZP else 0 end) as m_10,
	sum(case when MES = '11' then ZP else 0 end) as m_11,
	sum(case when MES = '12' then ZP else 0 end) as m_12
from zp
group by IIN

-- 2 вариант
with zp as
	(SELECT 
		zp.IIN, 
		zp.ZP,
		SUBSTRING(zp.PERIOD, 1, 2) as MES
	FROM TEST.OPV_IIN_BIN as zp
	WHERE zp.PERIOD like '%2023')
select 
	IIN,
	sum(case when MES = '01' then ZP else 0 end) as m_01,
	sum(case when MES = '02' then ZP else 0 end) as m_02,
	sum(case when MES = '03' then ZP else 0 end) as m_03,
	sum(case when MES = '04' then ZP else 0 end) as m_04,
	sum(case when MES = '05' then ZP else 0 end) as m_05,
	sum(case when MES = '06' then ZP else 0 end) as m_06,
	sum(case when MES = '07' then ZP else 0 end) as m_07,
	sum(case when MES = '08' then ZP else 0 end) as m_08,
	sum(case when MES = '09' then ZP else 0 end) as m_09,
	sum(case when MES = '10' then ZP else 0 end) as m_10,
	sum(case when MES = '11' then ZP else 0 end) as m_11,
	sum(case when MES = '12' then ZP else 0 end) as m_12
from zp
group by IIN