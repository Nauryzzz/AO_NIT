with susn as ( 
	SELECT distinct sps.IIN as IIN,
		  case when CODE_1 = '001000' then 1 else 0 end AS pensioner,
		  case when CODE_1 in ('002000','003000') then 1 else 0 end AS veteran_vov,
		  case when CODE_1 in ('011100','011200','011300','011400','011401','011402','011403') then 1 else 0 end AS invalidnost,
		  case when CODE_1 = '012001' then 1 else 0 end AS uhod_inv,
		  case when CODE_1 = '027007' then 1 else 0 end AS det_bez_popech,
		  case when CODE_1 = '039000' then 1 else 0 end AS mnogodet,
		  case when CODE_1 = '026000' then 1 else 0 end AS poluch_asp,
		  case when CODE_1 = '015000' then 1 else 0 end AS kandas
   FROM (select * from
	        (select IIN, CODE_1, ROW_NUMBER() OVER (PARTITION BY IIN, CODE_1 ORDER BY DSTART DESC) AS RN
	         from SOC_KARTA.SR_PERSON_SOURCE)
      where RN = 1
      	and CODE_1 in ('011100','011200','011300','011400','011401','011402',
                       '011403','001000','002000','003000','012001','027007',
                       '039000','026000','015000')) as sps
	WHERE pensioner = 1 or veteran_vov = 1 or invalidnost = 1 or 
		uhod_inv = 1 or det_bez_popech = 1 or mnogodet = 1 or 
		poluch_asp = 1 or kandas = 1)
select
	count(distinct case when sfqi.IS_VILLAGE_IIN = 0 then sfqi.ID_SK_FAMILY_QUALITY2 else null end) as cnt_fam_gorod,
	count(distinct case when sfqi.IS_VILLAGE_IIN = 1 then sfqi.ID_SK_FAMILY_QUALITY2 else null end) as cnt_fam_selo,
	count(case when sfqi.IS_VILLAGE_IIN = 0 then sfqi.IIN else null end) as cnt_iin_gorod,
	count(case when sfqi.IS_VILLAGE_IIN = 1 then sfqi.IIN else null end) as cnt_iin_selo
from SOC_KARTA.SK_FAMILY_QUALITY_IIN3 as sfqi 
where sfqi.IIN in (select IIN from susn)