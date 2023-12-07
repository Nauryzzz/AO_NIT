/* 45. Безработный выпускник УЗ */
with 

gbl_pers as 
	(select distinct
		gp.IIN as IIN
	from MU_FL.GBL_PERSON as gp
	where date_diff(year, toDateTime64(BIRTH_DATE, 0), today()) >= 16 and 
		gp.REMOVED = 0 and 
		gp.PERSON_STATUS_ID in (1, 2) and 
		(gp.EXCLUDE_REASON_ID is null or gp.EXCLUDE_REASON_ID = 1) and
		gp.CITIZENSHIP_ID  = 105),
		  
trud_dogovor as 
	(select distinct 
		e.IIN as IIN
	from MTSZN_ESUTD.EMPLOYEE as e
		inner join MTSZN_ESUTD.CONTRACT as c on c.EMPLOYEE_ID = e.ID 
	where c.TERMINATION_DATE is null),
	
ofic_bezrab as
	(select distinct 
		pers.IIN as IIN
	from
		(select distinct
			card.PA_CARD_ID as PA_CARD_ID,
			card.CODE_IIN as IIN
		from
			(select 
				PA_CARD_ID,
				upper(CODE_IIN) as CODE_IIN,
				row_number() over (partition by CODE_IIN order by SDU_LOAD_IN_DT desc) as num
			from MTSZN_EHALYK.C_HSDU_PA_CARD as pc
			where 
				pc.CODE_IIN is not null and
				upper(pc.CODE_IIN) <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8') as card
		where card.num = 1) as pers
	inner join 
		(select distinct 
			enr.PA_CARD_ID
		from MTSZN_EHALYK.C_HSDU_ENROLLMENT as enr
		where enr.DATE_CLOSE = '0000-00-00') as enr
	on pers.PA_CARD_ID = enr.PA_CARD_ID),
	
pensioner as
	(select distinct 
		pers.IIN as IIN
	from 
		(select distinct
			pers.SICID as SICID,
			pers.IIN as IIN
		from
			(select 
				SICID,
				upper(RN) as IIN,
				row_number() over (partition by RN order by REGDATE desc) as num
			from MTSZN_MAIN.C_SDU_PERSON
			where 
				RN is not null and
				upper(RN) <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8') as pers
		where pers.num = 1) as pers
	inner join
		(select distinct
			doc.PNCD_ID as SICID,
			doc.RFPM_ID as RFPM_ID
		from MTSZN_SOLIDARY.C_SDU_PNPD_DOCUMENT as doc
			inner join MTSZN_SOLIDARY.SR_SOURCE as sr on sr.CODE = doc.RFPM_ID
		where
			toYear(toDateTimeOrNull(doc.PNCP_DATE)) = toYear(now()) and 
			toMonth(toDateTimeOrNull(doc.PNCP_DATE)) between (toMonth(now() - interval 3 month)) and (toMonth(now() - interval 1 month)) and 
			sr.CODE in ('08',       /* Базовая пенсионная выплата */
						'0800',     /* Базовая пенсионная выплата */
						'08000001', /* Базовая пенсия */)) as doc
	on pers.SICID = doc.SICID),
	
esp as 
	(select distinct
		RNN as IIN
	from SK_FAMILY.PAYSYS_ESP_IIN_BIN_YEAR esp 
	where esp.PERIOD in (formatDateTime(today(), '%m%Y'), 
						 formatDateTime(date_sub(month, 1, today()), '%m%Y'),
						 formatDateTime(date_sub(month, 2, today()), '%m%Y'))),
						 
nobd as 
	(/*
	 ts.ID: 2 - Организации среднего образования(начального, основного среднего и общего среднего)
	 ts.ID: 3 - Организации технического и профессионального образования
	 ts.ID: 4 - Организации высшего образования
	 ts.ID: 5 - Организации высшего и (или) послевузовского образования
	 
	 e.LEARN_FORM_ID: 1 - очная
	 e.LEARN_FORM_ID: 6 - дневная
	 
	 e.EDU_PERIOD_ID: 0 - Текущий учебный год
	 
	 e.EDUSTATUS_ID: 1 - обучается (студенты)
	 
	 e.EDU_STATUS: 0 - Обучается (школьники)
	 e.EDU_STATUS: 4 - На выбытии из организации образования (школьники)
	*/
	
	select distinct 
		vt2.IIN as IIN
	from
		(select 
			vt1.IIN, 
			vt1.REG_DATE, vt1.OUT_DATE
		from
			(select 
				st.IIN as IIN,
				e.REG_DATE as REG_DATE, e.OUT_DATE as OUT_DATE, 
				row_number() over (partition by st.IIN order by e.REG_DATE desc) as num
			from MON_NOBD.STUDENT as st
				inner join MON_NOBD.EDUCATION as e on e.STUDENT_ID = st.ID 
				inner join MON_NOBD.SCHOOL as s on s.ID = e.SCHOOL_ID 
				inner join MON_NOBD.SCHOOL_ATTR as sattr on sattr.SCHOOL_ID = s.ID
				inner join MON_NOBD.D_TYPE_SCHOOL as ts on ts.ID = sattr.SCHOOL_TYPE_ID
			where s.DATE_CLOSE1 is null and
				e.EDU_PERIOD_ID = 0 and
				((ts.ID = 2 and e.EDU_STATUS in (0, 4)) or 
				 (ts.ID in (3, 4, 5) and e.EDUSTATUS_ID = 1 and e.LEARN_FORM_ID in (1, 6)))
			) as vt1
		where vt1.num = 1) as vt2
	where vt2.REG_DATE is not null and (vt2.OUT_DATE is null or toDate(vt2.OUT_DATE) >= today())),
	
inv as 
	(select distinct 
		pi.RN as IIN
	from MTSZN_CBDIAPP.PATIENT_INFO_ACTUAL as pi
	where pi.RN <> '4EE9CB68BAD1069BBE54103C9FBD957807CDE54A8B4BAC570A9326425D45E7B8' and 
		(toDate(pi.INV_ENDDATE) >= today() or pi.INV_ENDDATE = '0000-00-00')),
		
nobd_3years as 
	(select distinct 
		vt2.IIN as IIN
	from
		(select 
			vt1.IIN, 
			vt1.OUT_DATE
		from
			(select 
				st.IIN as IIN,
				toDate(e.OUT_DATE) as OUT_DATE, 
				row_number() over (partition by st.IIN order by e.REG_DATE desc) as num
			from MON_NOBD.STUDENT as st
				inner join MON_NOBD.EDUCATION as e on e.STUDENT_ID = st.ID) as vt1
		where vt1.num = 1 and vt1.OUT_DATE is not null) as vt2
	where vt2.OUT_DATE between date_sub(year, 3, today()) and today())
	
select
	toString(fm.SK_FAMILY_ID) as SK_FAMILY_ID,
	'filtr59' as filtr, -- безработный выпускник УЗ
	if(count(p45.IIN) > 0, 1, 0) as filtr_value
from
	(select
		gbl_pers.IIN as IIN
	from gbl_pers
		left join pensioner 							on pensioner.IIN 	= gbl_pers.IIN
		left join trud_dogovor							on trud_dogovor.IIN	= gbl_pers.IIN
		left join SK_FAMILY.OPV_ZP_3 as opv				on opv.RNN 			= gbl_pers.IIN
		left join SK_FAMILY.IP_BIN_DEISTVUIUSHIE as ip	on ip.IP			= gbl_pers.IIN
		left join SK_FAMILY.UCHREDITELI as too			on too.UCHR_IIN		= gbl_pers.IIN
		left join nobd 									on nobd.IIN 		= gbl_pers.IIN
		left join esp 									on esp.IIN 			= gbl_pers.IIN
		left join nobd_3years 							on nobd_3years.IIN 	= gbl_pers.IIN		
		left join ofic_bezrab 							on ofic_bezrab.IIN 	= gbl_pers.IIN
	where 
		if(pensioner.IIN 	= '',	null, pensioner.IIN) 	is null and
		if(trud_dogovor.IIN	= '',	null, trud_dogovor.IIN)	is null and
		if(opv.RNN 			= '', 	null, opv.RNN)  		is null and
		if(ip.IP 			= '', 	null, ip.IP)  			is null and
		if(too.UCHR_IIN 	= '', 	null, too.UCHR_IIN)  	is null and
		if(nobd.IIN 		= '', 	null, nobd.IIN)  		is null and
		if(esp.IIN 			= '', 	null, esp.IIN)  		is null and
		if(nobd_3years.IIN 	= '', 	null, nobd_3years.IIN)  is not null and
		if(ofic_bezrab.IIN 	= '', 	null, ofic_bezrab.IIN)  is null) as p45
inner join SK_FAMILY.SK_FAMILY_MEMBER as fm on fm.IIN = p45.IIN
group by toString(fm.SK_FAMILY_ID);