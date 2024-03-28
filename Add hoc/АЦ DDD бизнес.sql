drop table if exists CKB_NEW.AC_BIZNES;
create table CKB_NEW.AC_BIZNES (
    IIN_HASH String,
    REGION_NAME Nullable(String),
    KPN Nullable(Float64),
    IPN Nullable(Float64),
    SN Nullable(Float64),
    NAL_I Nullable(Float64),
    NDS Nullable(Float64),
    AKCIZ Nullable(Float64),
    NAL_GAME Nullable(Float64),
    GOS_TASK Nullable(Float64),
	EMPLOYEES_COUNT1 Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY IIN_HASH
SETTINGS index_granularity = 8192
as
with 
bin as
	(select distinct
		IIN_BIN,
		REGION_NAME
	from CKB_NEW.POKAZ_PM_TOO_IP_INFO),
pza as
	(select 
		m.IIN_BIN,
		m.REGION_NAME,
		sum(p.KPN) as KPN,
		sum(p.IPN) as IPN,
		sum(p.SN) as SN,
		sum(p.NAL_I) as NAL_I,
		sum(NDS) as NDS,
		sum(AKCIZ) as AKCIZ,
		sum(NAL_GAME) as NAL_GAME,
		sum(GOS_TASK) as GOS_TASK
	from bin as m 
	join CKB_NEW.POKAZ_PM_KOD_GOS_CLASSIFICATION as p on p.IIN_HASH = m.IIN_BIN
	group by m.IIN_BIN, m.REGION_NAME),
ecnt as
	(select 
		e.BIN,
		sum(toInt32(if(EMPLOYEES_COUNT1 = '', '0', EMPLOYEES_COUNT1))) as EMPLOYEES_COUNT1
	from CKB_NEW.POKAZ_PM_EMPLOYEE_INFO as e
	group by e.BIN
	having EMPLOYEES_COUNT1 > 0)
select 
	IIN_BIN,
	REGION_NAME,
	ifnull(KPN, 0) as KPN,
	ifnull(IPN, 0) as IPN,
	ifnull(SN, 0) as SN,
	ifnull(NAL_I, 0) as NAL_I,
	ifnull(NDS, 0) as NDS,
	ifnull(AKCIZ, 0) as AKCIZ,
	ifnull(NAL_GAME, 0) as NAL_GAME,
	ifnull(GOS_TASK, 0) as GOS_TASK,
	ifnull(EMPLOYEES_COUNT1, 0) as EMPLOYEES_COUNT1
from pza 
left join ecnt on ecnt.BIN = pza.IIN_BIN