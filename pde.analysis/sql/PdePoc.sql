drop table nav_result;
create table nav_result(
	name varchar(30),
	id int,
	mask int,
	description varchar(200)
)
drop table time_src;
create table time_src(
	name varchar(30),
	id int,
	description varchar(200)
)

--
drop table lsl_fix;
create table if not exists lsl_fix (
	session varchar(15),
	app varchar(15)
);
truncate table lsl_fix;
copy lsl_fix (time filler varchar(20), uncertainty filler varchar(20), source filler varchar(20), primaryid filler varchar(20), type filler varchar(20), session, app) from local 'C:\\Users\\cheyi\\git\\log.analysis\\log.analysis\\testoutput\\output.pde1.fix' delimiter ',' direct;
--
drop table lsl_ses;
create table if not exists lsl_ses (
	dt timestamp,
	uncertainty numeric(10,3),
	source varchar(20),
	primaryid varchar(20),
	fixtype varchar(20),
	sessionid varchar(20),
	appid varchar(15)
);
copy lsl_ses (dt, uncertainty, source, primaryid, fixtype filler, sessionid, appid) from local 'output.pde1.fix' delimiter ',' direct;
--
drop table lsl_min;
create table if not exists lsl_min(
    mdn varchar(20),
    deviceid varchar(20),
	imsi varchar(20),
	model varchar(60),
	euimid varchar(20),
	lastday date,
	techs varchar(20),
	subnumtype varchar(10)
);

drop table lsl_csv;
create table if not exists lsl_csv (
	MeasTime FLOAT,
	Valid BOOLEAN,
	Lat FLOAT,
	Lon FLOAT,
	HAE FLOAT,
	RawEast FLOAT,
	RawNorth FLOAT,
	RawUp FLOAT,
	dSFT FLOAT,
	Measurements int,
	NavResult int, --nav result
	UnitFault float,
	EEast float,
	ENorth float,
	EUp float,
	Hepe float,
	Fix int,
	FixesRequested int, 
	SmoothFixes int, 
	ReferencePN varchar(10),
	ServingBSID varchar(10),
	Session varchar(15) not null,
	TSource int, --time source
	MeasBandFreq varchar(10),
	Differential boolean,
	Aided boolean,
	Checked boolean,
	PltPower int,
	GPS int,
	TUncertainty float,
	ClockOffset float,
	ClockDrift float,
	VisibleSVs int,
	StrongMeas int, 
	RawSource int, --solution source
	InitSource int, --solution source
	Cov0 float,
	Cov1 float,
	Cov2 float,
	Cov3 float,
	Cov4 float,
	Cov5 float,
	SM_A float,
	SM_B float,
	SM_V float,
	SM_Theta float,
	TrueClock float,
	OutputSource int, --solution source
	EDOP float,
	NDOP float,
	VDOP float,
	SMMode float,
	SMLat float,
	SMLon float,
	SMHgt float,
	VNavResult float,
	Vmeas int,
	VUnitFault float,
	Ve float,
	Vn float,
	Vu float,
	NavEngine int,	
	Pilots int, 
	IniLat float,
	IniLon float,
	IHgt float,
	PrimaryID varchar(20) not null,
	Week int,
	dt TIMESTAMP not null,	
	SmEast float,
	SmNorth float,
	SmUp float,
	Final boolean,
	FlagH varchar(10),
	FlagO varchar(10),
	File varchar(10),
	RecordSize int,
	MarketID varchar(10), --LAC
	SwitchNo varchar(10), --RNC, switchno
	ServingCellID varchar(10), -- cellid
	StbUsed boolean,
	StbCalced float,
	StbCalcedSigma float,
	StbConsUsed boolean,
	StbConsValue float,
	StbConsSigma float,
	NID varchar(10), --mnc, network code, network id
	SID varchar(10), --mcc, country code, system id
	HandsetCapability varchar(20),
	PosAidUsed int,
	GLONASS float,
	VEEast float,
	VENorth float,
	VEup float,
	HEVE float,
	ValidPosTruth float,
	TruthLat float,
	TruthLong float,
	TruthHAE float,
	TruthHEPE float,
	ValidVelTruth float,
	TruthVe float,
	TruthVn float,
	TruthVup float,
	TruthHEVE float,
	DeltaVe float,
	DeltaVn float,
	DeltaVup float,
	TruthSpeed float, 
	MeasSpeed float,
	DeltaSpeed float, 
	TruthAz float, 
	MeasAz float, 
	DeltaAz float,
	SiteId varchar(15)
);
create projection prj_pde_primaryid as select primaryid from lsl_csv;
create projection prj_pde_dt as select dt from lsl_csv;
--load data for lsl_csv
truncate table lsl_csv;
truncate table lsl_ses;

copy lsl_csv (MeasTime,Valid,Lat,Lon,HAE,RawEast,RawNorth,RawUp,dSFT,Measurements,NavResult,UnitFault,EEast,ENorth,EUp,Hepe,Fix,FixesRequested,SmoothFixes,ReferencePN,ServingBSID,Session,TSource,MeasBandFreq,Differential,Aided,Checked,PltPower,GPS,TUncertainty,ClockOffset,ClockDrift, VisibleSVs,StrongMeas,RawSource,InitSource,Cov0,Cov1,Cov2,Cov3,Cov4,Cov5,SM_A,SM_B,SM_V,SM_Theta,TrueClock,OutputSource,EDOP,NDOP,VDOP,SMMode,SMLat,SMLon,SMHgt,VNavResult,Vmeas,VUnitFault,Ve,Vn,Vu,NavEngine,Pilots,IniLat,IniLon,IHgt,PrimaryID,Week,dt format 'YYYY/MM/DD-HH:MI:SS.MS',SmEast,SmNorth,SmUp,Final,FlagH,FlagO,File,RecordSize,MarketID,SwitchNo,ServingCellID,StbUsed,StbCalced,StbCalcedSigma,StbConsUsed,StbConsValue,StbConsSigma,NID,SID,HandsetCapability,PosAidUsed,GLONASS,VEEast,VENorth,VEup,HEVE,ValidPosTruth,TruthLat,TruthLong,TruthHAE,TruthHEPE,ValidVelTruth,TruthVe,TruthVn,TruthVup,TruthHEVE,DeltaVe,DeltaVn,DeltaVup,TruthSpeed, MeasSpeed,DeltaSpeed, TruthAz, MeasAz, DeltaAz, SiteId) from local 'xxx.csv' delimiter ',' direct;

alter table lsl_csv add column mdn varchar(20);
update lsl_csv set mdn = substring(primaryid, 7, 10);

copy lsl_min (mdn enclosed by '"',deviceid enclosed by '"',imsi enclosed by '"',model enclosed by '"',euimid enclosed by '"',lastday enclosed by '"' format 'MM/DD/YYYY',techs enclosed by '"',subnumtype enclosed by '"') from local 'C:\projects\TBDA\PDE\pde_log_analysis-publishing\data\batchMINlookup.csv' delimiter ',' direct exceptions 'c:\projects\TBDA\PDE\load.error';

---create the usage of user distribution
create table lsl_user (
	total int,
	primaryid varchar(20)
);
insert into lsl_user select count(*) as total, primaryid from lsl_csv group by primaryid;
commit;

----aggregate the usage for every minute
create table lsl_minute (
	total int,
	mindt timestamp
);
insert into lsl_minute select count(*) as total, timestamp_round(dt,'MI') as mindt from lsl_csv group by mindt;
commit
select mindt from lsl_minute order by total desc limit 30;
select primaryid from lsl_csv where timestamp_round(dt,'MI') in (select mindt from lsl_minute order by total desc limit 30) group by primaryid order by count(primaryid) desc limit 200;
select count(*) as mintotal, timestamp_round(dt,'MI') as min from lsl_csv where primaryid not in (select primaryid from lsl_csv where timestamp_round(dt,'MI') in (select mindt from lsl_minute order by total desc limit 30) group by primaryid order by count(primaryid) desc limit 200) group by min order by mintotal desc limit 30;

---aggregate the usage for every second
create table lsl_second (
  total int,
  secdt timestamp
);
insert into lsl_second select count(*) as total, timestamp_round(dt,'SS') as secdt from lsl_csv group by secdt;  
commit;
select secdt, total from lsl_second order by total desc limit 30;
select count(*) as sectotal, timestamp_round(dt,'SS') as sec from lsl_csv where primaryid not in (select primaryid from lsl_csv where timestamp_round(dt,'SS') in (select secdt from lsl_second order by total desc limit 30) group by primaryid order by count(primaryid) desc limit 200) group by sec order by sectotal desc limit 30;

select primaryid from lsl_csv where timestamp_round(dt,'SS') in (select secdt from lsl_second order by total desc limit 7) group by primaryid order by count(primaryid) desc limit 130;

--- using sps ---
--select duplicates
 select count(*) as cnt, primaryid, session, dt from lsl_csv group by primaryid, session, dt having count(*)>1;
-- 0 rows
--add primary key
create table lsl_csv_sps as select * from lsl_csv;
alter table lsl_csv_sps add primary key (primaryid, session, dt);
select analyze_constraints('lsl_csv_sps');

--for sql analysis
drop table lsl_sps_minute;
create table lsl_sps_minute(
    TASK_START_TIME timestamp NOT NULL,
	totalreq int,
	minute timestamp
);

select ?, count(*) as totalreq, min(dt) as minute from lsl_csv where dt> ? and dt<?

select timestampdiff('hour',clock_timestamp(), max(dt)) from lsl_csv_sps;  -- -237

update lsl_csv_sps set dt = timestampadd('hour', 237+25, dt);

--obfuscate sql
create table lsl_csv_obf as select * from lsl_csv;
X310002022754469F  ==> 555552022754469
update lsl_csv_obf set primaryid = concat('55555',substr(primaryid, 7, 10));
update lsl_csv_obf set primaryid = decode(primaryid,'0','1','1', '2', '2', '3', '3', '4','4','5','5', '6', '6', '7', '7', '8', '8', '9', '9', '0')

update lsl_csv_obf set primaryid = replace(primaryid, '1', 'a');
update lsl_csv_obf set primaryid = replace(primaryid, '2', 'b');
update lsl_csv_obf set primaryid = replace(primaryid, '3', 'c');
update lsl_csv_obf set primaryid = replace(primaryid, '4', 'd');
update lsl_csv_obf set primaryid = replace(primaryid, '5', 'e');
update lsl_csv_obf set primaryid = replace(primaryid, '6', 'f');
update lsl_csv_obf set primaryid = replace(primaryid, '7', 'g');
update lsl_csv_obf set primaryid = replace(primaryid, '8', 'h');
update lsl_csv_obf set primaryid = replace(primaryid, '9', 'i');
update lsl_csv_obf set primaryid = replace(primaryid, '0', 'j');



