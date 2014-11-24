drop table nav_result;
create table nav_result{
	name varchar(30),
	id int,
	mask int,
	description varchar(200)
}
drop table time_src;
create table time_src{
	name varchar(30),
	id int,
	description varchar(200)
}

--
drop table lsl_fix;
create table if not exists lsl_fix (
	session varchar(15),
	app varchar(15)
);
truncate table lsl_fix;
copy lsl_fix (time filler varchar(20), uncertainty filler varchar(20), source filler varchar(20), primaryid filler varchar(20), type filler varchar(20), session, app) from local 'C:\\Users\\cheyi\\git\\log.analysis\\log.analysis\\testoutput\\output.pde1.fix' delimiter ',' direct;

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
	Session varchar(15),
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
	VisibleSVs	int,
	StrongMeas	int, 
	RawSource	int, --solution source
	InitSource	int, --solution source
	Cov0	float,
	Cov1	float,
	Cov2	float,
	Cov3	float,
	Cov4	float,
	Cov5	float,
	SM_A	float,
	SM_B	float,
	SM_V	float,
	SM_Theta	float,
	TrueClock	float,
	OutputSource	int, --solution source
	EDOP	float,
	NDOP	float,
	VDOP	float,
	SMMode	float,
	SMLat	float,
	SMLon	float,
	SMHgt	float,
	VNavResult	float,
	Vmeas	int,
	VUnitFault	float,
	Ve	float,
	Vn	float,
	Vu	float,
	NavEngine int,	
	Pilots	int, 
	IniLat	float,
	IniLon	float,
	IHgt	float,
	PrimaryID	varchar(20),
	Week	int,
	dt	TIMESTAMP,	
	SmEast	float,
	SmNorth	float,
	SmUp	float,
	Final	boolean,
	FlagH	varchar(10),
	FlagO	varchar(10),
	File	varchar(10),
	RecordSize	int,
	MarketID	varchar(10), --LAC
	SwitchNo	varchar(10), --RNC, switchno
	ServingCellID	varchar(10), -- cellid
	StbUsed	boolean,
	StbCalced	float,
	StbCalcedSigma	float,
	StbConsUsed	boolean,
	StbConsValue	float,
	StbConsSigma	float,
	NID	varchar(10), --mnc, network code, network id
	SID	varchar(10), --mcc, country code, system id
	HandsetCapability varchar(20),
	PosAidUsed	int,
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
	DeltaAz float
);


truncate table lsl_csv;
copy lsl_csv (MeasTime,Valid,Lat,Lon,HAE,RawEast,RawNorth,RawUp,dSFT,Measurements,NavResult,UnitFault,EEast,ENorth,EUp,Hepe,Fix,FixesRequested,SmoothFixes,ReferencePN,ServingBSID,Session,TSource,MeasBandFreq,Differential,Aided,Checked,PltPower,GPS,TUncertainty,ClockOffset,ClockDrift, VisibleSVs,StrongMeas,RawSource,InitSource,Cov0,Cov1,Cov2,Cov3,Cov4,Cov5,SM_A,SM_B,SM_V,SM_Theta,TrueClock,OutputSource,EDOP,NDOP,VDOP,SMMode,SMLat,SMLon,SMHgt,VNavResult,Vmeas,VUnitFault,Ve,Vn,Vu,NavEngine,Pilots,IniLat,IniLon,IHgt,PrimaryID,Week,dt format 'YYYY/MM/DD-HH:MI:SS.MS',SmEast,SmNorth,SmUp,Final,FlagH,FlagO,File,RecordSize,MarketID,SwitchNo,ServingCellID,StbUsed,StbCalced,StbCalcedSigma,StbConsUsed,StbConsValue,StbConsSigma,NID,SID,HandsetCapability,PosAidUsed,GLONASS,VEEast,VENorth,VEup,HEVE,ValidPosTruth,TruthLat,TruthLong,TruthHAE,TruthHEPE,ValidVelTruth,TruthVe,TruthVn,TruthVup,TruthHEVE,DeltaVe,DeltaVn,DeltaVup,TruthSpeed, MeasSpeed,DeltaSpeed, TruthAz, MeasAz, DeltaAz) from local 'C:\\Users\\cheyi\\git\\log.analysis\\log.analysis\\testoutput\\output.pde1.csv' delimiter ',' direct;

