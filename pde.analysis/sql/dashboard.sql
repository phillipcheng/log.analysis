--
select primaryid, count(*) as cnt from lsl_csv group by primaryid order by cnt desc limit 25;

select primaryid, count(*) as cnt, dayofyear(dt) as dy from lsl_csv group by primaryid, dy order by dy, cnt desc limit 25;

--
drop table pde_cnt;
create table pde_cnt(
	primaryid varchar(30),
	total int
);
truncate table pde_cnt;
insert into pde_cnt select primaryid, count(*) from lsl_csv group by primaryid;
commit;

insert into lsl_csv select MeasTime,Valid,Lat,Lon,HAE,RawEast,RawNorth,RawUp,dSFT,Measurements,NavResult,UnitFault,EEast,ENorth,EUp,Hepe,Fix,FixesRequested,SmoothFixes,ReferencePN,ServingBSID,Session,TSource,MeasBandFreq,Differential,Aided,Checked,PltPower,GPS,TUncertainty,ClockOffset,ClockDrift, VisibleSVs,StrongMeas,RawSource,InitSource,Cov0,Cov1,Cov2,Cov3,Cov4,Cov5,SM_A,SM_B,SM_V,SM_Theta,TrueClock,OutputSource,EDOP,NDOP,VDOP,SMMode,SMLat,SMLon,SMHgt,VNavResult,Vmeas,VUnitFault,Ve,Vn,Vu,NavEngine,Pilots,IniLat,IniLon,IHgt,PrimaryID,Week,TIMESTAMPADD('m',-1,dt),SmEast,SmNorth,SmUp,Final,FlagH,FlagO,File,RecordSize,MarketID,SwitchNo,ServingCellID,StbUsed,StbCalced,StbCalcedSigma,StbConsUsed,StbConsValue,StbConsSigma,NID,SID,HandsetCapability,PosAidUsed,GLONASS,VEEast,VENorth,VEup,HEVE,ValidPosTruth,TruthLat,TruthLong,TruthHAE,TruthHEPE,ValidVelTruth,TruthVe,TruthVn,TruthVup,TruthHEVE,DeltaVe,DeltaVn,DeltaVup,TruthSpeed, MeasSpeed,DeltaSpeed, TruthAz, MeasAz, DeltaAz,app from lsl_csv;

-- export to csv file
vsql -o filename -c command