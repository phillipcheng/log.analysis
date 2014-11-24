drop table hss_ef;
create table if not exists hss_ef (
	dt TIMESTAMP,
	system varchar(16),
	process varchar(32),
	ssid varchar(32),
	evtId varchar(6),
	evtTxt varchar(500),
	imsi varchar(20),
	ohost varchar(80),
	srvDstName varchar(80),
	srvDstRlm varchar(80),
	clrReason varchar(2),
	VPLMN varchar(10),
	RATType varchar(10)
);
alter table hss_ef add column VPLMNMCC varchar(3);
update hss_ef set VPLMNMCC = left(VPLMN,3);
alter table hss_ef add column VPLMNMNC varchar(3);
update hss_ef set VPLMNMNC = right(VPLMN,char_length(VPLMN)-3);
update hss_ef set VPLMNMNC = ltrim(VPLMNMNC,'0');
update hss_ef set VPLMN = concat(VPLMNMCC, VPLMNMNC);

drop table hlr_ef;
create table if not exists hlr_ef (
	dt TIMESTAMP,
	system varchar(16),
	process varchar(32),
	ssid varchar(32),
	evtId varchar(6),
	evtTxt varchar(500),
	imsi varchar(20),
	e164 varchar(20),
	e164_cc varchar(5),
	e164_nanpa varchar(10),
	GTAddr varchar(10),
	gt_cc varchar(5),
	ReturnCause varchar(5)
);

create table if not exists hss_el (
	eid varchar(6),
	name varchar(100)
);

create table if not exists hlr_el (
	eid varchar(6),
	name varchar(100)
);

create table if not exists country_code (
	name varchar(100),
	fname varchar(200),
	tcode varchar(6)
);

create table if not exists nanpa (
	state varchar(2),
	npanxx varchar(7),
	ocn varchar(6),
	company varchar(100),
	ratecenter varchar(100)
);

create table if not exists mccmnc (
	mcc varchar(3),
	mnc varchar(3),
	country varchar (50),
	network varchar (100)
);
alter table mccmnc add column mccmnc varchar(6);
update table mccmnc set mccmnc = concat(mcc,mnc);
 
truncate table hss_ef;
truncate table hss_el;
truncate table hlr_ef;
truncate table hlr_el;

copy hss_ef (dt format 'YYYY-MM-DD-HH:MI:SS-US', system, process, ssid, evtId, evtTxt, imsi, ohost, srvDstName, srvDstRlm, clrReason, VPLMN, RATType) from local 'C:\\projects\\TBDA\\att-storedvalue\\ems.analysis\\dbinput\\input.hss1_obfus.txt' delimiter ',' direct;

copy hlr_ef (dt format 'YYYY-MM-DD-HH:MI:SS-US', system, process, ssid, evtId, evtTxt, imsi, e164, e164_cc, e164_nanpa, GTAddr, gt_cc, ReturnCause) from local 'C:\\project\\TBDA\\att-storedvalue\\ems.analysis\\dbinput\\input.hlr1.txt' delimiter ',' direct;

copy hss_el (eid, name) from local 'C:\\project\\TBDA\\att-storedvalue\\ems.analysis\\dbinput\\hssEvtType.txt' delimiter ',' direct;

copy hlr_el (eid, name) from local 'C:\\project\\TBDA\\att-storedvalue\\ems.analysis\\dbinput\\hlrEvtType.txt' delimiter ',' direct;

copy nanpa (state, npanxx, ocn, company, ratecenter) from local 'C:\\project\\TBDA\\att-storedvalue\\ems.analysis\\rawdata\\allutlzd.txt' delimiter '	' direct;

copy country_code (name, fname, tcode) from local 'C:\\project\\TBDA\\att-storedvalue\\ems.analysis\\rawdata\\iso_3166_2_countries.csv' delimiter ',' direct;

copy mccmnc (mcc, country, mnc, network) from local 'C:\\project\\TBDA\\att-storedvalue\\ems.analysis\\rawdata\\mccmnc.txt' delimiter '	' direct;

update hlr_el set name = 'InsSubData Timed Out' where eid='005608';