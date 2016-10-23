drop schema etllog cascade;
create schema if not exists etllog;
create table if not exists etllog.etlexception(startdt timestamp,wfname varchar(100),wfid varchar(200),actionname varchar(100),exception varchar(5000)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";
create table if not exists etllog.etlstat(startdt timestamp,enddt timestamp,wfname varchar(100),wfid varchar(200),actionname varchar(100),cnt1 decimal(20,5),cnt2 decimal(20,5),cnt3 decimal(20,5),cnt4 decimal(20,5)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";
