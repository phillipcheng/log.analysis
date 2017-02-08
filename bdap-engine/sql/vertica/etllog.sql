create table if not exists etllog.etlexception(startdt timestamp,wfname varchar(100),wfid varchar(200),actionname varchar(100),exception varchar(5000));
create table if not exists etllog.etlstat(startdt timestamp,enddt timestamp,wfname varchar(100),wfid varchar(200),actionname varchar(100),cnt1 numeric(20,5),cnt2 numeric(20,5),cnt3 numeric(20,5),cnt4 numeric(20,5));
