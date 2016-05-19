drop table Core_;
create table if not exists Core_(
endTime TIMESTAMP not null,duration int,avePerCoreCpuUsage numeric(15,5),peakPerCoreCpuUsage varchar(54),Core varchar(72),Machine numeric(15,5),UUID numeric(15,5));
