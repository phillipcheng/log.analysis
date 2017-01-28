drop schema if exists project1 cascade;
create schema if not exists project1;
create table if not exists project1.data1(startdt timestamp,sid varchar(20),a1id varchar(20),p1id varchar(20),p1v1 numeric(15,5),p1v2 numeric(15,5));
create table if not exists project1.data2(sid varchar(20),a2id varchar(20),p2id varchar(20),p2v1 numeric(15,5),p2v2 numeric(15,5));
create table if not exists project1.datamerge(sid varchar(20),startdt timestamp,a1id varchar(20),p1id varchar(20),p1v1 numeric(15,5),p1v2 numeric(15,5),wfid varchar(50),a2id varchar(20),p2id varchar(20),p2v1 numeric(15,5),p2v2 numeric(15,5));
