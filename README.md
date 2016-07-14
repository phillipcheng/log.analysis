log.analysis
============
# Features
## Hadoop based ETL platform
## Oozie as the workflow and coordination engine
User make up the workflow by using the out-of-box commands, or project specific commands.

Then use the oozie workflow engine and coordination engine to schedule the jobs.
## Commands
### static config
### dynamic config in
### dynamic config out
### wfid
Since ETL will be processed in batch mode, the wfid stands for workflow instance id. 

It will be generated to identify a ETL batch process instance, usually that stands for a list of input files (dataset).
### Single Process Mode
In single JVM process mode, each cmd is setup using
wfid, static config, dynamic config in, dynamic config out, defaultFs (hdfs).

Then the cmd will be passed offset, row, context as following signature.
```
List<String> process(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
```
User can ignore the offset and context.

Only the row will have the line of csv data if this command is chained.

### Mapreduce Mode
In mapreduce mode, each cmd is setup using 
wfid, static config, dynamic config in, dynamic config out, defaultFs (hdfs).

Then the cmd will be passed offset, row, context as following signature.
```
List<String> process(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
```
the offset is the byte offset from the beginning of the input file

the row is each line of the input file

the context is the mapper context, where user can get configuration and other info

each cmd returned the list of output lines which will be further processed or output.
### Chained Cmd

# Reusable Commands
## 1. CSV file transformation
**Class Name: etl.cmd.transform.CsvTransformCmd**
User can specify following column operations (Merge, Split, Update) on the fields for each line of the csv file.
###  Columns Merge
User can merge a range of columns by specifying the merge expression.

Example 1:
col.op=u|2:(fields[2].concat('-')).concat(fields[3])
	      Concatenate column 2 with column 3 by joint -

COLUMN2	        COLUMN3
10/24/2014	54:09.1
10/24/2014	54:08.8
	
MERGED_COLUMN
10/24/2014-54:09.1
10/24/2014-54:08.8

 
### Column Split
User can split specific column by specify the separator.
col.op=s|2:.
Split the column 2 into multiple fields using separator '.'

COLUMN2
39.711708

	

SPLITTED_COLUMN2	SPLITTED_COLUMN3
39		 	711708

### Single Column Update
User can transform the columns by specify the transformation expression.
u|2:fields[2].replace('\.'\,'') – Replace . in column 2 with empty string
u|3:fields[3].replace('coarse'\,'') – Replace ‘coarse’ in column 3 with empty string

COLUMN2		COLUMN3
39.711708	coarse Primary
	
UPDATED_COLUMN2		UPDATED_COLUMN3
3971170			Primary


### Add Transformed file name to row data
User can add the file name (after some transformation) to each row of data.

    Example 1:
```
add.filename=true
#PJ24002A_BBG2.csv =>BBG2
exp.filename=filename.substring(filename.length-8\, filename.length-4)
```
    Append the last 8 to 4 substring of the file name to the output csv.
### Skip header
Some input files comes with the header line, we need to skip that line for output.

    Example 1:
```
skip.header=true
```
### Row Ends with comma
Some input csv files, each line comes with an ending comma, we need to tell Command about this.

    Example 1:
```
input.endwithcomma=true
```
### Row Validation
Some input csv files has corrupted lines not intended to be sent to output.
We enable user to specify the row validation expression to validate each line

    Example 1:
```
row.validation=fields.length>10
```
    For this java script expression, the system varaible "fields" (an array of fields for each line) is passed.
## 2. Seed Input Generation
**Class Name: etl.cmd.GenSeedInputCmd**

Mapreduce (seed) input generation for non csv files.

This cmd will read all files under the input.folder, then generates a seed input file under output.seed.folder

The follow up cmd usually run under mapreduce mode and with the inputformat class set to NLineInputFormat, and linespermap set to 1.
### use.wfid
if set to true, the input.folder becomes input.folder + '/' + wfid
### Example 1:
static configuration:
```
input.folder=/pde/rawinput
output.seed.folder=/pde/seedinput
```
workflow configuration:
```xml
<action name="GenBinSeedInput">
	<java>
		<job-tracker>${jobTracker}</job-tracker>
		<name-node>${nameNode}</name-node>
		<main-class>etl.engine.ETLCmdMain</main-class>
		<arg>etl.cmd.GenSeedInputCmd</arg>
		<arg>${wf:id()}</arg>
		<arg>/pde/etlcfg/pde.bin.genseedinput.properties</arg>
		<arg>unused</arg>
		<arg>unused</arg>
		<capture-output/>
	</java>
	<ok to="BinCsvConverter"/>
	<error to="fail"/>
</action>
<action name="BinCsvConverter">
	<map-reduce>
		<job-tracker>${jobTracker}</job-tracker>
		<name-node>${nameNode}</name-node>
		<configuration>
			<property>
				<name>mapreduce.job.map.class</name>
				<value>etl.engine.InvokeMapper</value>
			</property>
			<property>
				<name>mapreduce.job.inputformat.class</name>
				<value>org.apache.hadoop.mapreduce.lib.input.NLineInputFormat</value>
			</property>
			<property>
				<name>mapreduce.input.lineinputformat.linespermap</name>
				<value>1</value>
			</property>
			<property>
				<name>mapreduce.job.outputformat.class</name>
				<value>org.apache.hadoop.mapreduce.lib.output.NullOutputFormat</value>
			</property>
			<property>
				<name>mapreduce.input.fileinputformat.inputdir</name>
				<value>${wf:actionData('GenBinSeedInput')['seed.input.filename']}</value>
			</property>
			<property>
				<name>cmdClassName</name>
				<value>etl.cmd.ShellCmd</value>
			</property>
			<property>
				<name>wfid</name>
				<value>${wf:id()}</value>
			</property>
			<property>
				<name>staticConfigFile</name>
				<value>/pde/etlcfg/tracefilter.shell.properties</value>
			</property>
		</configuration>
	</map-reduce>
</action>
```
In this example, GenBinSeedInput is an action using GenSeedInputCmd command, it will generate a file containing all the file names under input.folder specified within /pde/etlcfg/pde.bin.genseedinput.properties.

The next action called "BinCsvConverter" will transform each bin file into an csv file, this will be invoked under mapreduce mode.


## 3. SFTP fetch files Cmd
**Class Name: etl.cmd.SftpCmd**

copy files to hdfs from sftp servers

### Example 1:
```
incoming.folder=/test/sftp/incoming/
sftp.user=dbadmin
sftp.port=22
sftp.pass=password
sftp.folder=/data/mtccore/sftptest/
sftp.clean=true
sftp.getRetryTimes=3
sftp.connectRetryTimes=3
```
## 4. Dynamic Schema Cmd
**Class Name: etl.cmd.dynschema.DynSchemaCmd**

Generate or update the schema based on the input xml files, then generate the csv files accordingly.

### Example 1:

Static Configuration
```
#xml input folder
xml-folder=/test/dynschemacmd/input/
#csv output folder
csv-folder=/test/dynschemacmd/output/
#schema output folder
schema-folder=/test/dynschemacmd/schema/
#schema history folder, any updated or new schema generated will be put here with timestamp
schema-history-folder=/test/dynschemacmd/schemahistory/
#db schema name
prefix=sgsiwf

FileSystemAttrs.xpath=/measCollecFile/fileHeader/fileSender/@localDn
FileSystemAttrs.name=SubNetwork,ManagedElement
FileSystemAttrs.type=varchar(70),varchar(70)
TableSystemAttrs.xpath = ./granPeriod/@endTime,./granPeriod/@duration
TableSystemAttrs.name = endTime, duration
TableSystemAttrs.type = TIMESTAMP WITH TIMEZONE not null, varchar(10)

xpath.Tables = /measCollecFile/measData/measInfo
xpath.TableRow0 = measValue[1]
TableObjDesc.xpath = ./@measObjLdn
TableObjDesc.skipKeys=Machine,UUID,PoolId,PoolMember
TableObjDesc.useValues=PoolType
xpath.TableAttrNames = ./measType
xpath.TableRows = ./measValue 
xpath.TableRowValues = ./r
```

Input Xml:
```xml
<measCollecFile>
	<fileHeader fileFormatVersion="32.401 V5.0" vendorName="Alcatel-Lucent" dnPrefix="">
		<fileSender localDn="SubNetwork=vQDSD0101SGS-L-AL-20,ManagedElement=lcp-1" elementType="GmscServer,Vlr"/>
		<measCollec beginTime="2016-03-09T07:45:00+00:00"/>
	</fileHeader>
	<measData>
		<managedElement localDn="SubNetwork=vQDSD0101SGS-L-AL-20,ManagedElement=lcp-1" userLabel="" swVersion="R33.11.00"/>
		<measInfo>
			<granPeriod duration="PT300S" endTime="2016-03-09T07:50:00+00:00"/>
			<measType p="1">VS.avePerCoreCpuUsage</measType>
			<measType p="2">VS.peakPerCoreCpuUsage</measType>
			<measValue measObjLdn="Machine=vQDSD0101SGS-L-AL-20-CDR-01, UUID=a040d711-7ec2-4a5c-be90-6c7f82a3fe21, MyCore=0">
				<r p="1">2.59</r>
				<r p="2">9.13</r>
			</measValue>
			<measValue measObjLdn="Machine=vQDSD0101SGS-L-AL-20-CDR-01, UUID=a040d711-7ec2-4a5c-be90-6c7f82a3fe21, MyCore=1">
				<r p="1">2.26</r>
				<r p="2">8.83</r>
			</measValue>
		</measInfo>
	</measData>
</measCollecFile>
```
### Table Name
The table name is defined by "TableObjDesc".
the evaluated value of the TableObjDesc.xpath is a csv string, composed of different attributes, in this example, it is evaluated to "Machine=vQDSD0101SGS-L-AL-20-CDR-01, UUID=a040d711-7ec2-4a5c-be90-6c7f82a3fe21, MyCore=0"

TableObjDesc.skipKeys defined which keys are omitted in the table name composition, in this example "Machine", "UUID" are skipped, so the table name is "MyCore_".

### Table Fields
The fields of each table is composed of following 4 groups of fields:
1. System Attributes from File Scope: defined by "FileSystemAttrs"

In this example the xpath is defined as "/measCollecFile/fileHeader/fileSender/@localDn", it is evaluated to "SubNetwork,ManagedElement"
2. System Attributes from Table Scope: defined by "TableSystemAttrs"

In this example the xpath is defined as "./granPeriod/@endTime,./granPeriod/@duration", they are "duration","endTime"
3. Table Object Description Fields: defined by "TableObjDesc"

In this example the xpath is defined as "./@measObjLdn" within xpath.Tables (which is defined as "/measCollecFile/measData/measInfo" in this example.), they are evaluated to "Machine=vQDSD0101SGS-L-AL-20-CDR-01, UUID=a040d711-7ec2-4a5c-be90-6c7f82a3fe21, MyCore=0", and the fields will be added are "Machine,UUID, MyCore".
4. Table-wise attributes: defined by "xpath.TableAttrNames"

In this example, defined by "./measType" within xpath.Tables (which is defined as "/measCollecFile/measData/measInfo" in this example.), they are evaluated to "VS.avePerCoreCpuUsage,VS.peakPerCoreCpuUsage".

So following table will be created if not already exist in the schema:
```sql
create table sgsiwf.MyCore_(
endTime TIMESTAMP WITH TIMEZONE not null,
duration varchar(10),
SubNetwork varchar(70),
ManagedElement varchar(70),
Machine varchar(54),
MyCore numeric(15,5),
UUID varchar(72),
VS_avePerCoreCpuUsage numeric(15,5),
VS_peakPerCoreCpuUsage numeric(15,5));
```

## 5. Dynamic Sql Generation/Execution Cmd
**Class Name: etl.cmd.dynschema.DynSqlExecutorCmd**

This cmd is used together with dynamic schema Cmd.
Based on the schema generated, csv files generated, dynamic config generated by the previous cmd, 
this cmd will 
1. run the create/update table schema sql if specified in the dynmic config.
2. auto generate and run the copy cmd to copy the csv data to database

### Example 1
Static Configuration:
```
hdfs.webhdfs.root=http://192.85.247.104:50070/webhdfs/v1
csv-folder=/mtccore/csvdata/
schema-folder=/mtccore/schema/
prefix=sgsiwf
systemAttrs.name=endTime,duration,SubNetwork,ManagedElement

db.driver=com.vertica.jdbc.Driver
db.url=jdbc:vertica://192.85.247.104:5433/cmslab
db.user=dbadmin
db.password=password
db.loginTimeout=35
```
Workflow Configuration for dynSchema and dynSqlExecutor used sequentially.
```xml
<action name="GenCsv">
		<java>
			<job-tracker>${jobTracker}</job-tracker>
			<name-node>${nameNode}</name-node>
			<main-class>etl.engine.ETLCmdMain</main-class>
			<arg>etl.cmd.dynschema.DynSchemaCmd</arg>
			<arg>${wf:id()}</arg>
			<arg>/mtccore/etlcfg/sgsiwf.dynschema.properties</arg>
			<arg>unused</arg>
			<arg>${concat("/mtccore/schemahistory/sgsiwf.dyncfg_", wf:id())}</arg>
			<capture-output/>
		</java>
		<ok to="SqlExec"/>
	</action>
	<action name="SqlExec">
		<java>
			<job-tracker>${jobTracker}</job-tracker>
			<name-node>${nameNode}</name-node>
			<main-class>etl.engine.ETLCmdMain</main-class>
			<arg>etl.cmd.dynschema.DynSqlExecutorCmd</arg>
			<arg>${wf:id()}</arg>
			<arg>/mtccore/etlcfg/sgsiwf.sqlexecutor.properties</arg>
			<arg>${concat("/mtccore/schemahistory/sgsiwf.dyncfg_", wf:id())}</arg>
			<arg>unused</arg>
			<capture-output/>
		</java>
	</action>
```
## 6. Backup Cmd
**Class Name: etl.cmd.BackupCmd**

zip all the raw files and intermediate files and backup to data lake

Exmaple 1:
```
folder.filter=/test/BackupCmd/data/dynFolder1/,/test/BackupCmd/data/allFolder1/,/test/BackupCmd/data/wfidFolder1/
file.filter=dynFiles,ALL,WFID
data-history-folder=/test/datahistory/
```
user can specify a list of folder filters and file filters, 1 to 1 mapped.
there are 3 types of file.filter:
1. ALL: all files under the corresponding folder.filter
2. WFID: files starting with WFID under the corresponding folder.filter
3. other-name: values in the dynamic config keyed with "other-name", the value is a list of string, specifying the file names under the corresponding folder.filter

This cmd will zip all these files specified in a zip file named as wfid.zip under the data-history folder. Then it will remove them.

## 7. KCV to CSV Transformation Cmd
**Class Name: etl.cmd.transform.KcvToCsvCmd**

The key colon value format to csv format transformation.
static configuration:
```
record.start=^TIME:.* UNCERTAINTY:.*
#value,key regexp for the kcv format
record.vkexp=[\\s]+([A-Za-z0-9\\,\\. ]+)[\\s]+([A-Z][A-Z ]+)
record.fieldnum=8
kcv.folder=/etltest/kcvtransform/
use.wfid=false
```
sample input:
```
TIME: 1815,449649.119 UNCERTAINTY: 0.000 SOURCE: external Primary ID: X310007204992127F TYPE: standard fix SESSION: 202967223 APPLICATION: 327897
POSITION ENGINE: integrated RESULT: success   GPS: 0 AFLT: 7 EFLT: 0 ALTITUDE: 1 ORTHO: 0 TIME AID: 0 RTD: 0 STB: 0 POS: 0
```
sample output:
```
1815,449649.119,0.000,external Primary,X310007204992127F,standard fix,202967223,327897,,
1815,449648.824,0.000,external Primary,X310005414400271F,standard fix,202967224,262221,, 
```
1. record.start specify the regular expression to identify the begining of a record.
2. record.vkexp specify the value key regular expression to match the value and key
3. record.fieldnum specify the number of fields to extract for each record

## 8. Load Database Cmd
**Class Name: etl.cmd.LoadDataCmd**

load the csv files from dfs to database.

This cmd is used to load csv data to a predefined database.

Vertica hdfs connector support.

Example 1:
Static Configuration
```
hdfs.webhdfs.root=http://192.85.247.104:50070/webhdfs/v1
csv.folder=/pde/fixcsv1/
use.wfid=true
load.sql=copy lsl_ses (epochDt filler float\, dt as TO_TIMESTAMP(epochDt)\, uncertainty\, source\, primaryid\, fixtype\, sessionid\, appid)
db.driver=com.vertica.jdbc.Driver
db.url=jdbc:vertica://192.85.247.104:5433/cmslab
db.user=dbadmin
db.password=password
db.loginTimeout=35
```

## 9. Shell Cmd
**Class Name: etl.cmd.ShellCmd**

Example 1:
```
local.work.folder:/data/pde/tmp
hdfs.work.folder:hdfs://192.85.247.104:19000/pde
rawinput.folder:rawinput
csv.folder:csv
fix.folder:fix
#responsibility of the mr.command: process the input file from [rawinput.folder]
command=/data/pde/bin/invoke_tracefilter.sh $local.work.folder $hdfs.work.folder $rawinput.folder $key $csv.folder $fix.folder
```
invoke the shell commnad via MapReduce.

This cmd will replace the attributes and execute the cmd.

## 10. Event Based Message Parsing Cmd
**Class Name: etl.cmd.transform.EvtBasedMsgParseCmd**

Example 1:
```
#record type specification
#where is the event type
event.idx=5
#event type values
event.types=default,005730,005706

#main message specification
#where is the main message
message.idx=6
#how to extract the fields in the main message
message.fields=IMSI,E164,GTAddr,ReturnCause
#regexp to identify each message field
default.regexp=.+ E.164 ([0-9]+) .+
default.attr=E164
005730.regexp=.+ GT Addr ([0-9]+)
005730.attr=GTAddr
005706.regexp=.+ TimeOut : ([0-9]+) .+ GT Addr ([0-9]+)
005706.attr=IMSI,GTAddr
```


# Sample Projects
## PDE Analysis
### PDE Specific Cmds
#### Session To CSV Cmd

### ETL Flow
![PDE ETL Flow](https://raw.githubusercontent.com/phillipcheng/log.analysis/master/pde.analysis/pic/flow1.png)
## SGSIWF Analysis
### ETL Flow
![enter image description here](https://raw.githubusercontent.com/phillipcheng/log.analysis/master/mtccore/pic/flow1.png)
