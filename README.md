log.analysis
============
# Features
1. hadoop based ETL platform
2. oozie as the workflow and coordination engine
3. commands can be run under mapreduce model or single process model
# Sample Flow
PDE ETL Flow
![PDE ETL Flow](https://raw.githubusercontent.com/phillipcheng/log.analysis/master/pde.analysis/pic/flow1.png)
SGSIWF ETL Flow
![enter image description here](https://raw.githubusercontent.com/phillipcheng/log.analysis/master/mtccore/pic/flow1.png)
# Reusable Commands
## 1. CSV file transformation
###  Columns Merge
        expression supported
        Example:
        concatenate column 68 with column 69, and joined by '-'
        merge.idx=68:69:(a.concat('-')).concat(b)
        
        column 0 is weekly number since epoch, and column 1 is the time part
        following expression caculate the epoch time by merging these two columns
        merge.idx=0:1:(Number(a)*7*24*3600 + Number(b)).toString()

### Column Split
        Example:
        split the column 3 into 2 fields by first '.'
        split.idx=3:.
        
### Single Column Update
        Example:
        remove the '.' in column 2 and remove the '\' in column 3
        update.idx=2:a.replace('\.',''),3:a.replace('\\','');

### Add Transformed file name to row data
### Skip header
### Row Ends with comma
### Row Validation support
##    2. Seed Generation Cmd
        mapreduce (seed) input generation for non csv files
        Example:
        working.folder=hdfs://192.85.247.104:19000/pde
        input.folder=rawinput
        input.files.threshold=-1
        output.seed.folder=seedinput
##    3. SFTP fetch files Cmd
        copy files to hdfs from sftp servers
##    4. Dynamic Schema Cmd
        generate or update the schema based on the input xml files
##    5. Dynamic Sql Generation/Execution Cmd
        used together with dynamic schema Cmd
##    6. Backup Cmd
        zip all the raw files and intermediate files and backup to data lake
##    7. KCV to CSV Transformation Cmd
        kcv looks like
        key:Value key:Value
##    8. Load Database Cmd
        load database with csv file
        vertica hdfs connector support
##    9. Shell Command
        invoke shell command with parameters
##    10. Upload Command 
        copy load files to hdfs
