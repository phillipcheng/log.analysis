log.analysis
============
1. hadoop based etl platform
2. using oozie as the workflow and coordination engine
3. commands can be run in mapreduce model or single process model
4. reusable commands supported
    1. csv file transformation
    	a. column merge
    	b. column split
    	c. column remove
    	d. column append
    	e. column prepend
    2. mapreduce (seed) input generation for non csv files
    3. sftp fetch files command (copy files to hdfs from sftp servers)
    4. dynamic database schema command
    5. sql execution command (used together with dynamic schema)
    6. backup command (zip all the raw files and intermediate files and backup to data lake)
    7. kcv (key:Value key:Value) to csv transformation command
    8. load database with csv file command
    9. Shell Command (invoke shell command with parameters)
    10. Upload Command (copy load files to hdfs)
