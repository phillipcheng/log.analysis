Usage:
```
java -jar schema-generator.jar -gen &lt;path&gt; -from &lt;path&gt; -by &lt;path&gt;
java -jar schema-generator.jar -join &lt;path&gt; -with &lt;path&gt;
java -jar schema-generator.jar -append &lt;path&gt; -with &lt;path&gt;
java -jar schema-generator.jar -gensql &lt;path&gt; -from &lt;path&gt; [-dbschema <schema-name>]
java -jar schema-generator.jar -genfiletablemapping &lt;path&gt; -from &lt;path&gt;
 -append &lt;arg&gt;   Append the schema with the common schema
 -by &lt;arg&gt;       config json file
 -dbschema &lt;arg&gt; DB schema name
 -from &lt;arg&gt;     CSV file represents the schema / schema file
 -gen &lt;arg&gt;      Generate the schema
 -genfiletablemapping &lt;arg&gt;   Generate the file table mapping file from the schema
 -gensql &lt;arg&gt;   Generate the SQL file from the schema
 -help           display the usage help
 -join &lt;arg&gt;     Join the schema with another
 -with &lt;arg&gt;     Append/join with the schema json file
 ```
