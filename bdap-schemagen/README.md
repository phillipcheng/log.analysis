Usage:
```
java -jar schema-generator.jar -gen <path> -from <path> -by <path>
java -jar schema-generator.jar -join <path> -with <path>
java -jar schema-generator.jar -append <path> -with <path>
java -jar schema-generator.jar -gensql <path> -from <path> [-dbschema <schema-name>]
java -jar schema-generator.jar -genfiletablemapping <path> -from <path>
 -append <arg>   Append the schema with the common schema
 -by <arg>       config json file
 -dbschema <arg> DB schema name
 -from <arg>     CSV file represents the schema / schema file
 -gen <arg>      Generate the schema
 -genfiletablemapping <arg>   Generate the file table mapping file from the schema
 -gensql <arg>   Generate the SQL file from the schema
 -help           display the usage help
 -join <arg>     Join the schema with another
 -with <arg>     Append/join with the schema json file
 ```

TODO:
 * Field type mapping: special numeric type needs precision and scale
 