java -jar ..\target\schema.gen-r0.6.0.jar -multiple -gen huawei -from "..\src\test\resources\huawei\RAN Huawei CounterID to Name Translation Table.csv" -by ..\src\test\resources\huawei\measures-config.json
java -jar ..\target\schema.gen-r0.6.0.jar -gen huawei-dimensions.schema -from ..\src\test\resources\huawei\TableDimensions.csv -by ..\src\test\resources\huawei\dimensions-config.json
java -jar ..\target\schema.gen-r0.6.0.jar -multiple -to huawei -join huawei -with huawei-dimensions.schema
java -jar ..\target\schema.gen-r0.6.0.jar -final -multiple -to huawei -append huawei -with ..\src\test\resources\huawei\common-schema.json
java -jar ..\target\schema.gen-r0.6.0.jar -gensql huawei.ddl -from huawei -dbschema hwdb
java -jar ..\target\schema.gen-r0.6.0.jar -genfiletablemapping huawei_file_table_mapping.properties -from huawei
