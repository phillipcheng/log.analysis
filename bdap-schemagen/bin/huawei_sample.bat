java -jar ..\target\schema.gen-VVERSIONN.jar -multiple -gen huawei -from "..\src\test\resources\huawei\RAN Huawei CounterID to Name Translation Table.csv" -by ..\src\test\resources\huawei\measures-config.json
java -jar ..\target\schema.gen-VVERSIONN.jar -gen huawei-dimensions.schema -from ..\src\test\resources\huawei\TableDimensions.csv -by ..\src\test\resources\huawei\dimensions-config.json
java -jar ..\target\schema.gen-VVERSIONN.jar -multiple -to huawei -join huawei -with huawei-dimensions.schema
java -jar ..\target\schema.gen-VVERSIONN.jar -final -multiple -to huawei -append huawei -with ..\src\test\resources\huawei\common-schema.json
java -jar ..\target\schema.gen-VVERSIONN.jar -gensql huawei.ddl -from huawei -dbschema hwdb
java -jar ..\target\schema.gen-VVERSIONN.jar -genfiletablemapping huawei_file_table_mapping.properties -from huawei
