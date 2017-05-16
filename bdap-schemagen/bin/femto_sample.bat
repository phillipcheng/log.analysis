java -jar ..\target\schema.gen-VVERSIONN.jar -gen sfemto.schema -from ..\src\test\resources\femto\sFemto_OMRT_20150923.csv -by ..\src\test\resources\femto\config.json
java -jar ..\target\schema.gen-VVERSIONN.jar -gen efemto.schema -from "..\src\test\resources\femto\20151125_LTE eNB SmallCell_OMRT_VzW_Ver.2.1(VSR2.0).csv" -by ..\src\test\resources\femto\config.json
java -jar ..\target\schema.gen-VVERSIONN.jar -append sfemto.schema -with ..\src\test\resources\femto\common-schema.json
java -jar ..\target\schema.gen-VVERSIONN.jar -append efemto.schema -with ..\src\test\resources\femto\common-schema.json
java -jar ..\target\schema.gen-VVERSIONN.jar -gensql sfemto.ddl -from sfemto.schema -dbschema sfemto
java -jar ..\target\schema.gen-VVERSIONN.jar -genfiletablemapping sfemto_file_table_mapping.properties -from sfemto.schema
java -jar ..\target\schema.gen-VVERSIONN.jar -gensql efemto.ddl -from efemto.schema -dbschema efemto
java -jar ..\target\schema.gen-VVERSIONN.jar -genfiletablemapping efemto_file_table_mapping.properties -from efemto.schema
