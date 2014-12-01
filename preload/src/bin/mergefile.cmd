set JAVA_HOME=C:\Java\jdk1.7.0_67
set LA_HOME=C:\Users\cheyi\git\log.analysis\preload\target

setlocal enableDelayedExpansion
set CL=
for %%X in (%LA_HOME%\lib\*.jar) do set CL=!CL!;%%X

echo %LA_EXT%

%JAVA_HOME%\bin\java -Xmx2048m -cp "%LA_EXT%;%LA_HOME%\preload-0.0.1.jar;%CL%" log.analysis.preload.FileMerger %*