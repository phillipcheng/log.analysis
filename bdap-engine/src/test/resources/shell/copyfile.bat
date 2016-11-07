@ECHO OFF
set srcfolder= %1
set destfolder= %2

if exist %srcfolder% rd /s /q %srcfolder%
if exist %destfolder% rd /s /q %destfolder%
MKDIR %srcfolder%
MKDIR %destfolder%
@echo The test content goes here >%srcfolder%/testShell.txt
echo capture:key:value
xcopy /s %srcfolder% %destfolder%
