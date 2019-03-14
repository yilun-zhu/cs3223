@echo off
set WORKING_DIR=%~dp0
setx CLASSPATH "%WORKING_DIR%;%WORKING_DIR%\classes;%WORKING_DIR%\lib\CUP;%WORKING_DIR%\lib\JLEX;"
setx COMPONENT "%WORKING_DIR%"
echo "Query environment setup successfully"