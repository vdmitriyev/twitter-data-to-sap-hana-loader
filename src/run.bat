@echo off
REM	@author Viktor Dmitriyev
echo "Loading Twitter data into SAP HANA DB ..."
c:\Soft\sap\rev80\hdbclient\Python\python.exe twitter_data_loader.py > generated_output.txt
pause