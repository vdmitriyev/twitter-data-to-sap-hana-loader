About
=====
Simple python script that loads twitter data SAP HANA DB table.

Dependencies Setup
==================
* oauth2
```
pip install oauth2
```

Twitter Table Structure
====================================
* To create table in SAP HANA script use following script 'twitter_stream.hdbtable'

Credentials for the SAP HANA DB
======================================
* Create file 'sap_hana_credentials.py'
* Copy-&gt;Paste code below and insert your credentials
```
# Server 
SERVER = '&lt;server&gt;'
PORT = &lt;port&gt;

# User Credentials
USER = '&lt;user&gt;'
PASSWORD = '&lt;password&gt;'
```

Applications '&lt;port&gt; should be 3'&lt;instance number&gt;15, for example, 30015, if the instance is 00.

Run on Windows
==============
To main python script on windows machine you can use 'run.bat'.
Note: (a) all configurations must be performed before script can be executed properly;
```
run.bat
```	

Known Problems and Drawbacks
============================
*

Credits
=======
* 