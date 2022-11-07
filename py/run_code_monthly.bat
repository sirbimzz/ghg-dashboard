@ECHO OFF
TITLE Execute python script on anaconda environment
ECHO Please Wait...
:: Section 1: Activate the environment.
ECHO ============================
ECHO Conda Activate
ECHO ============================
@CALL "E:\Apps\digital\Anaconda\Scripts\activate.bat" base
:: Section 2: Execute python script.
ECHO ============================
ECHO Python mrv_monthly.py
ECHO ============================
python "E:\Apps\digital\dashboards\mrv\py\mrv_monthly.py"



ECHO ============================
ECHO End
ECHO ============================