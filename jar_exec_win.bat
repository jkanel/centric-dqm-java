@echo off

REM #####################################
REM #### COMMAND LINE OPTIONS ###########
REM #####################################
REM #### 
REM #### -p {Optional, Age (days) after which test cases are purged}
REM #### -t "{Optional, Tag (String, Comma Delimited)}"
REM #### -s "{Optional, Scenario Identifier (String, Comma Delimited)}"
REM #### -Djava.library.path="{path}" Required, Path containing DLLs 
REM #### 
REM #####################################

SET addonspath=".\addons"

java -Djava.library.path="%addonspath%" -jar "com.centric.dqm.jar" -p 20 -t "CAP,ABC" -s "TEST1,TEST2"
