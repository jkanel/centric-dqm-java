@echo off

REM #### Command Line Options ####

REM #### -t "{Tag (String, Comma Delimited)}"
REM #### -s "{Scenario Identifier (String, Comma Delimited)}"
REM #### -p {Age (days) after which test cases are purged}

REM ###############################

java -jar "com.centric.dqm.jar" -p 20 -t "CAP,ABC" -s "TEST1,TEST2"
