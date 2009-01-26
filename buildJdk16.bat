@echo off
rem $Id: //open/mondrian-release/3.0/buildJdk16.bat#2 $
rem Called recursively from 'ant release' to build the files which can only be
rem built under JDK 1.6.

rem Change the following line to point to your JDK 1.6 home.
set JAVA_HOME=C:\jdk1.6.0_01

rem Change the following line to point to your ant home.
set ANT_HOME=C:\open\thirdparty\ant

set PATH=%JAVA_HOME%\bin;%PATH%
%ANT_HOME%\bin\ant compile.java

rem End buildJdk16.bat

