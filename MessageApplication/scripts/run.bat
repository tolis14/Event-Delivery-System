@echo off
start Broker_6000.bat
ping -n 2 127.0.0.1 > nul
start Broker_6001.bat
ping -n 2 127.0.0.1 > nul
start Broker_6002.bat
