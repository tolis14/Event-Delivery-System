Event Delivery System Application in java.
Creators: Nikos Nikolopoulos, Stathis Parasidhs, Tolhs Kapeths.

Execution instructions: 
Inside scripts folder double click the run.bat script. This script will create 6 proccesses- 3 Brokers and 3 Client.
The Brokers are listening to ports 6000, 6001, 6002.
The Clients are named tolis, nikos, stathis.

More info:

- multimedia directory is a public repository for all clients from where they can upload files (normally this is 
the default folder where multimedia content is stored in mobile phones)

- ClientDirectories contains a directory for every client in which all the multimedia files (messages or stories) that 
he receives during runtime gets stored. It is used in order to check if files received correctly or not.

All paths inside application assume that it will be executed by cmd in a Windows enviroment. If you want to run the application 
from an IDE or in Operating System paths might need some modification.

** Paths that need modification if you want to run the project via an IDE**
** Just remove ..\\ in front of paths in the following lines or set the working directory "ProjectDir"\\src folder **
** for Example in Eclipse IDE Run Configuration->Arguments->Working directory->select other option and type **
** 								    {workspace_loc:MessageApplication\\src} **

Paths require modification if you run the project in a different OS.
	file    	|   line 
	Broker  	|   207  
	MessengerClient	|   64
	Publisher	|   14
	MultimediaFile  |   23
	Util		|   81
	Listener	|   83