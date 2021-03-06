# ToteRouter

ToteRouter was a fun project where I collaborated with Ian Griffith to create an automated process to send totes to their appropriate zones, so the warehouse team at MotoSport/Backcountry could pick the orders.  This was during the DC-in-a-DC WMS transition phase, so we had both Savant and the Backcountry warehouse infrastrucutre running concurrently.  The original idea was to have a warehouse staffer manually key in the tote routings versus finding a way to do an interim integration.  We felt that was pretty dumb, but our then-leadership told us that they couldn't spare any time from the engineering team on figuring this out.  So, Ian and I decided to do it on our own time and it saved the DC tons of pain on a ridiculous manual process. 

Basically, the pick ticket information came from Savant WMS where we had JDBC access to the Microsoft SQL Server.  Ian helped figure out the mappings of tote to zone and some of the SQL server mappings, while I did the Java stuff, pulling that data and sending it over to the existing Routes web service.  If I recall correctly, we had this set up as a scheduled task on the Savant server to run every few minutes to route totes between picks.  Anyway, MotoSport and Backcountry are totally integrated now, so this hacky program was retired at the end of the MotoSport Phase-2 Integration project after a successful 10-monthtour of duty in early 2014.

I saved this Java code because at one time, I hadn't really accessed RESTful web services, so I wanted to have a reference on how I did it.  If I had to do this same project again today, I'd use Python and Requests and have far less code, but I didn't know Python or how to work with REST at the time.  So, it's more for the history and memory.

------------
Files:
* ToteRouter.java is the program file - I looked through it and most of the comments were still good, but I did clarify a few comments and deleted some of the hard-coded lines in the test function.
* ToteRouter.properties is just the settings file, also took out the credentials and actual endpoint addresses.
