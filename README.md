Gunoba Scraper v2.0

a scraper for https://guncadindex.com/ to get all posts and then finds the odysee.com url go'es there and get's the files on lbry://

 Prerequisites
    
    Python 3.8+
    lbrynet CLI
    aiohttp
    beautifulsoup4
    requests

you will need to run the below, and then run the Gunoba Scraper if you wish to get the cad's.

On Windows you have two easy ways to run the lbrynet daemon:
1. Use the Odysee/LBRY Desktop app (easiest)
Download the latest desktop client from https://lbry.com/get and install it.
Launch the app. It automatically starts the bundled lbrynet daemon in the background on http://localhost:5279.
Leave the app running while you execute python master.py ... -s2. Stage 2 will connect to the daemon that the app started.


2. Run the CLI lbrynet manually
Grab the Windows CLI zip from https://github.com/lbryio/lbry-sdk/releases (look for an asset named like lbrynet-windows-x64.zip).
Extract it somewhere, e.g. C:\lbrynet.
Open PowerShell in that folder and start the daemon:
cd C:\lbrynet.\lbrynet.exe start
Keep that PowerShell window open; it will show logs. Once it says it’s listening on 5279, run your Stage 2 command in another window.
When you’re done scraping, stop the daemon with .\lbrynet.exe stop (or just close the Desktop app).

below are the run examples the first two are the key cmd to get files from 

  python master.py -x                                   # Stage1 scans site gets internal & external url save to db

  python master.py -s                                   # Stage2 gets lbry:// and starts download of files
 
  python master.py -s 1 -e 3 -o mylog.json              # start from page 1 end at 3 with a output to a json
  
  python master.py -f -v                                # Fast mode with verbose logging

  python master.py --db existing.db                     # save to existing DB
