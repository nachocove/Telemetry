#HOWTO
## Run the Telemetry Viewer
$ sh run.sh

This will run the Viewer on localhost 8081

Use the browser to connect to:
http://localhost:8081/

You can use various combination of search keys. The only thing mandatory is the timestamp.

## Installation for Mac
1. Ensure that you have Django >= 1.7
2. Install Postgres.app from http://postgresapp.com/. 
3. Run 
   $ sudo pip install psycopg2
   
   If you run into issues with pg_config (e.g  Error: pg_config executable not found), check the following:
   $ ls -l /Applications/Postgres.app/Contents/Versions/9.4/bin/pg_config
   
   and then put that in the path
   $ export PATH=/Applications/Postgres.app/Contents/Versions/9.4/bin:$PATH
   
   and run the following again 
   $ sudo pip install psycopg2
   
## Configuration
1. Update the database password in config/<project>_t3_redshift.json where project in dev, alpha, beta, prod
