#!/bin/python

import subprocess
import os
import shutil

print """
--------------------------------------------------------------------------------
GENERATE AND PUBLISH STATISTICS

This script generates Seersuite's statistics and publishes them to Tomcat. 

It calls the scripts for generating statistics located at bin/genStats and 
bin/genHomePageStats and copies them to the WEB-INF directory of the Seersuite
webapp

--------------------------------------------------------------------------------
"""

seersuite_path=raw_input("Home of your CSX instance? ")
tomcat_home=raw_input("Home of your Tomcat server? ")

os.environ["CSX_HOME"] = seersuite_path

os.chdir(seersuite_path + "/bin")

subprocess.call(seersuite_path + "/bin/genStats")
subprocess.call(seersuite_path + "/bin/genHomePageStats")

print("Stats have been generated to " + seersuite_path + "/bin/stats")
tomcat_stats_folder = tomcat_home + "webapps/de.tudarmstadt.ukp.eduseer.citeseerx/WEB-INF/stats"

print("Removing existing stats folder and copy new stats")
shutil.rmtree(tomcat_stats_folder, ignore_errors = True)
shutil.copytree(seersuite_path + "/bin/stats", tomcat_stats_folder)


