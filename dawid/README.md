
### This README explains the global structure of the code 
### Before trying to use anything, you must fill in a "config.py" file based on the "config_example.py"

Use : cp config_example.py config.py    (Do not push "config.py" on github, it should be in the git ignore file)

### The project has been divided into 3 main directories:
###
### First directory: "databases" which contains the shallalist called "BL"
### and the database of Toulouse called "liste.csv". It also contains another
### README file to explain in more detail the issues that we came across with 
### the shallalist database.
###
### Second directory: "DynFiltering" which contains the python scripts for creating
### the appropriate postgresql templates for our databases,
### the python script  which recovers the SHA1 footprints from the internet to 
### write it in the postgresql database,
### the Suricata ruleset writer which converts a database into a ruleset.txt file
### which can directly be imported into a Suricata configuration,
### a README file to explain in more detail.
###
### Third directory: "site" which contains the Flask app and jinja2 templates
### to generate the website,
### a README file to explain it in further detail.

### Fourth directory: "suricata rules" which contains the suricata rule sets created. 

### If you have any questions you can contact us:
### remon.majoor@student-cs.fr
### diva-oriane.marty@student-cs.fr
