Because the two databases looked at in this project have very different formats, 
there are two different CreatorDatabase files to create their corresponding postgres databases.

The creator files for toulouse and shalla each generates two postgres databases. One with the name of the groups and their coresponding index values, and the other with an index value, url, groupe index, sha1 (if there is one), sha1 certificate expiration date, timestamp of connection, and if an error is generated during the connection the error message. 

The creator file for suricata generates one database and fills it. It removes duplicate and expired sha1 certificates, and only takes the information from the databases that is needed to create a suricata rule. 

The Filler file generates the connections to the URLs and retrieves the SHA1 certificate if it exists and fills in the missing information for the postgres Database. 

The Rule Writer file creates Suricata rules for a given group name.
