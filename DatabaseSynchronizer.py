import wget
import os
url = "https://static.data.gouv.fr/resources/liste-noire-durls/20141127-153953/liste.csv"
os.system("rm ../liste.csv")
wget.download(url, '/home/rmajoor/liste.csv')
