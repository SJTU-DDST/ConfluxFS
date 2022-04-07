
username=The-System-UserName
centerPath=/home/$username/nfs/Band-Aggr/QDcache-Center
clientPath=/home/$username/nfs/Band-Aggr/QDcache-User

ssh $username@192.168.98.74 bash $clientPath/PyTfcode/clear.sh

ssh $username@192.168.98.73 bash $centerPath/bin/clear.sh
