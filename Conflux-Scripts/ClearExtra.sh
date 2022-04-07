
username=The-System-UserName
centerPath=/home/$username/nfs/Band-Aggr/Conflux-Center
clientPath=/home/$username/nfs/Band-Aggr/Conflux-User

ssh $username@User-IP bash $clientPath/PyTfcode/clear.sh

ssh $username@Center-IP bash $centerPath/bin/clear.sh
