#!/bin/bash
add-apt-repository ppa:deadsnakes/ppa -y
apt-get update
apt-get install python3.6 -y
echo 'alias python=python3.6' >> /home/ubuntu/.bashrc 
source /home/ubuntu/.bashrc

apt-get install python3-pip -y
python3.6 -m pip install --upgrade pip
python3.6 -m pip install mysql-connector
python3.6 -m pip install boto3

ssh-keyscan github.com >> /home/ubuntu/.ssh/known_hosts

cd /home/ubuntu
git clone https://github.com/cmu-rl/user_server.git
git clone https://github.com/cmu-rl/web_server.git

chown -R ubuntu:ubuntu ./web_server/
chown -R ubuntu:ubuntu ./user_server/

echo "#!/bin/sh -e" >> ./tmp
echo "/usr/bin/python3.6 /home/ubuntu/user_server/user_server.py" >> ./tmp
echo "exit 0" >> ./tmp
chmod +x ./tmp
rm /etc/rc.local
mv ./tmp /etc/rc.local

reboot now