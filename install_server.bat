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


REM echo "description "A script controlled by upstart"" >> ./tmp
REM echo "author \"Anton\"" >> ./tmp
REM echo "" >> ./tmp
REM echo "log console" >> ./tmp
REM echo "" >> ./tmp
REM echo "start on runlevel [2345]" >> ./tmp
REM echo "stop on runlevel [016]" >> ./tmp
REM echo "" >> ./tmp
REM echo "respawn" >> ./tmp
REM echo "" >> ./tmp
REM echo "script" >> ./tmp
REM echo "    exec /usr/bin/python3.6 /home/ubuntu/user_server/user_server.py" >> ./tmp
REM echo "end script" >> ./tmp

echo "[Unit]" >> ./tmp
echo "Description=My Script Service" >> ./tmp
echo "After=multi-user.target" >> ./tmp
echo "" >> ./tmp
echo "[Service]" >> ./tmp
echo "Type=simple" >> ./tmp
echo "StandardOutput=journal" >> ./tmp
echo "StandardError=journal" >> ./tmp
echo "ExecStart=/home/ubuntu/user_server/user_server.py" >> ./tmp
echo "" >> ./tmp
echo "[Install]" >> ./tmp
echo "WantedBy=multi-user.target" >> ./tmp

chmod 644 ./tmp
mv ./tmp /lib/systemd/system/user_server.service

systemctl daemon-reload
systemctl enable user_server.service

reboot now