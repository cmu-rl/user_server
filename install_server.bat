#!/bin/bash/
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt-get update
sudo apt-get install python3.6 -y
echo 'alias python=python3.6' >> ~/.bashrc 
source ~/.bashrc

sudo apt-get install python3-pip
python3.6 -m pip install mysql-connector
sudo python3.6 -m pip install boto3

REM echo 'publickeyhere' > ~/.ssh/id_rsa.pub
REM echo '-----BEGIN RSA PRIVATE KEY----------END RSA PRIVATE KEY-----' > ~/.ssh/id_rsa

ssh-keyscan github.com >> ~/.ssh/known_hosts

git clone https://github.com/cmu-rl/user_server.git
git clone https://github.com/cmu-rl/web_server.git

echo "start on runlevel [2345]\nstop on runlevel [!2345]\nexec ~/user_server/user_server.py" >> /etc/init/runServerOnStartup.conf

sudo reboot now