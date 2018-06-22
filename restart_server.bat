#!/bin/bash
sudo service user_server stop
cd ~/user_server/
git pull
sudo systemctl daemon-reload
sudo service user_server start