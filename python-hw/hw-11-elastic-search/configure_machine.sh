#!/usr/bin/env bash

sudo sysctl -w vm.max_map_count=262144

curl -sSL https://get.docker.com/ | sh
sudo apt-get install jq
sudo curl -L "https://github.com/docker/compose/releases/download/1.25.4/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo apt-get update
sudo apt-get install openjdk-8-jdk
sudo update-alternatives --config java

#sudo chmod 777 /usr/local/bin/docker-compose

git clone https://github.com/confluentinc/cp-demo
git clone https://github.com/axreldable/otus_data_engineer_2019_11_starikov.git

cp otus_data_engineer_2019_11_starikov/python-hw/hw-11-elastic-search/*.sh cp-demo/
cd cp-demo
chmod u+x *.sh

sudo ./scripts/start.sh
