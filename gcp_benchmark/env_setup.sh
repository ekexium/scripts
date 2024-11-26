#!/bin/bash

./gcp_setup.sh -v nightly -n nightly

curl https://sh.rustup.rs -sSf | sh -s -- -y
. "$HOME/.cargo/env"

sudo yum -y install curl
sudo yum -y install git
sudo yum -y install make
sudo yum -y install bison
sudo yum -y install gcc
sudo yum -y install glibc-devel
bash < <(curl -s -S -L https://raw.githubusercontent.com/moovweb/gvm/master/binscripts/gvm-installer)
source ~/.gvm/scripts/gvm
gvm install go1.23.2 -B
gvm use go1.23.2

cd ~
git clone https://github.com/ekexium/tidb.git
cd tidb
git checkout variable-pessimistic-autocommit
make server
cd bin
tar czvf tidb-server.tar.gz tidb-server
tiup cluster patch nightly tidb-server.tar.gz -R tidb -y