#!/bin/bash

# This sets up a developer testing environment that can be used with various
# openstack projects (mainly for taskflow, but for others it should work
# fine also).
#
# Some things to note:
#
# - The mysql server that is setup is *not* secured.
# - The zookeeper server that is setup is *not* secured.
# - The downloads from external services are *not* certificate verified.
#
# Overall it should only be used for testing/developer environments (it was
# tested on ubuntu 14.04 and rhel 6.x, for other distributions some tweaking
# may be required).

set -e
set -u

# If on a debian environment this will make apt-get *not* prompt for passwords.
export DEBIAN_FRONTEND=noninteractive

# http://www.unixcl.com/2009/03/print-text-in-style-box-bash-scripting.html
Box () {
    str="$@"
    len=$((${#str}+4))
    for i in $(seq $len); do echo -n '*'; done;
    echo; echo "* "$str" *";
    for i in $(seq $len); do echo -n '*'; done;
    echo
}

Box "Installing system packages..."
if [ -f "/etc/redhat-release" ]; then
    yum install -y -q mysql-devel postgresql-devel mysql-server \
                      wget gcc make autoconf
    mysqld="mysqld"
    zookeeperd="zookeeper-server"
elif [ -f "/etc/debian_version" ]; then
    apt-get -y -qq install libmysqlclient-dev mysql-server postgresql \
                           wget gcc make autoconf
    mysqld="mysql"
    zookeeperd="zookeeper"
else
    echo "Unknown distribution!!"
    lsb_release -a
    exit 1
fi

set +e
python_27=`which python2.7`
set -e

build_dir=`mktemp -d`
echo "Created build directory $build_dir..."
cd $build_dir

# Get python 2.7 installed (if it's not).
if [ -z "$python_27" ]; then
    py_version="2.7.9"
    py_file="Python-$py_version.tgz"
    py_base_file=${py_file%.*}
    py_url="https://www.python.org/ftp/python/$py_version/$py_file"

    Box "Building python 2.7 (version $py_version)..."
    wget $py_url -O "$build_dir/$py_file" --no-check-certificate -nv
    tar -xf "$py_file"
    cd $build_dir/$py_base_file
    ./configure --disable-ipv6 -q
    make --quiet

    Box "Installing python 2.7 (version $py_version)..."
    make altinstall >/dev/null 2>&1
    python_27=/usr/local/bin/python2.7
fi

set +e
pip_27=`which pip2.7`
set -e
if [ -z "$pip_27" ]; then
    Box "Installing pip..."
    wget "https://bootstrap.pypa.io/get-pip.py" \
         -O "$build_dir/get-pip.py" --no-check-certificate -nv
    $python_27 "$build_dir/get-pip.py" >/dev/null 2>&1
    pip_27=/usr/local/bin/pip2.7
fi

Box "Installing tox..."
$pip_27 install -q 'tox>=1.6.1,<1.7.0'

Box "Setting up mysql..."
service $mysqld restart
/usr/bin/mysql --user="root" --execute='CREATE DATABASE 'openstack_citest''
cat << EOF > $build_dir/mysql.sql
CREATE USER 'openstack_citest'@'localhost' IDENTIFIED BY 'openstack_citest';
CREATE USER 'openstack_citest' IDENTIFIED BY 'openstack_citest';
GRANT ALL PRIVILEGES ON *.* TO 'openstack_citest'@'localhost';
GRANT ALL PRIVILEGES ON *.* TO 'openstack_citest';
FLUSH PRIVILEGES;
EOF
/usr/bin/mysql --user="root" < $build_dir/mysql.sql

# TODO(harlowja): configure/setup postgresql...

Box "Installing zookeeper..."
if [ -f "/etc/redhat-release" ]; then
    # RH doesn't ship zookeeper (still...)
    zk_file="cloudera-cdh-4-0.x86_64.rpm"
    zk_url="http://archive.cloudera.com/cdh4/one-click-install/redhat/6/x86_64/$zk_file"
    wget $zk_url -O $build_dir/$zk_file --no-check-certificate -nv
    yum -y -q --nogpgcheck localinstall $build_dir/$zk_file
    yum -y -q install zookeeper-server java
    service zookeeper-server stop
    service zookeeper-server init --force
    mkdir -pv /var/lib/zookeeper
    python -c "import random; print random.randint(1, 16384)" > /var/lib/zookeeper/myid
elif [ -f "/etc/debian_version" ]; then
    apt-get install -y -qq zookeeperd
else
    echo "Unknown distribution!!"
    lsb_release -a
    exit 1
fi

Box "Starting zookeeper..."
service $zookeeperd restart
service $zookeeperd status
