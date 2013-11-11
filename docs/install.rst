Installation of a storage machine
=================================

.. highlight:: bash

Setting up required software
----------------------------
Update the package lists, and install the required system software::

 $ sudo apt-get update
 $ sudo apt-get install --yes qemu-utils virtualenvwrapper git \
     python-pip

Setting up NFS share
--------------------
Install the nfs-server package:: 
 
 $ sudo apt-get install nfs-server

Create the datastore directory::

 $ mkdir /datastore
 $ sudo chown cloud:cloud /datastore

Edit NFS exports::

 $ vim /etc/exports

Restert the nfs service::
 $ sudo /etc/init.d/nfs-kernel-server restart

Setting up Storage itself
-------------------------
Clone the git repository::

 $ git clone git@git.ik.bme.hu:circle/storagedriver.git storagedriver

Set up *virtualenvwrapper* and the *virtual Python environment* for the
project::

  $ source /etc/bash_completion.d/virtualenvwrapper
  $ mkvirtualenv storage

Set up default Storage configuration and activate the virtual environment::

  $ cat >>/home/cloud/.virtualenvs/storage/bin/postactivate <<END
  export AMQP_URI='amqp://cloud:password@host:5672/circle'
  END
  $ workon circle
  $ cd ~/storagedriver

Install the required Python libraries to the virtual environment::

  $ pip install -r requirements/local.txt

Copy the upstart scripts for celery services::

  $ sudo cp miscellaneous/storagecelery.conf /etc/init/

Start celery daemons::

  $ sudo start storagecelery
