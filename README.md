![](https://github.com/benw1/WINGS/workflows/WINGS/badge.svg?branch=develop)

# WINGS
WFIRST Infrared Nearby Galaxies Survey helpful code for making and processing WFIRST simulations

## Getting Started with `wpipe`

These instructions will get you a copy of the project up and running on your local machine for usage.

### Prerequisites

You need to have MySQL installed and a working Python3 installation. If your machine can connect to the Python Package Index (PyPI), the installation `wpipe` process will handle the package necessary dependencies. Otherwise, you will need to have the following packages installed:

```
numpy
pandas
tenacity
tables
sqlalchemy
mysql-connector-python
mysqlclient
astropy
jinja2
```

### Installing `wpipe`

#### Getting the sources files

You will first need to download the source files from this project to install the package. You may do so by either cloning the project and switching to the [`develop`](https://github.com/benw1/WINGS/tree/develop) branch with the following commands (given you have git installed):

```
git clone https://github.com/benw1/WINGS.git
cd WINGS
git checkout develop
```

or by downloading the [`develop`](https://github.com/benw1/WINGS/tree/develop) branch ZIP [here](https://github.com/benw1/WINGS/archive/develop.zip) and unzip it.

#### Installing the package

Once in the root directory of the project, you can install the package either using `pip install` the following way (recommended):

```
pip install .
```

or by simply running:

```
python setup.py install
```

#### Preparing the MySQL server

The package `wpipe` requires a working and running MySQL server to function. You may use either a local MySQL installation or prepare a MySQL database Docker container via the scripts provided in the [scripts](https://github.com/benw1/WINGS/tree/develop/scripts) directory. In both cases, `wpipe` is linked to the server via an engine URL that it needs before importing. If using a local MySQL installation, that engine URL must be set as an environment variable `WPIPE_ENGINEURL` in the following format:

```
mysql+pymysql://<username>:<password>@localhost/server
```
For example, in bash:
```
export WPIPE_ENGINEURL="mysql+pymysql://<username>:<password>@localhost/server"
```

If you want to have the MySQL server placed into its own Docker container, the following instructions will help you getting it set up.

##### Launching MySql 5.7.29 Docker Container

Install MySQL client if not already on your maching, for example
https://formulae.brew.sh/formula/mysql-client
or
sudo apt-get install mysql-client
or
https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-install.html

Make sure [Docker](https://www.docker.com/get-started) is setup on your computer, and that it is running.  On a mac, it can be started with:
```
open -a Docker
```

Then run the bash script in the script/ subdirectory
```
scripts/run_mysql_container.sh
```
This will pull the MySQL image, if needed, and will store the database in `"${HOME}/docker/storage/wings_mysql/"` when the container runs.  This will launch a database on port 8000 with root password of *password*.  To bring up the command line for the server run this command (make sure you have the MySQL client installed to be able to run the mysql commmand):

```
mysql --host localhost -P 8000 --protocol=tcp -u root -p
```

You will be prompted for a password, which is "password" by default.  The rest of the server setup should be handled by running the pipeline (ie running create database if needed).

Now set the environment variable for the engine url using your newly-installed docker database:

```
export WPIPE_ENGINEURL="mysql+pymysql://root:password@localhost:8000/server"
```

In this case, wpipe uses `pymysql` as the connector to MySQL only because there was trouble using other connectors for SQLAlchemy.  You can try other connectors, but ensure you have `pymysql` installed if you do use it.  Note that the repo install doesn't install `pymysql` since we don't use it in deployment. To install pymysql:

```
pip install PyMySQL
```

#### Setting useful environment variables

##### `PBS Scheduler`

The `wpipe` package has been developed in such way that it defaults to run jobs through the job scheduler software Portable Batch System (PBS). If your machine does not support PBS, you may remove that feature by setting up an environment variable `WPIPE_NO_PBS_SCHEDULER` to the value `1`.

##### User `default`

By default, the `wpipe` package will associate any entries added to the database to the user `default`. To use a different user name, you may enter that name in the environment variable `WPIPE_USER`.


### Running a pipeline

#### Making your first pipeline

After creating a directory anywhere on your machine and opening a terminal in it, you may run the following command to initialize a pipeline in that directory:

```
wingspipe init
```

This will initialize an empty pipeline in this directory, but you will want to associate some tasks and inputs to it. The source files of the package comes with a [test pipeline folder](https://github.com/benw1/WINGS/tree/develop/src/test) containing files that can be used to experiment with the creation of a pipeline. You may use these files to populate the pipeline in the following way:

```
wingspipe init -w <PATH_TO_WINGS>/src/test/data/tasks/ -i <PATH_TO_WINGS>/src/test/data/inputs/ -c <PATH_TO_WINGS>/src/test/data/default.conf
```

The 3 flags used in this command, namely `-w`, `-i` and `-c`, correspond respectively to the folder of task python files, the folder of input data files, and the default configuration file to be used. The task python files may be of interest as it shows how tasks should be written to make use of the wpipe functionalities.

#### Running and deleting a pipeline

Once a pipeline is ready to be run, simply use the command

```
wingspipe run
```

If you want to remove a pipeline, you may do so by using

```
wingspipe delete
```


### Stopping docker database


For stopping the docker container use the normal Docker command line tools and look for the container named *wingsmysql*.
```
docker container stop wingsmysql
```


## Contributing

<!-- Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details. -->

## Authors

See the list of [contributors](https://github.com/benw1/WINGS/graphs/contributors) who participated in this project.

## License

<!-- This project is licensed under the  - see the [LICENSE.md](LICENSE.md) file for details -->

## Acknowledgments
