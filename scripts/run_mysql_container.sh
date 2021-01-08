#!/bin/bash

# Create a MySql database with docker.  Will set DB root password to just password for now.  DB is published on port 8000.

WINGS_DB_STORAGE="${HOME}/docker/storage/wings_mysql/"
# create path if it doesn't exist to the data storage
if [ ! -d "${WINGS_DB_STORAGE}" ]
then
  mkdir -p "${WINGS_DB_STORAGE}"
fi

# pull the mysql container from docker hub if it isn't already there
if [ $(docker image ls | grep mysql | grep 5.7.29 | wc -l) -eq 0 ]
then
  docker pull mysql:5.7.29
fi


# run the docker container and mount the data spot
if [ ! $(ls "${WINGS_DB_STORAGE}" | wc -l) -gt 0 ]
then
  docker run --rm --detach --name=wingsmysql -v "${WINGS_DB_STORAGE}:/var/lib/mysql" --env="MYSQL_ROOT_PASSWORD=password" --env="MYSQL_DATABASE=server" --publish 8000:3306 mysql:5.7.29
  sleep 2 # let container start
#  mysql --host localhost -P 8000 --protocol=tcp -u root -p -e "create database server"
else
  docker run --rm --detach --name=wingsmysql -v "${WINGS_DB_STORAGE}:/var/lib/mysql" --publish 8000:3306 mysql:5.7.29
fi