#!/bin/bash

airflow connections --add --conn_id redshift --conn_type 'postgres' --conn_host [enter host] --conn_schema sparkify --conn_login dwhadmin --conn_password [enter password] --conn_port 5439

