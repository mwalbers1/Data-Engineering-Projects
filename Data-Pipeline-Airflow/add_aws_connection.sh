#!/bin/bash

airflow connections --add --conn_id aws_credentials --conn_type 'Amazon Web Services' --conn_login [AWS key] --conn_password [AWS secret]