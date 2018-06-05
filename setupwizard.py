#!/usr/bin/env python3
import psycopg2

print('This file will allow for easy setup of your postgreSQL database and the required tables/permissions.')
print('Make sure you have followed the instructions in the documentation file leading up to running this file.')

#Obtain the appropriate information about the user account (probably the postgres user)
print('The program will need to gather information about the postgreSQL user you wish to run the installations as.')
print('We recommend this to just be the default postgres user, as it will have all the required permissions, but a different user can be used if desired.')

use_postgres = raw_input("Would you like to use the default 'postgres' user for the installation? (y/n): ")
if use_postgres is 'y':
    has_pass = raw
    