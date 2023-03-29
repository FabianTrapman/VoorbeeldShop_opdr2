import psycopg2
from pymongo import MongoClient
import pymongo

'''
Handige dingen om op te slaan
postgres_connection_list = ['localhost', 'oefen_webshop2', 'postgres', 'pgadmin2', '5432']
fetch_products = ['products', 'localhost', 27017, 'huwebshop']


'''

dataset = {'visitors': {'_id': 'varchar(100) PRIMARY KEY', 'recommendable':'varchar(100)',
                        'sessionprofile_id': 'varchar(100)'},

           'products': {'_id': 'varchar(50) PRIMARY KEY', "price": "VARCHAR(255)",
                       'name': 'VARCHAR(255)',  'flavor': 'VARCHAR(255)',
                       'category': 'VARCHAR(255)', 'sub_category': 'VARCHAR(255)', 'sub_sub_category': 'VARCHAR(255)',
                       'sub_sub_sub_category': 'VARCHAR(255)', 'herhaalaankopen': 'boolean',
                       'recommendable': 'boolean', "brand": "VARCHAR(255)"},

           'sessions': {'profile_id': 'varchar(50) PRIMARY KEY', 'date': 'varchar(255)', 'source': 'varchar(255)',
                     'action': 'varchar(255)', 'pagetype': 'varchar(255)', 'product': 'varchar(255)',
                     'time_on_page': 'int4', 'max_time_inactive': 'int4', 'click_count': 'int4',
                     'elements_clicked': 'int4', 'scrolls_down': 'int4', 'scrolls_up': 'int4'},

           'session': {'profile_id': 'varchar(50) PRIMARY KEY'}}



def connection_mongo(host, port, database):
    '''
    Establishes a connection with the desired MongoDB database

    All parameters need to be correct and correspond with the mongoDB for it to work
    :param host: The host in wich you have your desired database located (usually localhost)
    :param port: The port that corresponds to the host
    :param database: The database that you'd like to access within the host
    :return: Connection with the database
    '''

    # Identifies the location of your database
    client = MongoClient(host=host, port=port)

    # Identifies the database within your host
    db = client[database]

    return db

def fetch_json_mongo(mongo_connection, dataset):
    '''
    Uses the connection_mongo function to access Json data located in your mongoDB

    :param folder: The folder where the desired Json data is located
    :return: A neat organized list with data

    RETURN DATA STRUCTURE
        [{'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES'},
         {'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES'},
         {'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES'}]
    '''

    folder = mongo_connection[0]
    host = mongo_connection[1]
    port = mongo_connection[2]
    database = mongo_connection[3]

    section = {}

    # Establishes a connection with your database
    db = connection_mongo(host, port, database)

    section = db[folder]
    insert_list = list(dataset[folder].keys())

    return_data = {}
    return_data_final = []

    for value in section.find():
        return_data = {}

        for value_sub in value:

            if value_sub == 'events':
                for i in range(len(value['events'])):
                    return_data1 = dict(value['events'][i-1])
                    print(dict(value['events'][i-1]))
                    return_data_final.append(return_data1)

            if value_sub in insert_list:
                if value_sub == 'price':
                    return_data[value_sub] = value[value_sub]['selling_price']
                elif value[value_sub] is None:
                    return_data[value_sub] = '-1'
                else:
                    return_data[value_sub] = value[value_sub]

        return_data_final.append(return_data)

    return return_data_final

def type_json_mongo(data, folder):
    '''
    In order to insert the data in to your PostgresDB
    it's valie types need to correspond to the column types.
    This function edits it's values to make sure there aren't any errors while inserting.

    :param data: The return data from fetch_json_mongo
    :param folder: The folder from which you imported the data
    :return:
    '''

def connection_postgres(host, database, user, pw, port):
    '''
    Establishes a connection with the desired PostgresQL database

    :param host: The host in wich you have your desired database located (usually localhost)
    :param database: The database that you'd like to access within the host
    :param user: Your postgres username (usually postgres)
    :param pw: Your postgres password (usually pgadmin2)
    :param port: The port that corresponds to the host
    :return:
    '''

    return psycopg2.connect(host=host, database=database, user=user, password=pw, port=port)

def table_postgres(data, connection_list):
    '''
    Takes structure from the dictionary and puts them in to a postgresQL database

    :param data:
    :param connection_list: Takes a list for the connection_postgres function
                            structured as followed, [host, database, user, pw, port]
    '''

    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()

    query = ''''''

    for sub in data:
        query = ''''''
        query += 'CREATE TABLE ' + sub + ' ('
        teller = 0

        for sub_sub in data[sub]:
            teller += 1
            query += sub_sub + ' ' + data[sub][sub_sub]

            if teller != len(data[sub]):
                query += ', '
            else:
                query += ');'

        cur.execute(query)

    conn.commit()
    cur.close()
    conn.close()

def insert_postgres(table_name, insert_data, connection_list):
    '''
    Takes data from the dictionary and puts them in to a postgresQL database

    :param table_name: The name of the table you'd like to add data to
    :param insert_data: A dictionary consisting of columns as keys and values as values
    :param connection_list: Takes a list for the connection_postgres function
                            structured as followed, [host, database, user, pw, port]

    INSERT DATA STRUCTURE
        {'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES'}
    '''

    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()

    for line in insert_data:
        dict(line)

        keys = list(line.keys())
        separator = ", "
        skeys = separator.join(keys)

        values = list(line.values())
        svalues = []



        for i in values:
            var = i
            if type(i) == str:
                var = i.replace("\'", ' ')
            elif type(i) == list:
                for i2 in i:
                    var = i2.replace("\'", '')

            svalues.append(var)

        svalues = str(svalues).strip('[').strip(']')
        svalues = str(svalues).strip('"')


        query = (f'''INSERT INTO ''' + table_name + ''' (''' + skeys + ''')
VALUES (''' + str(svalues) + ''');''')

        print(query)
        cur.execute(query)

    conn.commit()
    cur.close()
    conn.close()
