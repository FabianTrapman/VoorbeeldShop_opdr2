import psycopg2
from pymongo import MongoClient

dataset = {'profile': {'_id': 'varchar(10) PRIMARY KEY', 'recommendable':'varchar(10)',
                        'sessionprofile_id': 'varchar(50)'},

           'product': {'_id': 'varchar(50) PRIMARY KEY', "price": "VARCHAR(255)",
                       'name': 'VARCHAR(255)',  'flavor': 'VARCHAR(255)',
                       'category': 'VARCHAR(255)', 'sub_category': 'VARCHAR(255)', 'sub_sub_category': 'VARCHAR(255)',
                       'sub_sub_sub_category': 'VARCHAR(255)', 'herhaalaankopen': 'boolean',
                       'recommendable': 'boolean', "brand": "VARCHAR(255)"},

           'event': {'profile_id': 'varchar(50) PRIMARY KEY', 'date': 'varchar(255)', 'source': 'varchar(255)',
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

def fetch_json_mongo(folder, host, port, database, dataset):
    '''
    Uses the connection_mongo function to access Json data located in your mongoDB

    :param folder: The folder where the desired Json data is located
    :return: A neat organized list with data
    '''

    section = {}

    # Establishes a connection with your database
    db = connection_mongo(host, port, database)

    # The folder parameter decides which type of data gets accessed
    if folder == 'products':
        section = db.products
        insert_list = list(dataset['product'].keys())
    elif folder == 'sessions':
        section = db.sessions
        insert_list = list(dataset['event'].keys())
    elif folder == 'visitors':
        section = db.visitors
        insert_list = list(dataset['profile'].keys())

    return_data = {}
    return_data_final = []

    for value in section.find():
        for value_sub in value:

            if value_sub == 'events':
                for i in range(len(value['events'])):
                    return_data1 = dict(value['events'][i-1])
                    print(dict(value['events'][i-1]))
                    return_data_final.append(return_data1)

            if value_sub in insert_list:
                if value_sub == 'price':
                    return_data[value_sub] = value[value_sub]['selling_price']
                else:
                    return_data[value_sub] = value[value_sub]

        return_data_final.append(return_data)

    return return_data_final

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
    Takes data from the dictionary and puts them in to a postgresQL database

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

def insert_postgres(table_name, insert_data, connection_list):
    '''
    Takes data from the dictionary and puts them in to a postgresQL database

    :param table_name: The name of the table you'd like to add data to
    insert_data: A dictionary consisting of columns as keys and values as values
    :param connection_list: Takes a list for the connection_postgres function
                            structured as followed, [host, database, user, pw, port]
    '''

    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()

    query = f'''INSERT INTO {table_name}({list(insert_data.keys)})
                VALUES ({list(insert_data.values)})'''

    cur.execute(query)


products = fetch_json_mongo('visitors', 'localhost', 27017, 'huwebshop', dataset)

for doc in products.find():
    print(doc)