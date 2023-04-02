import psycopg2
from pymongo import MongoClient

# A dictionary to make the table_postgres function run more smoothly
dataset = {'visitors': {'_id': 'varchar(100) PRIMARY KEY', 'recommendable': 'varchar(100)',
                        'sessionprofile_id': 'varchar(100)'},

           'products': {'_id': 'varchar(50) PRIMARY KEY', "price": "VARCHAR(255)",
                        'name': 'VARCHAR(255)',  'flavor': 'VARCHAR(255)',
                        'category': 'VARCHAR(255)', 'sub_category': 'VARCHAR(255)', 'sub_sub_category': 'VARCHAR(255)',
                        'sub_sub_sub_category': 'VARCHAR(255)', 'herhaalaankopen': 'boolean',
                        'recommendable': 'boolean', "brand": "VARCHAR(255)"},

           'sessions': {'t': 'varchar(255)', 'source': 'varchar(255)',
                        'action': 'varchar(255)', 'pagetype': 'varchar(255)', 'product': 'varchar(255)',
                        'time_on_page': 'int4', 'max_time_inactive': 'int4', 'click_count': 'int4',
                        'elements_clicked': 'int4', 'scrolls_down': 'int4', 'scrolls_up': 'int4', 'buid': 'varchar(255)'},

           'BUIDS': {'_id': 'varchar(50)', 'buids': 'varchar(50)'}
           }


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

    :param mongo_connection: A list with mongoDB info
    :param dataset: A dictionary with the desired values and its types
    :return: A neat organized list with data

    RETURN DATA STRUCTURE
        [{'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES'},
         {'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES'},
         {'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES'}]
    '''

    # Establishes a connection with your database
    folder = mongo_connection[0]
    host = mongo_connection[1]
    port = mongo_connection[2]
    database = mongo_connection[3]
    db = connection_mongo(host, port, database)
    section = db[folder]

    # The mongoDB has many attributes, this list will make sure that
    # we only extract the attributes that we need, using an if statement
    insert_list = list(dataset[folder].keys())

    # All extracted data dictionaries will be added to this list
    return_data_final = []

    # when testing a small amount of data is inserted
    # teller = 0

    # Looping through mongoDB data
    for value in section.find():

        # when testing
        # teller += 1
        # if teller == 10:
        #     break

        # For every instance a new data dictionary is made
        return_data = {}

        # Looping through mongoDB data keys
        for value_sub in value:

            # The line's buid gets added
            try:
                buid_data = value['buid'][0]
            except:
                'niks'

            # Since the events object is a nested dictionary
            # we need to loop through it seperately
            if value_sub == 'events':

                for i in range(len(value['events'])):
                    return_data1 = dict(value['events'][i-1])
                    return_data1['buid'] = buid_data
                    return_data_final.append(return_data1)

            # Line 65, 66
            if value_sub in insert_list:

                # Since the price object is a nested dictionary
                if value_sub == 'price':
                    return_data[value_sub] = value[value_sub]['selling_price']

                # If any data is missing, a '-1' will take it's place
                elif value[value_sub] is None:
                    return_data[value_sub] = '-1'

                # Most data then gets put in as 'COLUMNS': 'VALUES'
                else:
                    return_data[value_sub] = value[value_sub]

        print(return_data)
        return_data_final.append(return_data)
    print(return_data_final)
    return return_data_final

def fetch_query_BUIDS(mongo_connection, postgres_connection):
    '''
    Makes a dictionary with BUIDS as its key and the corresponding profile_id as value

    :param mongo_connection: A list with mongoDB info
    :param buids_dict: A dictionary with
    :return:
    '''

    # Establishes a connection with your database
    folder = 'visitors'
    host = mongo_connection[1]
    port = mongo_connection[2]
    database = mongo_connection[3]
    db = connection_mongo(host, port, database)
    section = db[folder]

    buids_dict = {}

    for line in section.find():
        try:
            for buid in line['buids']:
                val = str(line['_id'])
                val2 = val.replace("'", '')
                buids_dict[buid] = val
        except:
            'dan niet'

    # A connection with the Database
    conn = connection_postgres(postgres_connection[0], postgres_connection[1], postgres_connection[2],
                               postgres_connection[3], postgres_connection[4])

    cur = conn.cursor()

    # The keys act as column names, query line 1
    keys = list(buids_dict.keys())

    # It's values act as values, query line 2
    values = list(buids_dict.values())

    for i in range(len(keys)):

        # Data gets inserted
        query = (f'''INSERT INTO BUIDS (buids, _id)
VALUES (\'''''' + keys[i-1] + '''\', \'''' + values[i-1] + '''\');''')

        print(query)
        cur.execute(query)

    # When all id's are query'd, they get commited
    conn.commit()
    cur.close()
    conn.close()


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


def type_json_mongo(data, folder):
    '''
    In order to insert the data in to your PostgresDB
    it's valie types need to correspond to the column types.
    This function edits its values to make sure there aren't any errors while inserting.

    :param data: The return data from fetch_json_mongo
    :param folder: The folder from which you imported the data
    :return: A neater version of data
    '''

    svalues = []

    if folder == 'products':

        for i in data:
            var = i
            if type(i) == str:
                var = i.replace("\'", ' ')
            elif type(i) == list:
                for i2 in i:
                    var = i2.replace("\'", '')

            svalues.append(var)

        svalues = str(svalues).strip('[').strip(']')
        svalues = str(svalues).strip('"')

    elif folder == 'visitors':

        svalues = str(svalues).replace('"', '\'')

    elif folder == 'sessions':
        for val in data:
            if val == None:
                svalues.append(-1)
            else:
                svalues.append(val)

        svalues[0] = str(svalues[0])

        svalues = str(svalues).strip('[').strip(']')


    return svalues


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

    # A connection with the Database
    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()

    # For every id in the data, a query gets made
    for line in insert_data:
        dict(line)

        # The keys act as column names, query line 1
        keys = list(line.keys())
        if len(keys) != 1:
            separator = ", "
            skeys = separator.join(keys)

            # It's values act as values, query line 2
            values = list(line.values())
            svalues = type_json_mongo(values, table_name)
            print(svalues)
            print(len(svalues))

            # Data gets inserted
            query = (f'''INSERT INTO ''' + table_name + ''' (''' + skeys + ''')
    VALUES (''' + str(svalues) + ''');''')

            print(query)
            cur.execute(query)

    # When all id's are query'd, they get commited
    conn.commit()
    cur.close()
    conn.close()