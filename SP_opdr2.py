from main_webshop import *

print('- products\n'
      '- visitors\n'
      '- sessions\n'
      '- BUIDS')
folder = input('Welke  folder wil je overzetten naar je database?: ')

# De tabel len worden gemaakt in de postgresQL database
postgres_lijst = ['localhost', 'webshop', 'postgres', 'pgadmin2', '5432']
# table_postgres(dataset, postgres_lijst)

# De data wordt uit de Mong oDB gehaald als Json, en de benodigde data wordt
# er uit gefilterd en netjes weergegeven in een lijst met dictionary's
if folder == 'BUIDS':
    mongo_lijst = ['visitors', 'localhost', 27017, 'huwebshop']
    fetch_query_BUIDS(mongo_lijst, postgres_lijst)

else:
    fetch_data = [folder, 'localhost', 27017, 'huwebshop']
    fetched_data = fetch_json_mongo(fetch_data, dataset)

    # De data wordt op de juiste plek gezet in de postgresQL database
    # insert_postgres(folder, fetched_data, postgres_connection_list)
    insert_postgres(folder, fetched_data, postgres_lijst)
