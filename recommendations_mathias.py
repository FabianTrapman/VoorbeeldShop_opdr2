def create_similar_visitors_table(database, user, password, host, port):
    # Connect to MongoDB
    mongo_client = pymongo.MongoClient(host=host, port=27017)
    mongo_db = mongo_client[database]
    mongo_profiles = mongo_db["profiles"]

    # Connect to PostgreSQL
    conn = psycopg2.connect(database='webshop', user=user, password=password, host=host, port=5433)
    cur = conn.cursor()

    # Query MongoDB for last viewed products by visitors
    last_viewed_products = mongo_profiles.find({"recommendations.viewed_before": {"$exists": True}},
                                               {"_id": 1, "recommendations": 1})
    # Create a new table in PostgreSQL
    #cur.execute("DROP TABLE similar_visitors")
    cur.execute(f"CREATE TABLE similar_visitors (visitor_id VARCHAR(255), product_id VARCHAR(255))")

    # Create a record in the table for each visitor_product combi
    for line in last_viewed_products:
        visitor_id = str(line["_id"])
        for product_id in line["recommendations"]["viewed_before"]:
            #print(visitor_id)
            #print(product_id)
            cur.execute(f"INSERT INTO similar_visitors (visitor_id, product_id) VALUES (%s, %s)",
                        (str(visitor_id), str(product_id)))

    # Commit the changes
    conn.commit()
    # Close the connections
    cur.close()
    conn.close()


'''
VRAAG:
- Welke producten moet ik klant X aanbevelen?
"Kalnten die dezelfde producten als klant X bekeken, hebben ook de volgende producten bekeken"
Stap 1:
- Selecteer alle klanten die ooit zelfde producten hebben bekeken als klant X
SELECT visitor_id FROM table WHERE product_id IN (SELECT product_id FROM table WHERE visitor_id = X_id)
Stap 2:
- Slecteer alle producten die al deze klanten ooit hebben bekeken, gesorteert naar meest voorkomende producten
SELECT product_id FROM table WHERE visitor_id IN (SELECT visitor_id FROM table WHERE product_id IN (SELECT product_id FROM table WHERE visitor_id = X_id)) SORT 
ProductId	Count
aaaaa		44
bbbb		23
cccc		9
ddddd		2
eeee		1
'''


def create_similar_price_tables(database, user, password, host, port):
    # Connect to MongoDB
    mongo_client = pymongo.MongoClient(host=host, port=27017)
    mongo_db = mongo_client[database]
    mongo_collection = mongo_db["products"]

    # Connect to PostgreSQL
    conn = psycopg2.connect(database='webshop', user=user, password=password, host=host, port=5433)
    cur = conn.cursor()

    # Query MongoDB for products with similar prices and create tables for each range
    ranges = [(0, 1000), (1000, 5000), (5000, 10000)]
    for r in ranges:
        min_price, max_price = r
        table_name = f"similar_prices_{min_price}_{max_price}"
        similar_products = mongo_collection.find({"price.selling_price": {"$gt": min_price, "$lt": max_price}},
                                                 {"_id": 1, "name": 1, "price": 1})

        # Create a new table in PostgreSQL
        cur.execute(f"CREATE TABLE {table_name} (_id VARCHAR PRIMARY KEY, name TEXT, price NUMERIC)")

        # Insert the data into the new table
        for product in similar_products:
            cur.execute(f"INSERT INTO {table_name} (_id, name, price) VALUES (%s, %s, %s)",
                        (product["_id"], product["name"], product["price"]["selling_price"]))

    # Commit the changes and close the connections
    conn.commit()
    cur.close()
    conn.close()
