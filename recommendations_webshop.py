import psycopg2
from pymongo import MongoClient
from main_webshop import *
import nltk
import re
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

'''
                                ****************** Similair Products ******************
                                ****************** Similair Products ******************
                                ****************** Similair Products ******************
                                ****************** Similair Products ******************
'''

def bought_product(profile_id, connection_list):
    '''
    Calls upon 2 queries to eventually get a profile's viewed products

    :param profile_id:
    :return: viewed products
    '''

    # connection with postgres gets made
    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()

    # The corresponding buid from the profile_id gets called upon
    query = f'''
        SELECT buids
        FROM BUIDS
        WHERE _id = \'{profile_id}\';'''

    # fetching buid
    cur.execute(query)
    fetched_buid = cur.fetchone()[0]

    # The products that have been viewed by this profile_id during this buid get called upon
    query2 = f'''
        SELECT product
        FROM sessions
        WHERE buid = \'{fetched_buid}\';'''

    # fetching product
    cur.execute(query2)
    fetched_product = cur.fetchall()

    conn.commit()
    cur.close()
    conn.close()

    return fetched_product


def category_product(list_fetched, connection_list):
    '''
    Takes a product that has been bought by the customer, it then compares it with all
    other products by categories. Then returns all products in a list.

    :param list_fetched:
    :return:
    '''

    prod = ''

    # Because sometimes a -1 gets passed through
    # This loop looks for a real product_id and assigns a variable to it
    for i in list_fetched:
        if '-1' not in tuple(i):
            prod = tuple(i)[0]
            break

    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()


    #
    query = f'''
            select products._id, products.category, products.sub_category, products.sub_sub_category, product_properties.doelgroep, product_properties.soort, product_properties.variant
            from products
            inner JOIN product_properties
            on products._id = product_properties._id'''

    cur.execute(query)
    fetched_list = cur.fetchall()

    dictionary = {}

    for i in fetched_list:
        if '-1' in i or None in i:
            'niks'
        else:
            dictionary[i[0]] = i[0] + ' ' + i[1] + ' ' + i[2] + ' ' + i[3] + ' ' + i[4] + ' ' + i[5] + ' ' + i[6]

    print(dictionary)
    dictionary_key = list(dictionary.keys())
    dictionary_value = list(dictionary.values())

    conn.commit()
    cur.close()
    conn.close()

    count_vect = CountVectorizer()

    count_matrix = count_vect.fit_transform(dictionary_value)

    cosine_sim = cosine_similarity(count_matrix, count_matrix)

    # Obtain the index of the product that matches the title
    idx = dictionary_key.index(prod)

    # Get the pairwsie similarity scores of all products with that product
    # And convert it into a list of tuples as described above
    sim_scores = list(enumerate(cosine_sim[idx]))

    # Sort the products based on the cosine similarity scores
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

    # Get the scores of the 30 most similar products. Ignore the first product.
    sim_scores = sim_scores[1:30]

    # Get the product indices
    product_indices = [i[0] for i in sim_scores]

    final_list = []

    for i in product_indices:
        final_list.append(dictionary_key[i])

    # Return the top 30 most similar products
    return final_list

def similair_product(profile_id, connection_list):
    '''
    When a user opens the webshop it shows the most similair products
    to products that they have viewed in the past. The function takes
    the categories into account and the product properties.
    First it takes those products properties and then filters through them with 'count vectorization',
    to find the perfect match.

    I found out about the count vectorization through a blogpost
    source : https://towardsdatascience.com/building-a-recommender-system-for-amazon-products-with-python-8e0010ec772c

    This is the main function where all the other sub functions get called upon.
    To end up putting it in to the recommended products.

    :param profile_id:
    :param connection_list:
    :return: top 30 recommended items
    '''

    return category_product(bought_product(profile_id, postgres_lijst), postgres_lijst)

postgres_lijst = ['localhost', 'webshop', 'postgres', 'pgadmin2', '5432']
print(similair_product('5a09ca9ca56ac6edb447bd76', postgres_lijst))

'''
                                ****************** most viewed products ******************
                                ****************** most viewed products ******************
                                ****************** most viewed products ******************
                                ****************** most viewed products ******************
'''

