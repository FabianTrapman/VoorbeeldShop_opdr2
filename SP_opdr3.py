from main_webshop import *

print('- category\n'
      '- visitors\n'
      '- price')

filter = input('Hoe wil jij de data gaan ordenen.')

if filter == 'price':
      create_similar_price_tables('huwebshop', 'postgres', 'xxx', 'localhost', 27017)

elif filter == 'visitors':
      create_similar_visitors_table('huwebshop', 'postgres', 'xxx', 'localhost', 27017)

else:
      print('Dit is geen kiesbare optie.')