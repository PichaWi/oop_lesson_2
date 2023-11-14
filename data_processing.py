import csv, os
import combination_gen

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

players = []
with open(os.path.join(__location__, 'Players.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players.append(dict(r))

teams = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams.append(dict(r))

titanic = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanic.append(dict(r))

class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table

    def __is_float(self, element):
        if element is None:
            return False
        try:
            float(element)
            return True
        except ValueError:
            return False

    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            if self.__is_float(item1[aggregation_key]):
                temps.append(float(item1[aggregation_key]))
            else:
                temps.append(item1[aggregation_key])
        return function(temps)

    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def pivot_table(self, keys_to_pivot_list, keys_to_aggregate_list, aggregate_func_list):
        unique_values_list = [set(item[key] for item in self.table) for key in keys_to_pivot_list]
        combinations = combination_gen.gen_comb_list(unique_values_list)

        pivoted_table = []

        for combination in combinations:
            filtered_table = self
            for key, value in zip(keys_to_pivot_list, combination):
                filtered_table = filtered_table.filter(lambda x: x[key] == value)

            row = [value for value in combination]

            for agg_key, agg_func in zip(keys_to_aggregate_list, aggregate_func_list):
                if not hasattr(agg_func, '__code__'):
                    row.append(agg_func(filtered_table.table))
                else:
                    row.append(filtered_table.aggregate(agg_func, agg_key))

            pivoted_table.append(row)

        return pivoted_table

    def __str__(self):
        return self.table_name + ':' + str(self.table)

table1 = Table('cities', cities)
table2 = Table('countries', countries)
table3 = Table('players', players)
table4 = Table('teams', teams)
table5 = Table('titanic', titanic)
my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_DB.insert(table3)
my_DB.insert(table4)
my_DB.insert(table5)
my_table1 = my_DB.search('cities')
my_table4 = my_DB.search('players')
my_table10 = my_DB.search('titanic')

print("Test filter: only filtering out cities in Italy")
my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
print(my_table1_filtered)
print()

print("Test select: only displaying two fields, city and latitude, for cities in Italy")
my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
print(my_table1_selected)
print()

print("Calculting the average temperature without using aggregate for cities in Italy")
temps = []
for item in my_table1_filtered.table:
    temps.append(float(item['temperature']))
print(sum(temps)/len(temps))
print()

print("Calculting the average temperature using aggregate for cities in Italy")
print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
print()

print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
my_table2 = my_DB.search('countries')
my_table3 = my_table1.join(my_table2, 'country')
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
print(my_table3_filtered.table)
print()
print("Selecting just three fields, city, country, and temperature")
print(my_table3_filtered.select(['city', 'country', 'temperature']))
print()

print("Print the min and max temperatures for cities in EU that do not have coastlines")
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
print()

print("Print the min and max latitude for cities in every country")
for item in my_table2.table:
    my_table1_filtered = my_table1.filter(lambda x: x['country'] == item['country'])
    if len(my_table1_filtered.table) >= 1:
        print(item['country'], my_table1_filtered.aggregate(lambda x: min(x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
print()

print('Player who in the teams with is in word, have played less than 200 minutes and have passes more than 100')
player_select = []
my_table4_first_filtered = my_table4.filter(lambda x: 'ia' in x['team'])
my_table4_filtered = my_table4_first_filtered.filter(lambda x: int(x['minutes']) < 200).filter(lambda x: int(x['passes']) > 100)
my_table4_selected = my_table4_filtered.select(['players', 'surname', 'team', 'position'])
print(my_table4_selected)
print()

print('Games played by teams')
avg_below_ten = []
my_table5 = my_DB.search('teams')
my_table5_filtered = my_table5.filter(lambda x: int(x['ranking']) < 10)
my_table5_filtered2 = my_table5.filter(lambda x: int(x['ranking']) >= 10)
my_table11_sum = my_table5_filtered.aggregate(lambda x: sum(x), 'games')
my_table11_len = my_table5_filtered.aggregate(lambda x: len(x), 'games')
my_table12_sum = my_table5_filtered2.aggregate(lambda x: sum(x), 'games')
my_table12_len = my_table5_filtered2.aggregate(lambda x: len(x), 'games')
print('Teams ranking that below 10')
print((my_table11_sum / my_table11_len))
print('vs')
print('Teams ranking that are above or equal to 10')
print((my_table12_sum / my_table12_len))
print()

print('Number of passes made by forward vs midfielders')
my_table6_filtered = my_table4.filter(lambda x: x['position'] == 'forward')
my_table6_selected = my_table6_filtered.select(['players', 'passes'])
my_table7_filtered = my_table4.filter(lambda x: x['position'] == 'midfielder')
my_table7_selected = my_table7_filtered.select(['players', 'passes'])
my_table13_sum = my_table6_filtered.aggregate(lambda x: sum(x), 'passes')
my_table13_len = my_table6_filtered.aggregate(lambda x: len(x), 'passes')
my_table14_sum = my_table7_filtered.aggregate(lambda x: sum(x), 'passes')
my_table14_len = my_table7_filtered.aggregate(lambda x: len(x), 'passes')
print('Passes made by forward position')
print((my_table13_sum / my_table13_len))
print('vs')
print('Passes made by midfielder position')
print((my_table14_sum / my_table14_len))
print()

my_table15_filtered = my_table10.filter(lambda x: x['class'] == '1')
my_table16_filtered = my_table10.filter(lambda x: x['class'] == '3')
my_table15_sum = my_table15_filtered.aggregate(lambda x: sum(x), 'fare')
my_table15_len = my_table15_filtered.aggregate(lambda x: len(x), 'fare')
my_table16_sum = my_table16_filtered.aggregate(lambda x: sum(x), 'fare')
my_table16_len = my_table16_filtered.aggregate(lambda x: len(x), 'fare')
print('Fare of people in first class')
print(my_table15_sum / my_table15_len)
print('Vs')
print('Fare of people in third class')
print(my_table16_sum / my_table16_len)
print()

print('Survival rate of male vs female')
male_yes = []
male_no = []
female_yes = []
female_no = []
my_table17_filtered = my_table10.filter(lambda x: x['gender'] == 'M').filter(lambda x: x['survived'] == 'yes')
my_table18_filtered = my_table10.filter(lambda x: x['gender'] == 'F').filter(lambda x: x['survived'] == 'yes')
my_table19_filtered = my_table10.filter(lambda x: x['gender'] == 'M').filter(lambda x: x['survived'] == 'no')
my_table20_filtered = my_table10.filter(lambda x: x['gender'] == 'F').filter(lambda x: x['survived'] == 'no')
for item in my_table17_filtered.table:
    male_yes.append(item)
for item1 in my_table18_filtered.table:
    male_no.append(item1)
for item2 in my_table19_filtered.table:
    female_yes.append(item2)
for item3 in my_table20_filtered.table:
    female_no.append(item3)
male_all = len(male_yes) + len(male_no)
female_all = len(female_yes) + len(female_no)
print('Rate of survived of male ')
print(((male_all) - len(male_yes)) / male_all * 100)
print('Vs')
print('Rate of survived of female')
print(((female_all) - len(female_yes)) / female_all * 100)
print()

table23 = Table('titanic', titanic)
my_DB.insert(table23)
my_table23 = my_DB.search('titanic')
my_pivot = my_table23.pivot_table(['class', 'gender', 'embarked'],
                                  ['fare', 'fare', 'fare', 'last'],
                                  [lambda x: min(x), lambda x: max(x), lambda x: sum(x)/len(x), lambda x: len(x)])
print(my_pivot)
print()

my_table24 = my_DB.search('players')
my_pivot2 = my_table24.pivot_table(['position'],
                                   ['passes', 'shots'],
                                   [lambda x: sum(x)/len(x), lambda x: sum(x)/len(x)])
print(my_pivot2)
print()

my_pivot3 = my_table3.pivot_table(['EU', 'coastline'],
                                  ['temperature', 'latitude', 'latitude'],
                                  [lambda x: sum(x)/len(x), lambda x: min(x), lambda x: max(x)])
print(my_pivot3)
print()

my_pivot4 = my_table23.pivot_table(['class', 'gender', 'survived'],
                                   ['survived', 'fare'],
                                   [lambda x: len(x), lambda x: sum(x)/len(x)])
print(my_pivot4)
print()
