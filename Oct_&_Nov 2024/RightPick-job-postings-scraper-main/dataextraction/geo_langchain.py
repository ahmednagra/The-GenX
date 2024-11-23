import csv
from geonamescache import GeonamesCache
from unidecode import unidecode
from langchain.chains import LLMChain
from langchain.chat_models import ChatOpenAI, ChatAnthropic
from langchain.prompts import ChatPromptTemplate, HumanMessagePromptTemplate
from dotenv import load_dotenv, find_dotenv
import re
import os
from tqdm import tqdm
import logging
from langchain.chains.openai_functions import create_structured_output_chain

# Function to extract cities from a string containing multiple cities in parentheses
def extract_cities_parens(city_string):
    logging.info(f'🔎 Extracting cities from: {city_string}')
    match = re.search(r'\(.*?\)', city_string)
    if match:
        # Remove parentheses and split by commas
        cities = match.group(0)[1:-1].split(', ')
        return cities
    else:
        # If no match is found, return the original string in a list
        return [city_string]

# Load environment variables
load_dotenv(find_dotenv())

# csvfile_city_to_country = 'info/cities_to_country_region.csv'
# csvfile_cities_known_exceptions = 'info/cities_known_exceptions.csv'


gc = GeonamesCache()
countries = gc.get_countries()
cities = gc.get_cities()

continents = {
    'AF': 'Africa',
    'AS': 'Asia',
    'EU': 'Europe',
    'NA': 'North America',
    'OC': 'Oceania',
    'SA': 'South America',
    'AN': 'Antarctica'
}

country_to_continent = {country['iso']: continents[country['continentcode']] for country in countries.values()}

countries_names = [country_data['name'] for country_data in countries.values()]
continents_names = [continent for continent in continents.values()]

with open('info/cities_known_exceptions.csv', 'r') as f:
    reader = csv.reader(f)
    known_exceptions = {row[0]: row[1] for row in reader}


#### LLM Logic ####

# Create models
chatgpt = ChatOpenAI(model_name="gpt-3.5-turbo-0125", temperature=0)  
# gpt4 = ChatOpenAI(model_name="gpt-4", temperature=0)
# claude = ChatAnthropic(model="claude-v1")

llm = chatgpt
# llm = gpt4 
# llm = claude

# Prepare a prompt for the LLM
human_template = f'''Given a list of cities, the goal is to find the corresponding country and region for each city. However, some cities are not found in the pre-defined list of known cities due to spelling variations or different names. The known countries and regions are given in the Python lists 
```python
countries = {countries_names}
regions = {continents_names}
```

'''

human_template += '''The input is a city name or a location that is not found in the list of known cities and your task is to guess the corresponding country and region. You should consider potential spelling variations, abbreviations, and alternative names when guessing. Your response should be a Python tuple with two elements: the country and the region, as they appear in the list of known countries and regions.

Here are some examples:

- If the input is "Cologne", you must return: ("Germany", "Europe")
- If the input is "Frankfurt", you must return: ("Germany", "Europe")
- If the input is "Gothenburg", you must return: ("Sweden", "Europe")
- If the input is "Hong Kong SAR", you must return: ("Hong Kong", "Asia")
- If the input is "Kuwait", you must return: ("Kuwait", "Asia")
- If the input is "Silicon Valley", you must return: ("United States", "North America")

Please implement this functionality and make sure that you return a valid Python tuple. If you cannot find a match, return `None`.

Input: "{input}"
'''

# print(human_template.format(input='Cologne'))

human_message_prompt = HumanMessagePromptTemplate.from_template(human_template)
chat_prompt = ChatPromptTemplate.from_messages([human_message_prompt])

chain = LLMChain(llm=llm, prompt=chat_prompt)

def guess_country_and_region(city):
    result = chain.predict(input=city)
    match = re.search(r'\((\'[^\']*\'|\"[^\"]*\"), (\'[^\']*\'|\"[^\"]*\")\)', result)
    if match:
        country_guessed, region_guessed = eval(match.group(1).strip()), eval(match.group(2).strip())
        logging.info(f'✅ Country and Region found by the LLM: {city} → {country_guessed}, {region_guessed}')
        return country_guessed, region_guessed
    else:
        logging.error(f'❌ Country and Region not found by the LLM: {city}')
        return None, None
    
### City to Country and Region Dictionary ###

def build_city_to_country_region_dict():
    cities_to_countries_regions = {} 
    for city_data in cities.values():
        city_name = unidecode(city_data['name'])
        # if city_name already in cities_to_countries_regions, append city_data to the list of cities with the same name
        if city_name in cities_to_countries_regions:
            cities_to_countries_regions[city_name].append(city_data)
        # else, create a new entry in cities_to_countries_regions
        else:
            cities_to_countries_regions[city_name] = [city_data]

    cities_to_country_region = {}
    for city, city_data_list in tqdm(cities_to_countries_regions.items()):
        if len(city_data_list) == 1:
            city_data = city_data_list[0]
            several_occurrences = False
        else:
            # if a city is found with several occurrences, get the most populous one
            city_data = sorted(city_data_list, key=lambda x: x['population'], reverse=True)[0]
            several_occurrences = True
        country = countries[city_data['countrycode']]['name']
        region = country_to_continent[city_data['countrycode']]
        if several_occurrences:
            logging.info(f'✅ Resolved ambiguous city name: {city} → {country}, {region}')

        cities_to_country_region[city] = {'country': country, 'region': region}
    return cities_to_country_region


if os.path.exists('info/cities_to_country_region.csv'):
    cities_to_country_region = {}
    with open('info/cities_to_country_region.csv', mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            city, country, region = row
            cities_to_country_region[city] = {'country': country, 'region': region}
else:
    cities_to_country_region = build_city_to_country_region_dict()
    with open('info/cities_to_country_region.csv', mode='w', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        for city, data in cities_to_country_region.items():
            writer.writerow([city, data['country'], data['region']])


def get_city_info(city):
    # Check if the city is a known exception
    city_name = known_exceptions.get(city, city)

    # Use the unaccented version of the city name to get the city data
    country_region_data = cities_to_country_region.get(unidecode(city_name))

    if not country_region_data:
        # If the city is not found, iterate through all cities and use alternate names
        for city in cities.values():
            if city_name in city['alternatenames']:
                country_region_data = {
                    'country': countries[city['countrycode']]['name'],
                    'region': country_to_continent[city['countrycode']]
                }
                # Save the known exception into known_exceptions and CSV
                unidecode_real_city_name = unidecode(city['name'])
                known_exceptions[city_name] = unidecode_real_city_name
                with open('info/cities_known_exceptions.csv', 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([city_name, unidecode_real_city_name])
                break
        # If the city is still not found, use the LLM to guess the country and region
        if not country_region_data:
            country, region = guess_country_and_region(city_name)
            # Save the guessed country and region into cities_to_country_region and CSV
            if country is not None and region is not None:
                country_region_data = {"country": country, "region": region}
                cities_to_country_region[unidecode(city_name)] = country_region_data
                with open('info/cities_to_country_region.csv', 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([unidecode(city_name), country, region])
    
    if not country_region_data:
        raise Exception(f'❌ City not found: {city_name}')

    return country_region_data['country'], country_region_data['region']



# cities_examples = ["Abu Dhabi", "Amsterdam", "Athens", "Atlanta", "Austin", "Bangkok", "Beijing", "Berlin", "Bogota", "Boston", "Brussels", "Bucharest", "Buenos Aires", "Cairo", "Calgary", "Casablanca", "Charlotte", "Chicago", "Cleveland", "Cologne", "Colombo", "Columbus", "Dallas", "Denver", "Detroit", "Doha", "Dusseldorf", "Frankfurt", "Geneva", "Gothenburg", "Gurugram", "Hamburg", "Hanoi", "Helsinki", "Hong Kong SAR", "Houston", "Tokyo", "Istanbul", "Jakarta", "Johannesburg", "Kuala Lumpur", "Kuwait", "Lagos", "Lima", "London", "Luanda", "Luxembourg", "Manama", "Manila", "Panama City", "Santiago", "Sao Paulo", "Medellin", "Milan", "Munich", "Rio de Janeiro", "Shanghai", "Shenzhen", "Singapore City", "Stockholm", "Stuttgart", "Taipei", "Vienna", "Zurich", "Mexico City", "Miami", "Minneapolis", "Montevideo", "Montreal", "Nairobi", "New Jersey", "New York City", "Lisbon", "Lyon", "Madrid", "Rome", "Seoul", "Osaka", "Oslo", "Philadelphia", "Pittsburgh", "Prague", "Paris", "Quito", "Riyadh", "San Francisco", "Santo Domingo", "Seattle", "Silicon Valley", "Los Angeles", "St. Louis", "Stamford", "Tel Aviv", "Toronto", "Vancouver", "Washington DC"]


# for city in cities_examples:
#     city_info = get_city_info(city)
#     print(f'{city}: {city_info}')


# # Print cities named "Athens"
# print([c for c in cities.values() if c['name'] == 'Athens'])

def get_locations(locations_list, job_title_and_description=None):
    # Process the list to handle strings containing multiple cities
    final_location_list = []
    for location in locations_list:
        if isinstance(location, list):
            for city_string in location:
                final_location_list.extend(extract_cities_parens(city_string))
        else:
            final_location_list.extend(extract_cities_parens(location))

    # If final_location_list is empty, try to get the location from the job title and description
    if not final_location_list and job_title_and_description:
        job_title, job_description = job_title_and_description
        final_location_list = infer_location_from_description(job_title, job_description, llm=llm)
    
    # Get the country and region for each city
    final_location_list = [(city,) + get_city_info(city) for city in final_location_list]

    return final_location_list


def infer_location_from_description(job_title, job_description, llm=llm):
    # Prepare a prompt for the LLM
    prompt = f'''
    Given the job title and description, try to infer the location (i.e. city or cities) of the job. 

    # Job Title
    {job_title}


    # Job Description
    {job_description}

    Please infer the location (city or cities) of the job.
    '''

    location_json_schema = {
        'name': 'extract_locations',
        'description': 'Extract the locations (list of cities where the job is offered) from a job posting.',
        'type': 'object',
        'properties': {
            'inferred_locations': {
                'title': 'Inferred Locations',
                'type': 'array',
                'items': {
                    'type': 'string'
                },
                'description': 'Locations (list of cities where the job is offered) inferred from the job posting.'
            }
        }
    }

    # Use the LLM to predict the location
    chat_prompt = ChatPromptTemplate.from_messages([HumanMessagePromptTemplate.from_template(prompt)])

    chain = create_structured_output_chain(llm=llm, prompt=chat_prompt, output_schema=location_json_schema)
    result_llm = chain.run(job_title=job_title, job_description=job_description)
    
    result_dict = [city if city != 'None' else None for city in result_llm['inferred_locations']]

    return result_dict

# # Test the function infer_location_from_description via get_locations

# problematic_job = {"url": "https://www.metacareers.com/jobs/209207782207703/", "title": "Research Scientist Intern, Human Computer Interaction, Toronto | Chercheur stagiaire dans le domaine de l'interaction homme-machine, Toronto", "location": [], "description": "Meta's mission is to give people the power to build community and bring the world closer together. Through our family of apps and services, we empower billions to share what matters most to them and strengthen connections. At Meta, our teams are dedicated builders, constantly iterating and solving problems to help individuals worldwide build communities and connect meaningfully. Join us in this mission as a Research Intern at Reality Labs, where we focus on inventing user interface technologies for augmented-reality experiences. You'll contribute to developing novel interactions and interfaces, and evaluating their effectiveness in terms of learnability, expertise development, and human factor considerations like comfort and fatigue.\n\nWe are looking for PhD candidates to work alongside expert researchers, designers, and software engineers. "}


# print(get_locations(problematic_job['location'], (problematic_job['title'], problematic_job['description'])))