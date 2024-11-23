import csv
from geonamescache import GeonamesCache
from unidecode import unidecode
import re
import os
from tqdm import tqdm
import logging
from .utils import chat_completion_request, execute_function_call

# Function to extract cities from a string containing multiple cities in parentheses
def extract_cities_parens(city_string):
    """
    Extract cities from a string containing multiple cities in parentheses or separated by slashes.

    Args:
        city_string (str): A string containing city names, which may include multiple cities in parentheses or separated by slashes.

    Returns:
        list: A list of city names extracted from the input string, with state information preserved for US cities.

    Example:
        >>> extract_cities_parens("Silicon Valley (San Francisco, Mountain View, Palo Alto)")
        ['San Francisco', 'Mountain View', 'Palo Alto']
        >>> extract_cities_parens("San Francisco, CA/New York, NY/Chicago, IL")
        ['San Francisco, CA', 'New York, NY', 'Chicago, IL']
        >>> extract_cities_parens("Boston, MA/Paris, France (multiple locations)")
        ['Boston, MA', 'Paris']
        >>> extract_cities_parens("Wilmington, NC/Arlington, VA")
        ['Wilmington, NC', 'Arlington, VA']
    """
    logging.info(f'🔎 Extracting cities from: {city_string}')

    def process_city_string_containing_commas(city_string):
        """
        Given a string representing a city or a list of cities, extract the cities.
        """
        system_template = '''The input is a string representing a city or a list of cities. Your task is to return the corresponding list of cities.

        Here are some examples:

        - If the input is "Frankfurt, Germany", you must return: ["Frankfurt"]
        - If the input is "Frankfurt, Berlin, Stuttgart", you must return: ["Frankfurt", "Berlin", "Stuttgart"]
        - If the input is "Gothenburg", you must return: ["Gothenburg"]
        - If the input is "Charleston, SC; Wilmington, NC", you must return: ["Charleston, SC", "Wilmington, NC"]
        - If the input is "Remote, UK", you must return: ["Remote, UK"]
        - If the input is "Rayleigh, United States - New Jersey or United States - North Carolina; Weehawken in United States - New Jersey or United States - North Carolina", you must return: ["Rayleigh, NJ", "Weehawken, NC"]
        - If the input is "New York, London", you must return: ["New York", "London"]
        - If the input is "New York, United States", you must return: ["New York"]
        - If the input is "Silicon Valley (San Francisco, Mountain View, Palo Alto)", you must return: ["San Francisco", "Mountain View", "Palo Alto"]
        - If the input is "Boston, MA/Paris, France (multiple locations)", you must return: ["Boston, MA", "Paris"]
        - If the input is "San Francisco, CA/New York, NY/Chicago, IL", you must return: ["San Francisco, CA", "New York, NY", "Chicago, IL"]
        - If the input is "Wilmington, NC/Arlington, VA", you must return: ["Wilmington, NC", "Arlington, VA"]

        Please implement this functionality and make sure that you return a valid Python list. If you cannot find a match, return [].
        '''
        human_prompt = f'''Extract the cities from the following string: "{city_string}"'''

        extract_cities_tool = {
            "type": "function",
            "function": {
                "name": "extract_cities",
                "description": "Extract city names from the given string.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "cities": {
                            "type": "array",
                            "items": {
                                "type": "string"
                            },
                            "description": "List of extracted city names"
                        }
                    },
                    "required": ["cities"]
                }
            }
        }

        response = chat_completion_request(
            messages=[
                {"role": "system", "content": system_template},
                {"role": "user", "content": human_prompt}
            ],
            tools=[extract_cities_tool],
            tool_choice={"type": "function", "function": {"name": "extract_cities"}}
        )

        result = execute_function_call(response.choices[0].message, function_name="extract_cities")

        city_list = result.get('cities', [])
        
        if city_list:
            logging.info(f'🏙️ Cities extracted by function calling: {city_string} → {city_list}')
        else:
            logging.error(f'❌🏙️ No cities found by the LLM: {city_string}')
            # Fallback: return the original string as a single-item list
            city_list = [city_string]

        return city_list

    # Use the general-purpose function to extract cities
    cities = process_city_string_containing_commas(city_string)
    
    # Process each city to ensure consistent formatting
    return [city.strip() for city in cities if city.strip()]


gc = GeonamesCache()
countries = gc.get_countries()
cities = gc.get_cities()
sorted_cities_values = sorted(cities.values(), key=lambda x: x['population'], reverse=True)

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

# Get the current directory of the script (which is inside 'dataextraction')
script_dir = os.path.dirname(os.path.abspath(__file__))

# Move up one level to get the project root
project_root = os.path.abspath(os.path.join(script_dir, '..'))

# Construct the full path to the CSV file from the project root
csv_file_path = os.path.join(project_root, 'info', 'cities_known_exceptions.csv')

# Check if the CSV file exists at the constructed path
if os.path.exists(csv_file_path):
    print(f"CSV file found at {csv_file_path}")
else:
    # Fall back to relative path if the file is not found
    csv_file_path = 'info/cities_known_exceptions.csv'
    print(f"Falling back to relative path: {csv_file_path}")


# csv_file_path = csv_file_path or 'info/cities_known_exceptions.csv'
# with open('info/cities_known_exceptions.csv', 'r') as f:
with open(csv_file_path, 'r') as f:
    reader = csv.reader(f)
    known_exceptions = {}
    for row in reader:
        if len(row) >= 2:
            known_exceptions[row[0]] = row[1]
        else:
            logging.warning(f"Skipping invalid row in cities_known_exceptions.csv: {row}")


#### LLM Logic ####

# Prepare a prompt for the LLM
system_template = f'''Given a list of cities, the goal is to find the corresponding country and region for each city. However, some cities are not found in the pre-defined list of known cities due to spelling variations or different names. The known countries and regions are given in the following Python lists: 
```python
countries = {countries_names}
regions = {continents_names}
```
'''


def guess_standardized_city_country_and_region(city, system_template=system_template):
    """
    Given a city name or a location that is not found in the list of known cities,
    guess the corresponding country and region. Consider potential spelling variations,
    abbreviations, and alternative names when guessing. Handle ambiguous cases by
    including state abbreviations when necessary.

    Args:
        city (str): The name of the city or location to guess the country and region for.
        system_template (str, optional): The system template for the LLM prompt, including the list of known countries and regions.

    Returns:
        tuple: A tuple with three elements: the standardized city name, the country, and the region, as they appear in the list of known countries and regions.
                If no match is found, returns (None, None, None).

    Example:
        >>> guess_standardized_city_country_and_region("New York, NY, United States")
        ("New York City", "United States", "North America")
        >>> guess_standardized_city_country_and_region("Charleston, SC")
        ("Charleston, SC", "United States", "North America")
        >>> guess_standardized_city_country_and_region("Arlington, TX")
        ("Arlington, TX", "United States", "North America")
    """
    human_template = '''The input is a city name or a location that is not found in the list of known cities. Your task is to:
1. Standardize the city name:
   - Use the most common, internationally recognized name for the city
   - Resolve abbreviations and alternative names
   - Remove extraneous information (e.g., country names)
   - Preserve special cases such as "Silicon Valley", i.e., retain well-known tech hubs or major metropolitan areas that a job-seeking user would likely search for as if they were cities
   - For ambiguous city names in the United States, include the state abbreviation (e.g., "Charleston, SC" or "Wilmington, NC", because there are also cities named "Charleston, WV" and "Wilmington, DE")
2. Determine the corresponding country
3. Identify the associated region (continent): 'Africa', 'Asia', 'Europe', 'North America', 'Oceania', 'South America', 'Antarctica', 'Worldwide'
   - Match the country to its continent
   - Use "Worldwide" for global remote work opportunities

Consider potential spelling variations, abbreviations, and alternative names when guessing. Your response should be a Python tuple with three elements: the standardized city name, the country, and the region, as they appear in the list of known countries and regions.

Here are some examples:

- If the input is "Frankfurt", you must return: ("Frankfurt", "Germany", "Europe")
- If the input is "Frankfurt Am Main", you must return: ("Frankfurt", "Germany", "Europe")
- If the input is "Gothenburg", you must return: ("Gothenburg", "Sweden", "Europe")
- If the input is "Hong Kong SAR", you must return: ("Hong Kong", "Hong Kong", "Asia")
- If the input is "Kuwait", you must return: ("Kuwait City", "Kuwait", "Asia")
- If the input is "Silicon Valley", you must return: ("Silicon Valley", "United States", "North America")
- If the input is "Remote" or "Work at Home" or "Virtual", you must return: ("Remote", "Worldwide", "Worldwide")
- If the input is "Remote, UK", you must return: ("Remote", "United Kingdom", "Europe")
- If the input is "New York, NY, United States", you must return: ("New York City", "United States", "North America")
- If the input is "Toronto, ON", you must return: ("Toronto", "Canada", "North America")
- If the input is "Taipei City", you must return: ("Taipei", "Taiwan", "Asia")
- If the input is "Waterfall City", you must return: ("Waterfall City", "South Africa", "Africa")
- If the input is "Becker", you must return: ("Becker", "United States", "North America")
- If the input is "Charleston, SC", you must return: ("Charleston, SC", "United States", "North America"), because there is also a city "Charleston, WV"
- If the input is "Wilmington, NC", you must return: ("Wilmington, NC", "United States", "North America"), because there is also a city "Wilmington, DE"
- If the input is "Arlington, TX", you must return: ("Arlington, TX", "United States", "North America"), because there is also a city "Arlington, VA"

Please implement this functionality and make sure that you return a valid Python tuple. If you cannot find a match, return (None, None, None).

Input: "{input}"
'''
    result = chat_completion_request(
        messages=[
            {"role": "system", "content": system_template},
            {"role": "user", "content": human_template.format(input=city)}
        ],
    )
    match = re.search(r'\((\'[^\']*\'|\"[^\"]*\"), (\'[^\']*\'|\"[^\"]*\"), (\'[^\']*\'|\"[^\"]*\")\)', result.choices[0].message.content)
    if match:
        city_standardized, country_guessed, region_guessed = eval(match.group(1).strip()), eval(match.group(2).strip()), eval(match.group(3).strip())
        logging.info(f'✅ City, Country and Region found by the LLM: {city} → {city_standardized}, {country_guessed}, {region_guessed}')
        return city_standardized, country_guessed, region_guessed
    else:
        logging.error(f'❌ City, Country and Region not found by the LLM: {city}')
        return None, None, None

### City to Country and Region Dictionary ###

def build_city_to_country_region_dict():
    """
    Build a dictionary mapping city names to their corresponding country and region.

    This function processes a dictionary of city data, resolves any ambiguities by selecting the most populous city
    when multiple cities share the same name, and creates a mapping of city names to their respective country and region.

    Returns:
        dict: A dictionary where keys are city names and values are dictionaries with 'country' and 'region' keys.

    Example:
        >>> build_city_to_country_region_dict()
        {
            'New York City': {'country': 'United States', 'region': 'North America'},
            'London': {'country': 'United Kingdom', 'region': 'Europe'},
            'Paris': {'country': 'France', 'region': 'Europe'},
            # ...
        }
    """
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


# Construct the full path to the CSV file from the project root
cities_to_country_file_path = os.path.join(project_root, 'info', 'cities_to_country_region.csv')

# Check if the CSV file exists at the constructed path
if os.path.exists(cities_to_country_file_path):
    print(f"CSV Cities to country file found at {cities_to_country_file_path}")
    cities_to_country_region = {}
    # with open('info/cities_to_country_region.csv', mode='r') as infile:
    with open(cities_to_country_file_path, mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            city, country, region = row
            cities_to_country_region[city] = {'country': country, 'region': region}

            a=1
else:
    # Fall back to relative path if the file is not found
    cities_to_country_file_path = 'info/cities_to_country_region.csv'
    print(f"Falling back to relative path: {cities_to_country_file_path}")

    cities_to_country_region = build_city_to_country_region_dict()

    # with open('info/cities_to_country_region.csv', mode='w', encoding='utf-8') as outfile:
    with open(csv_file_path, mode='w', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        for city, data in cities_to_country_region.items():
            writer.writerow([city, data['country'], data['region']])

            a=1

# if os.path.exists('info/cities_to_country_region.csv'):
#     cities_to_country_region = {}
#     with open('info/cities_to_country_region.csv', mode='r') as infile:
#         reader = csv.reader(infile)
#         for row in reader:
#             city, country, region = row
#             cities_to_country_region[city] = {'country': country, 'region': region}
# else:
#     cities_to_country_region = build_city_to_country_region_dict()
#
#     # with open('info/cities_to_country_region.csv', mode='w', encoding='utf-8') as outfile:
#     with open(csv_file_path, mode='w', encoding='utf-8') as outfile:
#         writer = csv.writer(outfile)
#         for city, data in cities_to_country_region.items():
#             writer.writerow([city, data['country'], data['region']])


def get_city_info(city, cities_values=sorted_cities_values, known_exceptions=known_exceptions, countries=countries, country_to_continent=country_to_continent, cities_to_country_region=cities_to_country_region):
    """
    Get the city name, country and region for a given city.

    Args:
        city (str): The name of the city to look up.
        cities_values (iterable): An iterable of city data: for example, a list of dictionaries, each with a 'geonameid' key.
        known_exceptions (dict): A dictionary of known city name exceptions.
        countries (dict): A dictionary of country data, keyed by country code.
        country_to_continent (dict): A dictionary mapping country codes to continents.
        cities_to_country_region (dict): A dictionary mapping city names to their corresponding country and region.

        Example of cities_values iterable:
            cities_dict = {
                '1234567': {
                    'geonameid': 1234567,
                    'name': 'New York City',
                    'alternatenames': ['NYC', 'New York', 'Nueva York'],
                    'latitude': 40.7127837,
                    'longitude': -74.0059413,
                    'feature_class': 'P',
                    'feature_code': 'PPLA',
                    'country_code': 'US',
                    'admin1_code': 'NY',
                    'admin2_code': '061',
                    'admin3_code': '',
                    'admin4_code': '',
                    'population': 8175133,
                    'elevation': 10,
                    'dem': 10,
                    'timezone': 'America/New_York',
                    'modification_date': '2019-09-05'
                },
                # ... other cities ...
            }
            cities = cities_dict.values()
        
        Example of countries dictionary:
            countries = {
                'US': {
                    'geonameid': 6252001,
                    'name': 'United States',
                    'iso': 'US',
                    'iso3': 'USA',
                    'isonumeric': '840',
                    'fips': 'US',
                    'continent': 'NA',
                    'capital': 'Washington',
                    'area_in_sq_km': 9629091.0,
                    'population': 310232863,
                    'phone': '1',
                    'tld': '.us',
                    'currency_code': 'USD',
                    'currency_name': 'Dollar',
                    'postal_code_format': '#####-####',
                    'postal_code_regex': '^\\d{5}(-\\d{4})?$',
                    'languages': 'en-US,es-US,haw,fr',
                    'neighbours': 'CA,MX,CU'
                },
                # ... other countries ...
            }
        
        Example of cities_to_country_region dictionary:
            cities_to_country_region = {
                'New York City': {'country': 'United States', 'region': 'North America'},
                'London': {'country': 'United Kingdom', 'region': 'Europe'},
                'Paris': {'country': 'France', 'region': 'Europe'},
                # ...
            }
    
    Returns:
        tuple: A tuple containing the city name, country and region for the given city.
            Returns ('Remote', 'Worldwide', 'Worldwide') for 'remote' or 'work at home'.

    Raises:
        Exception: If the city is not found in any of the data sources.

    Notes:
        This function first checks for known exceptions, then looks up the city in the
        cities_to_country_region dictionary. If not found, it searches through alternate
        names in the cities_values data. As a last resort, it uses an LLM to guess the country
        and region. Results are cached for future use.

    Example:
        >>> get_city_info('New York City')
        ('New York City', 'United States', 'North America')
    """
    # Check if the city is a known exception
    city_name = known_exceptions.get(city, city)

    # Use the unaccented version of the city name to get the city data
    country_region_data = cities_to_country_region.get(unidecode(city_name))
    if not country_region_data:
        # Handle remote or work at home cases
        if city_name.lower() in ['remote', 'work at home']:
            return 'Remote', 'Worldwide', 'Worldwide'
        
        # Search through alternate names in cities data
        for city_geonames_data in cities_values:
            if city_name in city_geonames_data['alternatenames']:
                country_code = city_geonames_data['countrycode']
                country_region_data = {
                    'country': countries[country_code]['name'],
                    'region': country_to_continent[country_code]
                }
                unidecode_real_city_name = unidecode(city_geonames_data['name'])
                unidecode_real_city_name = known_exceptions.get(unidecode_real_city_name, unidecode_real_city_name)
                
                # Update known exceptions and cities_to_country_region only if city_name is not empty and not already in the dictionaries
                if city_name:
                    if city_name not in known_exceptions:
                        known_exceptions[city_name] = unidecode_real_city_name
                        with open('info/cities_known_exceptions.csv', 'a', newline='', encoding='utf-8') as f:
                            csv.writer(f).writerow([city_name, unidecode_real_city_name])
                    if unidecode_real_city_name not in cities_to_country_region:
                        cities_to_country_region[unidecode_real_city_name] = {"country": country_region_data['country'], "region": country_region_data['region']}
                        with open('info/cities_to_country_region.csv', 'a', newline='', encoding='utf-8') as f:
                            csv.writer(f).writerow([unidecode_real_city_name, country_region_data['country'], country_region_data['region']])
                else:
                    raise Exception("⚠️ City name is empty!")
                
                return unidecode_real_city_name, country_region_data['country'], country_region_data['region']
        
        # If still not found, use LLM to guess
        standardized_city, country, region = guess_standardized_city_country_and_region(city_name)
        if not standardized_city:
            raise Exception(f"❌ City not found by the LLM: {city_name}")
        if not country:
            raise Exception(f"❌ Country not found by the LLM, for city: {city_name}")
        if not region:
            raise Exception(f"❌ Region not found by the LLM, for city: {city_name}")
        standardized_city = standardized_city.strip().title()
        country = country.strip().title()
        region = region.strip().title()
        
        if standardized_city and country and region:
            standardized_city = known_exceptions.get(standardized_city, standardized_city)

            # Search through potential alternate names in cities data
            for city_geonames_data in cities_values:
                if standardized_city in city_geonames_data['alternatenames']:
                    standardized_city = unidecode(city_geonames_data['name'])
            
            # Update known exceptions if not already present
            if city_name not in known_exceptions:
                known_exceptions[city_name] = standardized_city
                with open('info/cities_known_exceptions.csv', 'a', newline='', encoding='utf-8') as f:
                    csv.writer(f).writerow([city_name, standardized_city])
            
            # Update cities_to_country_region if not already present
            if unidecode(standardized_city) not in cities_to_country_region:
                cities_to_country_region[unidecode(standardized_city)] = {"country": country, "region": region}
                with open('info/cities_to_country_region.csv', 'a', newline='', encoding='utf-8') as f:
                    csv.writer(f).writerow([unidecode(standardized_city), country, region])
            
            return standardized_city, country, region
        
        # If all attempts fail, raise an exception
        raise Exception(f'❌ City not found: {city_name}')

    return unidecode(city_name), country_region_data['country'], country_region_data['region']


# cities_examples = ["Abu Dhabi", "Amsterdam", "Athens", "Atlanta", "Austin", "Bangkok", "Beijing", "Berlin", "Bogota", "Boston", "Brussels", "Bucharest", "Buenos Aires", "Cairo", "Calgary", "Casablanca", "Charlotte", "Chicago", "Cleveland", "Cologne", "Colombo", "Columbus", "Dallas", "Denver", "Detroit", "Doha", "Dusseldorf", "Frankfurt", "Geneva", "Gothenburg", "Gurugram", "Hamburg", "Hanoi", "Helsinki", "Hong Kong SAR", "Houston", "Tokyo", "Istanbul", "Jakarta", "Johannesburg", "Kuala Lumpur", "Kuwait", "Lagos", "Lima", "London", "Luanda", "Luxembourg", "Manama", "Manila", "Panama City", "Santiago", "Sao Paulo", "Medellin", "Milan", "Munich", "Rio de Janeiro", "Shanghai", "Shenzhen", "Singapore City", "Stockholm", "Stuttgart", "Taipei", "Vienna", "Zurich", "Mexico City", "Miami", "Minneapolis", "Montevideo", "Montreal", "Nairobi", "New Jersey", "New York City", "Lisbon", "Lyon", "Madrid", "Rome", "Seoul", "Osaka", "Oslo", "Philadelphia", "Pittsburgh", "Prague", "Paris", "Quito", "Riyadh", "San Francisco", "Santo Domingo", "Seattle", "Silicon Valley", "Los Angeles", "St. Louis", "Stamford", "Tel Aviv", "Toronto", "Vancouver", "Washington DC"]


# for city in cities_examples:
#     city_info = get_city_info(city)
#     print(f'{city}: {city_info}')


# # Print cities named "Athens"
# print([c for c in cities.values() if c['name'] == 'Athens'])

def get_locations(locations_list, job_title_and_description=None, known_exceptions=known_exceptions):
    """
    From a list of city names and/or a job title and description, returns a list of triples (city, country, region).
    This function turns a list like ["London", "Silicon Valley (San Francisco, Mountain View, Palo Alto)", "Paris/Lyon/Marseille", "Madrid"] into [("London", "United Kingdom", "Europe"), ("San Francisco", "United States", "North America"), ("Mountain View", "United States", "North America"), ("Palo Alto", "United States", "North America"), ("Paris", "France", "Europe"), ("Lyon", "France", "Europe"), ("Marseille", "France", "Europe"), ("Madrid", "Spain", "Europe")].
    It also handles remote work and infers the location from the job title and description if the locations_list is empty.
    
    Args:
        locations_list (list): A list of city names, which can be either a single city or multiple cities separated by commas or with forward slashes. For example: ["London", "Silicon Valley (San Francisco, Mountain View, Palo Alto)", "Paris/Lyon/Marseille", "Madrid"].
        job_title_and_description (tuple, optional): A tuple containing the job title and description, used to infer the location if locations_list is empty.
        known_exceptions (dict, optional): A dictionary of known exceptions for city names.

    Returns:
        list: A list of tuples, where each tuple contains the city name, country, and region.
              Note: If a city name is found in the known_exceptions dictionary, its standardized name will be used for processing and in the output.

    Example:
        >>> locations_list = ["San Francisco", "New York (Manhattan/Brooklyn)", "Remote", "NY"]
        >>> job_title_and_description = ("Software Engineer", "Work from home or in our offices in San Francisco or New York.")
        >>> get_locations(locations_list, job_title_and_description, known_exceptions)
        # Output: [('San Francisco', 'United States', 'North America'), ('Manhattan', 'United States', 'North America'), ('Brooklyn', 'United States', 'North America'), ('Remote', 'Worldwide', 'Worldwide'), ('New York City', 'United States', 'North America')]

    Note:
        If 'NY' is in known_exceptions with the value 'New York City', the function will use 'New York City' for processing and in the output.
    """
    # Process the list to handle strings containing multiple cities
    final_location_list = []
    for location in locations_list:
        if location and isinstance(location, str) and location.strip():
            final_location_list.append(location.strip())
        elif isinstance(location, list):
            final_location_list.extend(
                [
                    city
                    for city in location
                    if city and isinstance(city, str) and city.strip()
                ]
            )

    # If final_location_list is empty, try to get the location from the job title and description
    if not final_location_list and job_title_and_description:
        job_title, job_description = job_title_and_description
        final_location_list = infer_location_from_description(
            job_title, job_description
        )

    # Get the country and region for each city
    city_country_region_list = []
    for city in final_location_list:
        extracted_cities = extract_cities_parens(city)
        for extracted_city in extracted_cities:
            standardized_city, country, region = get_city_info(extracted_city)
            city_country_region_list.append((standardized_city, country, region))

    # Remove duplicates while ignoring accents
    final_location_list = list(
        set(
            (unidecode(city), unidecode(country), unidecode(region))
            for city, country, region in city_country_region_list
        )
    )

    return final_location_list


def infer_location_from_description(job_title, job_description):
    """
    Infer the location (city or cities) of the job from the job title and description.

    Args:
        job_title (str): The title of the job.
        job_description (str): The description of the job.

    Returns:
        list: A list of inferred locations (cities) where the job is offered.
              If the job allows for remote work without specifying a location,
              'Remote' will be included in the list, which will be considered worldwide.
              If remote work is mentioned with a specific country indication,
              it will be included as 'Remote, [Country]' (e.g., 'Remote, UK' or 'Remote, Canada').

    Example:
        >>> job_title = "Software Engineer"
        >>> job_description = "Work from home or in our offices in San Francisco or New York."
        >>> infer_location_from_description(job_title, job_description)
        ['Remote', 'San Francisco', 'New York']
    """
    # Prepare a prompt for the LLM
    system_prompt = "Given the job title and description, try to infer the location (i.e. city or cities) of the job. If the job allows for remote work without specifying a location, please include 'Remote' in the list of cities, which will be considered worldwide. If remote work is mentioned with a specific country indication, include it as 'Remote, [Country]' (e.g., 'Remote, UK' or 'Remote, Canada')."

    human_prompt = f'''
# Job Title
{job_title}


# Job Description
{job_description}

Please infer the location (city or cities) of the job.
'''

    location_tool = {
        "type": "function",
        "function": {
            'name': 'extract_locations',
            'description': 'Extract the locations (list of cities where the job is offered) from a job posting.',
            'parameters': {
                'type': 'object',
                'properties': {
                    'inferred_locations': {
                        'type': 'array',
                        'items': {
                            'type': 'string'
                        },
                        'description': 'Locations (list of cities where the job is offered) inferred from the job posting.'
                    }
                }
            }
        }
    }

    # Use the LLM to predict the location
    response = chat_completion_request(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": human_prompt}
        ],
        tools=[location_tool],
        tool_choice={"type": "function", "function": {"name": "extract_locations"}}
    )

    result_llm = execute_function_call(response.choices[0].message, function_name="extract_locations")

    result_list = [city if city and city != 'null' and city != 'None' else None for city in result_llm['inferred_locations']]

    return result_list


# # Test the function infer_location_from_description via get_locations

# problematic_job = {"url": "https://www.metacareers.com/jobs/209207782207703/", "title": "Research Scientist Intern, Human Computer Interaction, Toronto | Chercheur stagiaire dans le domaine de l'interaction homme-machine, Toronto", "location": [], "description": "Meta's mission is to give people the power to build community and bring the world closer together. Through our family of apps and services, we empower billions to share what matters most to them and strengthen connections. At Meta, our teams are dedicated builders, constantly iterating and solving problems to help individuals worldwide build communities and connect meaningfully. Join us in this mission as a Research Intern at Reality Labs, where we focus on inventing user interface technologies for augmented-reality experiences. You'll contribute to developing novel interactions and interfaces, and evaluating their effectiveness in terms of learnability, expertise development, and human factor considerations like comfort and fatigue.\n\nWe are looking for PhD candidates to work alongside expert researchers, designers, and software engineers. "}


# print(get_locations(problematic_job['location'], (problematic_job['title'], problematic_job['description'])))

def print_most_common_city_name_country_region(city_name):
    from geonamescache import GeonamesCache
    from unidecode import unidecode
    from termcolor import colored

    gc = GeonamesCache()
    countries = gc.get_countries()
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
    cities = gc.get_cities()
    sorted_cities = sorted(cities.values(), key=lambda x: x['population'], reverse=True)
    # Search through potential alternate names in cities data
    for city_geonames_data in sorted_cities:
        if city_name in city_geonames_data['alternatenames']:
            country_code = city_geonames_data['countrycode']
            country = countries[country_code]['name']
            region = country_to_continent[country_code]
            unidecode_real_city_name = unidecode(city_geonames_data['name'])
            print(colored(f"Most common city name/country/region: {unidecode_real_city_name}, {country}, {region}\n", "green"))
            print(colored(f"Alternative city names: {city_geonames_data['alternatenames']}", "yellow"))
            return
    print(colored(f"❌ City not found: {city_name}", "red"))