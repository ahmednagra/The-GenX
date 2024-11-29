from bs4 import BeautifulSoup
import hashlib
from itemadapter import ItemAdapter
from tenacity import retry, stop_after_attempt, wait_random_exponential
from termcolor import colored
import json
from openai import OpenAI
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

GPT_MODEL = "gpt-4o-mini"
client = OpenAI()


def get_titles_and_urls(html_links, base_url="https://www.metacareers.com"):
    title_url_pairs = []

    for link in html_links:
        soup = BeautifulSoup(link, 'html.parser')
        a_tag = soup.find('a')

        url = base_url + a_tag.get('href')
        title = a_tag.get_text().strip()

        title_url_pairs.append((title, url))

    return title_url_pairs


def get_id_unique(spider_name, job, title=None, id=None):
    # Get the title and url from the job
    title = job['title'] if title is None else title
    if id is None:  
        if spider_name == "mckinsey":
            url = job['friendlyURL']
        elif spider_name == "bcg":
            url = job['apply_url']
        elif spider_name == "bain":
            url = job['url']
        elif spider_name == "google":
            url = job['url']
        elif spider_name == "kearney":
            url = job['url']
        elif spider_name == "netflix":
            url = job['id']
        elif spider_name == "airbnb":
            url = job['url']
        elif spider_name == "lek":
            url = job['end_of_url']
        elif spider_name == 'occstrategy':
            url = job['url']
        else:
            url = job['id']
        id = url

    identifier_str = title + id

    # Compute and return the SHA-256 hash
    return hashlib.sha256(identifier_str.encode()).hexdigest()

def parse_answer(answer, key_names, keys, separator="||"):
    # Split the answers given by the LLM into chunks separated by `separator`
    chunks = answer.split(separator)

    data = {}

    for key_name, key, chunk in list(zip(key_names, keys, chunks)):
        # remove leading/trailing whitespaces, split by lines, and ignore the first line
        values = chunk.strip().split("\n")
        values[0] = values[0].replace(f"{key_name}:", "")

        # removing trailing line returns and empty elements from values list
        values = [value.strip() for value in values if value.strip()]
        
        if len(values) == 1:
            data[key] = values[0] if values[0] != 'None' else None
        elif values:
            data[key] = [value for value in values if value != 'None']
        else:
            data[key] = None

    return data


@retry(wait=wait_random_exponential(multiplier=1, max=40), stop=stop_after_attempt(5))
def chat_completion_request(messages, tools=None, tool_choice=None, model=GPT_MODEL, temperature=0):
    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=tools,
            tool_choice=tool_choice,
            temperature=temperature
        )
        return response
    except Exception as e:
        print("🚫 Unable to generate ChatCompletion response")
        print(f"Exception: {e}")
        return e

def pretty_print_conversation(messages):
    role_to_color = {
        "system": "red",
        "user": "green",
        "assistant": "blue",
        "function": "magenta",
    }
    
    for message in messages:
        if message["role"] == "system":
            print(colored(f"system: {message['content']}\n", role_to_color[message["role"]]))
        elif message["role"] == "user":
            print(colored(f"user: {message['content']}\n", role_to_color[message["role"]]))
        elif message["role"] == "assistant" and message.get("function_call"):
            print(colored(f"assistant: {message['function_call']}\n", role_to_color[message["role"]]))
        elif message["role"] == "assistant" and not message.get("function_call"):
            print(colored(f"assistant: {message['content']}\n", role_to_color[message["role"]]))
        elif message["role"] == "function":
            print(colored(f"function ({message['name']}): {message['content']}\n", role_to_color[message["role"]]))

# Previous Function
# def execute_function_call(message, function_name="extract_job_info"):
#     if message.tool_calls[0].function.name == function_name:
#         return json.loads(message.tool_calls[0].function.arguments)
#     else:
#         raise ValueError(f"🚨 INFER JOB SCRIPT: Unknown function call: {message.tool_calls[0].function.name} 🚨")

# Modified Function with IImproved Error handling and Error Message
def execute_function_call(message, function_name="extract_job_info"):
    try:
        # Ensure that there is at least one tool call
        if not message.tool_calls or len(message.tool_calls) == 0:
            return {
                "error": "NoToolCalls",
                "message": "🚨 INFER JOB SCRIPT: No tool calls present in the message 🚨"
            }

        # Check if the tool call matches the expected function name
        if message.tool_calls[0].function.name == function_name:
            # Try to parse the arguments
            return json.loads(message.tool_calls[0].function.arguments)
        else:
            return {
                "error": "UnknownFunction",
                "message": f"🚨 INFER JOB SCRIPT: Unknown function call: {message.tool_calls[0].function.name} 🚨"
            }

    except (json.JSONDecodeError, AttributeError) as e:
        # Handle JSON parsing issues or missing attributes in the message
        return {
            "error": "ParsingError",
            "message": f"🚨 INFER JOB SCRIPT: Error parsing function call arguments - {str(e)} 🚨"
        }

    except Exception as e:
        # Catch any other unforeseen errors
        return {
            "error": "UnexpectedError",
            "message": f"🚨 INFER JOB SCRIPT: Unexpected error - {str(e)} 🚨"
        }