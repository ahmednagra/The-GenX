from langchain.chains import LLMChain
from langchain.chat_models import ChatOpenAI, ChatAnthropic
from dotenv import load_dotenv, find_dotenv
from langchain.prompts import (
    ChatPromptTemplate,
    PromptTemplate,
    SystemMessagePromptTemplate,
    AIMessagePromptTemplate,
    HumanMessagePromptTemplate,
)

from langchain.tools.python.tool import PythonREPLTool
from langchain.agents.mrkl.base import ZeroShotAgent
from langchain.agents.agent import AgentExecutor
from langchain.chains.openai_functions import create_structured_output_chain
import json

# Load environment variables
load_dotenv(find_dotenv()) 

# Create models
chatgpt = ChatOpenAI(model_name="gpt-3.5-turbo-0125", temperature=0.1)  
gpt4 = ChatOpenAI(model_name="gpt-4", temperature=0.1)
# claude = ChatAnthropic(model="claude-instant-1")

llm = chatgpt

industries = ['consulting', 'finance', 'technology', 'other']
seniorities = ['intern', 'analyst', 'associate', 'manager']

# tools = [PythonREPLTool()]

# PYTHON_AGENT_PREFIX = 'You have access to a python REPL, which you can use to execute python code.\nIf you get an error, debug your code and try again.\nOnly use the output of your code to answer the question. \nYou might know the answer without running any code, but you should still run the code to get the answer.\nIf it does not seem like you can write code to answer the question, just return "I don\'t know" as the answer.\n This is the tool you have access to:\n'

# # From https://github.com/hwchase17/langchain/blob/master/langchain/agents/mrkl/prompt.py :
# FORMAT_INSTRUCTIONS = """Use the following format:

# Question: the input question you must answer
# Thought: you should always think about what to do
# Action: the action to take, should be one of [{tool_names}]
# Action Input: the input to the action
# Observation: the result of the action
# ... (this Thought/Action/Action Input/Observation can repeat N times)
# Thought: I now know the final answer
# Final Answer: the final answer to the original input question"""
# SUFFIX = """Begin!

# Question: {input}
# Thought:{agent_scratchpad}"""


def get_prompt(industries=industries, seniorities=seniorities): #, agent_prefix=PYTHON_AGENT_PREFIX, tools=tools):
    # # https://langchain-fanyi.readthedocs.io/en/latest/_modules/langchain/agents/mrkl/base.html#ZeroShotAgent
    # tool_strings = "\n".join([f"{tool.name}: {tool.description}" for tool in tools])
    # tool_names = ", ".join([tool.name for tool in tools])
    # format_instructions = FORMAT_INSTRUCTIONS.format(tool_names=tool_names)
    # template = "\n\n".join([agent_prefix, tool_strings, format_instructions])

    system_template=f'''# General instructions
You are an agent that extracts information from job postings. For each of the following categories, extract the corresponding information from the job posting:

1. Salary: Extract the salary.
2. Benefits: Extract the benefits. Write each benefit on a new line.
3. Requirements: Extract the requirements. Write each requirement on a new line.
4. Responsibilities: Extract the responsibilities. Write each responsibility on a new line.
5. Industry: Extract the industry, and make sure its value is one of the following: {industries}.
6. Seniority: Extract the seniority, and make sure its value is one of the following: {seniorities}, according to the guidelines below.

If you cannot extract any of the above, write "None" for that category.

## Seniority guidelines

Generally speaking, and only if no other inference can be made from the below extended industry guidelines, or where it helps complementing the below industry guidelines, our seniority scale could be inferred using the number of years required in the job description as follows:

- Intern: 0 years of full-time experience or still studying (student status) towards an undergraduate or postgraduate degree (bachelor or BSc or BA or LLB or BCL or master or master of arts or master of science or MA or MSc or master of business administration or MBA or master of research or MRes or MPhil or doctorate or PhD or DPhil)
- Analyst: 0-2 years of full-time professional experience
- Associate: 2-5 years of professional experience
- Manager: 5+ years of professional experience

### Consulting

All consulting jobs will be standardized against McKinsey job titles, with an equivalence of seniority to be matched for each position at each consulting firm as follows.

Titles falling outside of the below terms, such as data engineer, software engineer, data scientist, designer, product specialist, product manager, product owner or scrum master should be referenced as belonging to the technology industry and inferred following the technology guidelines.

Below is an outline of our terminology and the associated job titles at McKinsey.

McKinsey:
- Intern: Intern or Spring Intern or Summer Intern
- Analyst: Business Analyst or Fellow or Senior Business Analyst
- Associate: Associate or Specialist or Consultant or Junior Associate or Specialist
- Manager: Engagement Manager or Expert or Associate Partner or Expert Engagement Manager or Director

Equivalences at other consultancies:

BCG:
- Intern: Intern or Spring Intern or Summer Intern
- Analyst: Associate Consultant
- Associate: Consultant
- Manager: Team Leader or Project Leader or Principal or Partner or Managing Director

Bain:
- Intern: Intern or Spring Intern or Summer Intern
- Analyst: Associate Consultant or Senior Associate Consultant
- Associate: Consultant
- Manager: Case Leader or Case Team Leader or Manager or Principal or Partner

### Finance

All finance jobs will be standardized against Goldman Sachs job titles, with an equivalence of seniority to be matched for each position at each bank as follows.

Titles falling outside of the below terms, such as data engineer, software engineer, data scientist, designer, product specialist, product manager, product owner or scrum master should be referenced as belonging to the technology industry and inferred following the technology guidelines.

Below is an outline of our terminology and the associated job titles at Goldman Sachs.

Goldman Sachs:
- Intern: Intern or Spring Intern or Summer Intern
- Analyst: Analyst
- Associate: Associate
- Manager: Vice President or Executive Director or Managing Director or Partner Managing Director

Equivalences at other banks:

JPMorgan:
- Intern: Intern or Spring Intern or Summer Intern
- Analyst: Analyst
- Associate: Associate
- Manager: Vice President or Executive Director or Managing Director

Morgan Stanley:
- Intern: Intern or Spring Intern or Summer Intern
- Analyst: Analyst
- Associate: Associate
- Manager: Vice President or Executive Director or Managing Director

### Technology

All technology jobs will be standardized against the below job titles, with an equivalence of seniority to be matched for each position at each technology employer as follows.

Below is an outline of our terminology and the associated job titles for technology roles.

- Intern: Intern or Spring Intern or Summer Intern
- Analyst: Analyst or Manager or Engineer or Developer or Scientist or Designer
- Associate: Associate or Manager or Owner or Engineer or Developer or Scientist or Designer
- Manager: Lead or Team Lead or Master or Director or Vice President or Senior Vice President or Chief

To distinguish between overlapping analyst or associate job titles in the technology industry, an inference should be made using the number of years of experience required in the job description, where all jobs requiring more than 2 years of experience should be considered to be associate level.
'''

    human_template="""# Job posting

For the following job posting, please return the details in the desired format:

{input}
"""

# Please return the details in the following format:

# Salary:
# <Salary>
# ||
# Benefits:
# <Benefit 1>
# <Benefit 2>
# ... 
# ||
# Requirements:
# <Requirement 1>
# <Requirement 2>
# ...
# ||
# Responsibilities:
# <Responsibility 1>
# <Responsibility 2>
# ...
# ||
# Industry:
# <Industry>
# ||
# Seniority:
# <Seniority>"""

    
    # assistant_template="""Thought:{agent_scratchpad}"""
    
    # system_message_prompt = SystemMessagePromptTemplate.from_template(system_template) # doesn't work with Claude
    human_message_prompt = HumanMessagePromptTemplate.from_template(system_template+human_template)
    # assistant_message_prompt = AIMessagePromptTemplate.from_template(assistant_template)

    chat_prompt = ChatPromptTemplate.from_messages([human_message_prompt]) # , assistant_message_prompt])

    return chat_prompt

def create_agent(llm, prompt, tools, verbose=False, **kwargs):
    # Inspired from: https://python.langchain.com/en/latest/_modules/langchain/agents/agent_toolkits/python/base.html#create_python_agent
    llm_chain = LLMChain(llm=llm, prompt=prompt)
    tool_names = [tool.name for tool in tools]
    agent = ZeroShotAgent(llm_chain=llm_chain, allowed_tools=tool_names, **kwargs)
    return AgentExecutor.from_agent_and_tools(
        agent=agent,
        tools=tools,
        verbose=verbose,
    )



keys = ['salary', 'benefits', 'requirements', 'responsibilities', 'industry', 'seniority']
key_names = ['Salary', 'Benefits', 'Requirements', 'Responsibilities', 'Industry', 'Seniority']

def parse_answer(answer, separator="||"):
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

def get_job_info(job_posting):
    # agent_executor = create_agent(llm=llm, prompt=get_prompt(), tools=tools)
    # result_dict = agent_executor.run(input=job_posting)
    # print(result_dict)
    prompt = get_prompt()

    # Setting the JSON schema for extracting information from a job posting
    langchain_json_schema = {
        'name': 'extract_job_info',
        'description': 'Extract relevant information from a job posting.',
        'type': 'object',
        'properties': {
            'salary': {
                'title': 'Salary',
                'description': 'Salary (or salary range) inferred from the job posting.'
            },
            'benefits': {
                'title': 'Benefits',
                'type': 'array',
                'items': {
                    'type': 'string'
                },
                'description': 'Benefits inferred from the job posting.'
            },
            'requirements': {
                'title': 'Requirements',
                'type': 'array',
                'items': {
                    'type': 'string'
                },
                'description': 'Requirements inferred from the job posting.'
            },
            'responsibilities': {
                'title': 'Responsibilities',
                'type': 'array',
                'items': {
                    'type': 'string'
                },
                'description': 'Responsibilities inferred from the job posting.'
            },
            'industry': {
                'title': 'Industry',
                'enum': industries,
                'description': 'Industry inferred from the job posting.'
            },
            'seniority': {
                'title': 'Seniority',
                'enum': seniorities,
                'description': 'Seniority inferred from the job posting.'
            }
        },
        # 'required': ['salary', 'benefits', 'requirements', 'responsibilities', 'industry', 'seniority']
    }

    chain = create_structured_output_chain(llm=llm, prompt=prompt, output_schema=langchain_json_schema)
    result_llm = chain.run(input=job_posting)
    # result_dict = parse_answer(result_llm)
    
    result_dict = {key: value if value != 'None' else None for key, value in result_llm.items()}

    return result_dict

# example_job_posting = """
# Company Description:  

# We are a fast-growing tech startup that provides cutting-edge software solutions for businesses across various industries. Our company has recently raised $10 million in funding from top-tier investors and we are poised for rapid growth. We have a dynamic and collaborative work culture that values innovation, creativity, and excellence.

# Job Overview:  

# We are seeking a talented and experienced Java Developer to join our development team. The successful candidate will be responsible for designing, developing, and maintaining software applications using Java technologies. They will work closely with cross-functional teams to deliver high-quality software solutions that meet customer requirements.  

# Key Responsibilities:

# Design, develop, and maintain Java-based software applications  
# Write clean, efficient, and well-documented code  
# Collaborate with cross-functional teams to understand requirements and deliver high-quality software solutions  
# Participate in code reviews and contribute to the development of best practices  
# Troubleshoot and debug software issues as needed  
# Stay up-to-date with emerging trends and technologies in Java development  

# Requirements:  

# Bachelor's degree in Computer Science or a related field  
# 3+ years of experience in Java development  
# Experience with Spring Framework, Hibernate, and SQL  
# Strong understanding of object-oriented programming principles  
# Familiarity with agile software development methodologies  
# Excellent problem-solving and analytical skills  
# Strong communication and teamwork skills

# """

# print(get_job_info(example_job_posting))
