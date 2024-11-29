import sys
sys.path.append('.')

from .utils import chat_completion_request, execute_function_call

industries = ['consulting', 'finance', 'technology']
seniorities = ['intern', 'analyst', 'associate', 'manager']


system_template=f'''# General instructions
You are an agent that extracts information from job postings. For each of the following categories, extract the corresponding information from the job posting:

1. Salary: Extract the salary.
2. Benefits: Extract the benefits. Write each benefit on a new line.
3. Requirements: Extract the requirements. Write each requirement on a new line.
4. Responsibilities: Extract the responsibilities. Write each responsibility on a new line.
5. Industry: Extract the industry, and make sure its value is one of the following: {industries}.
6. Seniority: Extract the seniority, and make sure its value is one of the following: {seniorities}, according to the guidelines below.

If you cannot extract any of the above, return "null" (or the empty list) for that category.

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

keys = ['salary', 'benefits', 'requirements', 'responsibilities', 'industry', 'seniority']
key_names = ['Salary', 'Benefits', 'Requirements', 'Responsibilities', 'Industry', 'Seniority']


def get_job_info(job_posting):
    try:
    # Setting the function calling schema for extracting information from a job posting
        tool = {
            "type": "function",
            "function": {
                "name": "extract_job_info",
                "description": "Extract relevant information from a job posting.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "salary": {
                            "type": "string",
                            "description": "Salary (or salary range) inferred from the job posting.",
                        },
                        "benefits": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Benefits inferred from the job posting.",
                        },
                        "requirements": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Requirements inferred from the job posting.",
                        },
                        "responsibilities": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Responsibilities inferred from the job posting.",
                        },
                        "industry": {
                            "type": "string",
                            "enum": industries,
                            "description": "Industry inferred from the job posting.",
                        },
                        "seniority": {
                            "type": "string",
                            "enum": seniorities,
                            "description": "Seniority inferred from the job posting.",
                        },
                    },
                    'required': ['industry', 'seniority']
                }
            }
        }

        # Add more specific prompts for each category
        human_template = f"""# Job posting
    
    For the following job posting, please return the details in the desired format:
    
    {job_posting}

    Please pay special attention to:
    1. Salary: Look for specific numbers or ranges mentioned.
    2. Benefits: Include both standard and unique perks offered.
    3. Requirements: Focus on both technical skills and soft skills required.
    4. Responsibilities: Highlight the main duties and tasks of the role.
    5. Industry: Determine the primary industry based on the company description and role.
    6. Seniority: Use both the title and required experience to determine the appropriate level.
    """

        response = chat_completion_request(
            messages=[
                {"role": "system", "content": system_template},
                {"role": "user", "content": human_template}
            ],
            tools=[tool],
            tool_choice={"type": "function", "function": {"name": "extract_job_info"}}
        )

        result_llm = execute_function_call(response.choices[0].message, function_name="extract_job_info")

        # Check if the result contains an error
        # if 'error' in result_llm:
        if result_llm.get('error', {}):

            # Return the error to be handled by the calling function, or return a custom error message
            return {
                "error": result_llm['error'],
                "message": f"Failed to extract job info due to: {result_llm['message']}"
            }

        # Clean up the result
        result_dict = {key: value if value and value != 'null' and value != 'None' else None for key, value in result_llm.items()}

        # # Post-processing step
        # result_dict = post_process_job_info(result_dict)

        return result_dict
    except Exception as e:
        # Return a dictionary indicating failure
        return {"error": str(e), "job_posting": job_posting}

# example_job_posting = """
# Company Description:

# We are a fast-growing tech startup that provides cutting-edge software solutions for businesses across various industries. Our company has recently raised $10 million in funding from top-tier investors, and we are poised for rapid growth. We have a dynamic and collaborative work culture that values innovation, creativity, and excellence.

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

# def post_process_job_info(job_info):
#     # Normalize salary
#     if job_info['salary']:
#         # Convert salary ranges to a standard format
#         # e.g., "$50,000 - $70,000 per year" to "50000-70000"
#     # Standardize benefits
#     if job_info['benefits']:
#         standard_benefits = ['health insurance', '401k', 'paid time off', 'remote work']
#         job_info['benefits'] = [benefit for benefit in job_info['benefits'] if any(std in benefit.lower() for std in standard_benefits)]

#     # Clean up requirements and responsibilities
#     for key in ['requirements', 'responsibilities']:
#         if job_info[key]:
#             job_info[key] = [item.strip() for item in job_info[key] if len(item.strip()) > 10]

#     return job_info
