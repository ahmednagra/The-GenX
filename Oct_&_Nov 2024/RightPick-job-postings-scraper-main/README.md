# RightPick Job Postings Web Scraper

> **TL;DR:** Build Scrapy spiders launched by a cron job with ScrapeOps monitoring, to populate a Firestore DB.

## Overview

RightPick is expanding its job posting scrapers to incorporate additional employers' open roles. We're implementing economical and optimized Scrapy spider for a range of prestigious companies in consulting, finance, and technology sectors.

## Guidelines

>[!IMPORTANT]
> Please ALWAYS refer to existing spiders’ code in which everything is correctly implemented (McKinsey, BCG and Meta are some examples) to see and mimic exactly how the following attribute is updated. Following the existing spiders’ full logic is a key part of our delivery requirements.
> For example, the attribute `remove_obsolete_jobs` is not handled correctly in some spiders (Apple, Amazon, Bank of America, Microsoft, Morgan Stanley, etc): this is a crucial attribute because it ensures that the jobs are kept updated, and obsolete jobs are removed in a timely manner (with our `RemoveOutdatedJobsPipeline` in `jobscraper/pipelines.py`).

## Codebase Insights

- Our spiders are designed to scrape job postings and related data for storage in a Firestore database. (The legacy code related to Webflow can be ignored.)
- We prioritise cost-efficient scraping methods
    - **steering clear of headless browsers** as much as possible (e.g., Puppeteer, Selenium, Playwright) and Scrapy Splash,
    - and using internal APIs as much as possible
    
    to minimise resource usage and avoid potential blocking.
    
- Key features include:
    - Handling of dynamic content and pagination
    - Removal of outdated jobs (`remove_obsolete_jobs` attribute)
    - Handling of the `max_jobs` argument for controlled scraping
    - Use of OpenAI's models for semantic data extraction and location resolution
- Developers should draw inspiration from our existing spiders (especially: Meta, McKinsey, and BCG), which cover a variety of cases including handling dynamic content and pagination. Special attention should be given to the deletion of outdated jobs pipeline and the `max_jobs` argument.
  - 📢 **Spider Naming Convention**:
    When adding a new spider, it's important to adhere to the following naming conventions to ensure consistency and clarity:

    1. **Spider Name:**
        - The spider's name should be a sluggified version of the company name. For example, for "OC&C Strategy Consultants," the spider name should be something like `occstrategy` (or `occstrategyconsultants`).
        - The spider file should also be named `[name].py` (e.g., `occstrategy.py` or `occstrategyconsultants.py`).
        - Avoid prefixes like "careers" in the name when they are not specific to the company (e.g., `careersoccstrategy` would *not* be a good name).

    2. **Spider Class Name:**
        - The spider class name should follow camel case formatting, with capital letters for acronyms, and should be appended with "Spider." For example, the class name for "OC&C Strategy Consultants" could be `OCCStrategySpider` (or `OCCStrategyConsultantsSpider`).

    3. **Company Title:**
        - The company title, which should be added to the `COMPANY_TITLES` dictionary of `jobscraper/spiders/__init__.py`, should be the official name of the company. In our example, it should be "OC&C Strategy Consultants."
        - Make sure that it’s one of the keys of the `employers_dict` dictionary of the mobile app: [RightPick App Employers Dictionary](https://github.com/RightPick-team/RightPick-app/blob/fcf758870fc570d26550220ed862a35eff62a5e3/constants/SearchOptions.tsx#L282).

    To recap, example, for the company "OC&C Strategy Consultants":

    - **Spider Name:** `occastrategy`
    - **Spider File:** `occastrategy.py`
    - **Spider Class Name:** `OCCStrategySpider`
    - **Company Title:** "OC&C Strategy Consultants"
- We resort to OpenAI's latest GPT models for semantic data extraction (with function calling features), and for resolving ambiguous location cases when the standard package `geonamescache` falls short. These functionalities have been implemented in a standalone module designed to work on its own, however, potential debugging of corner cases may require inspection (so familiarity with OpenAI's models would be highly beneficial).
- Across all spiders, we extract a set of common fields (refer to the `jobscraper/spiders/__init__.py` file):
    - `company` (a slug to identify the spider which should be a key in the dictionary `COMPANY_TITLES` of `jobscraper/spiders/__init__.py`, and it should also be equal to the spider name `spider.name`) and `company_title` (the displayed name of the company in the app, which should be a value in the dictionary `COMPANY_TITLES`).
    Note that these are added automatically via `JobPipeline` in `jobscraper/pipelines.py`.
    - a custom `id_unique` (to identify job postings uniquely irrespective of the employer, computed with a function `get_id_unique` from the spider name and the job URL, to check whether we’ve already seen the job in future scrapings),
    - `url`, `title`, `location` (a list of cities, later (in the pipeline `JobPipeline`) turned into a list of triples  `(city, country, region/continent)`), `description`, `salary`, `benefits`, `requirements`, `responsibilities`, `industry`, and `seniority`.
    - fields like `date_scraped`, `location_list_flattened`, `company_title` are added via `JobPipeline` in `jobscraper/pipelines.py`.

>[!WARNING]
>
> **Locations:**
>
> The `location` field in the spider should be a list of cities. For example (from the Spotify spider):
>
> ```python
> job_dict = {
>     "id": job.get("id"),
>     "title": job.get("text"),
>     # ...
>     "location": [city.get("location") for city in job.get("locations", [])],
> }
> ```
>
> This list is later, in the pipeline `JobPipeline`, converted into a list of triples (city, country, region/continent) using the `get_locations` function from the `dataextraction` module. For instance, a list like `job_dict['location'] = ["London", "Silicon Valley (San Francisco, Mountain View, Palo Alto)", "Paris/Lyon/Marseille", "Madrid"]` would be transformed, in the pipeline,  into:
>
> ```python
> [
>    ("London", "United Kingdom", "Europe"),
>    ("San Francisco", "United States", "North America"),
>    ("Mountain View", "United States", "North America"),
>    ("Palo Alto", "United States", "North America"),
>    ("Paris", "France", "Europe"),
>    ("Lyon", "France", "Europe"),
>    ("Marseille", "France", "Europe"),
>    ("Madrid", "Spain", "Europe")
> ]
> ```
>
> **Seniority, Industry, Responsibilities, Benefits, Salary, Requirements**:
>
> For these fields, we use our custom `get_job_info` function (`from dataextraction import get_job_info`) to automatically extract information from the job posting with an LLM. This function should be called with a string containing at least the job title and description, and can include additional information if available. Here's an example of how to use it:
>
> ```python
> job_posting_text = f"""Job title:\n {further_info['title']}
> Description:\n{further_info['description']}
> """
> job_info = get_job_info(job_posting_text)
> ```
>
> The `get_job_info` function returns a dictionary with the following schema:
>
> ```python
> {
>     "salary": str,  # Optional: Salary (or salary range) inferred from the job posting
>     "benefits": List[str],  # Optional: Benefits inferred from the job posting
>     "requirements": List[str],  # Optional: Requirements inferred from the job posting
>     "responsibilities": List[str],  # Optional: Responsibilities inferred from the job posting
>     "industry": str,  # Mandatory: Industry inferred from the job posting
>     "seniority": str,  # Mandatory: Seniority inferred from the job posting
> }
> ```
>
> Note that 'industry' and 'seniority' are mandatory fields, while the others are optional. The function uses AI to extract this information, so the quality of the output depends on the information provided in the job posting text.
            
> [!TIP]
> Additionally, we capture extra fields that may vary depending on the employer when they provide relevant insights into the job postings.
> Hired developer is expected to take the initiative to identify and extract these extra variable fields (depending on how valuable they are deemed to be), as part of the spider development process.

## Deliverables

For each employer:

- Optimized Scrapy spiders extracting job postings with details like title, location, description, and application link.
- Common fields extracted across all spiders: `company_title`, `company`, `title`, `industry`, `seniority`, `location`, `url`, `date_scraped`, `salary`, `benefits`, `requirements`, `responsibilities`, `description`, `id_unique`, and `location_list_flattened`.
    - Add passing checks for optional/extra fields (like `employment_type`, for example), but ensure that mandatory fields (`title`, `industry`, `seniority`, `location`, `url`, `salary`, `description`, `id_unique`) yield errors if not fetched properly.
    - Modify the `SPIDERS` dictionary accordingly in `jobscraper/spiders/__init__.py`. The extracted fields vary depending on the employer and may include items such as `job_id`, `apply_url`, `job_type`, `department`, and more.
    In the file `jobscraper/spiders/__init__.py`:
        ```python
        COMMON_FIELDS = ["company_title", "company", "title", "industry", "seniority", "location", "url", "date_scraped", "salary", "benefits", "requirements", "responsibilities", "description", "id_unique", "location_list_flattened"]
        
        SPIDERS = {
            'mckinsey': {'file_name': 'mckinsey.py', 'class_name': 'McKinseySpider', 'custom_fields':  ["job_id", "job_skill_group", "job_skill_code", "interest", "interest_category", "functions", "industries", "who_you_will_work_with", "what_you_will_do", "your_background", "post_to_linkedin", "job_apply_url"]},
        # [...]
        }
        ```
        
- Employer-specific fields when relevant.
- Checking for already seen jobs should be done **as early as possible** to avoid unnecessary processing. For example, the following:
    
    ```python
    job_dict = defaultdict(lambda: None, job_dict)
    id_unique = get_id_unique(self.name, job_dict)
    self.scraped_jobs_dict[id_unique] = job_dict["title"]
    self.seen_jobs_count += 1
    if id_unique in seen_jobs_set:
        self.logger.info(f'👀 Job "{job_dict["title"]}" already seen. Skipping...')
        continue
    self.fetched_count += 1
    job_dict["id_unique"] = id_unique
    ```

    should be placed as early as possible in the job processing loop to avoid unnecessary processing of already seen jobs.
- Efficient error handling and logging.
- Documentation detailing the spider's functionality and any specific considerations.
- Integration with the existing pipeline, including proper implementation of `remove_obsolete_jobs` and `max_jobs` handling.
- Pay special attention to pagination, to make sure no job listings are missed.
- Make sure that your spiders are properly tested and yield NO error.

## Additional Instructions

Please follow these guidelines when working on the spiders:

- Import internal modules using the format: `from jobscraper.spiders import close_spider` (for example) instead of `from RightPick.jobscraper.spiders import close_spider`
- Place new spiders directly in the `jobscraper/spiders` directory on the `main` branch
- When significantly overwriting an existing spider, rename the old spider (e.g., `bain_old.py`) and move it to the `jobscraper/spiders/old` folder.
- Double-check that logging is implemented to save logs in `data/logs/[spider_name]/` , by including, at the beginning of the spider:
    
    ```python
    log_dir = f"data/logs/{name}"
    os.makedirs(log_dir, exist_ok=True)
    custom_settings = {
        "LOG_FILE": f'data/logs/{name}/{name}_jobs_{datetime.now().strftime("%Y%m%d%H%M%S")}.log',
        "LOG_LEVEL": "DEBUG",
        "LOG_ENABLED": True,
    }
    ```
> [!TIP] 
> For POST requests, if you encounter 500 Internal Server Error with the ScrapeOps Proxy SDK, add `'sops_keep_headers': True` to the `meta` dictionary of `scrapy.Request` to resolve the issue. 
> Example:
>   
>```python
> class OliverwymanSpider(scrapy.Spider):
>    name = "oliverwyman"
>    # [...]
>    headers = { ... }
>    json_data = { ... }
>    fetched_count = 0
>    scraped_jobs_dict = dict()
>
>    def start_requests(self):
>        self.seen_jobs_set = get_seen_jobs(self.name)
>        self.max_jobs = int(getattr(self, "max_jobs", MAX_JOBS))
>        yield scrapy.Request(
>            "https://careers.marshmclennan.com/widgets",
>            headers=self.headers,
>            body=json.dumps(self.json_data),
>            method="POST",
>            meta={'sops_keep_headers': True},
>        )
>    # [...]
>```

- If JavaScript rendering is **required (and *only* if it’s absolutely necessary)**, use the ScrapeOps Proxy SDK's built-in JavaScript rendering capabilities. Add the following to the `meta` dictionary of your `scrapy.Request`:
    
    ```python
    meta={
        'sops_render_js': True,
        'wait_for': '.selector-to-wait-for'  # Replace with appropriate selector
    }
    ```
    
    This approach uses ScrapeOps to render JavaScript without the need for a separate Splash server. The `wait_for` parameter ensures the specified element is loaded before scraping begins.
    
    For more detailed information on using ScrapeOps for JavaScript rendering, refer to the ScrapeOps documentation: [JavaScript Rendering with ScrapeOps](https://scrapeops.io/docs/web-scraping-proxy-api-aggregator/advanced-functionality/javascript-scenario/)


# Submitting a Spider

To submit a new spider (i.e., to be in a state where you can confidently declare that the work for a spider is complete), follow these steps to ensure proper testing and deployment:

1. **Testing the Spider:**
    - Ensure `TESTING=True` is set in your `.env` file.
    - Run the spider twice:
        - First, with `FORCE_NO_SEEN_JOBS=True` to simulate fetching jobs for the first time:
            
            ```bash
            FORCE_NO_SEEN_JOBS=True scrapy crawl [spider_name]
            ```
            
        - Then, with `FORCE_ALREADY_SEEN_CACHE_COMPUTATION=True` to avoiding reusing the empty cache, and instead recomputing it:
            
            ```bash
            FORCE_ALREADY_SEEN_CACHE_COMPUTATION=True scrapy crawl [spider_name]
            ```
            
    - Check the log files in `data/logs/[spider_name]/[timestamp].log` for *both* runs.
    - Verify in the Scrapy stats (at the end of ***each*** log file) that we have:
        - `'finish_reason': 'finished'`
        - `'log_count/ERROR': 0`
        - `'log_count/CRITICAL': 0`
    - Ensure that during the second run, the message `👀 Job "[Job Title]" already seen. Skipping...` is displayed for every job.
2. **Upload Log Files:**
    - Both resulting log files must be uploaded to UpWork, for Benoit and Younesse to check.
3. **Deploying on ScrapeOps:**
    
    
    ### ScrapeOps Guidelines
    
    If you’re deploying on ScrapeOps, to ensure proper deployment and updating of the scraper code on the AWS instance, follow these steps:
    
    - Navigate to the ScrapeOps dashboard.
    - Go to “Servers & Deployment” > “AWS 03/24”.
    - Click on `RightPick-job-postings-scraper`.
        > [!TIP]
        > When deploying code, click on “Deploy Code” and ensure that the “Deploy Script” is set up correctly. Use the following commands:
        >
        > ```bash
        > sudo chown -R scrapeops:scrapeops /home/scrapeops/RightPick-job-postings-scraper
        > sudo chmod 777 /home/scrapeops/RightPick-job-postings-scraper/seen_jobs_cache.json
        > cd /home/scrapeops/RightPick-job-postings-scraper
        > git pull origin main
        > whoami
        > ```
        
    - After deployment, on the same page, check the log of the newly created entry in "Deployment/Install Script Logs” by clicking on “View Log” to verify there are no errors.
    
    **Note:** The repository in the AWS instance does not update automatically. Manual deployment is necessary to ensure the latest code is running. The new spider will now appear and should be scheduled, with Benoit and/or Younesse's approval.