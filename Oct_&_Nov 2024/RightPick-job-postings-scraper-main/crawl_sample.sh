#!/bin/bash

# This script is used to crawl job postings from various company websites using Scrapy spiders.
# The script accepts three optional arguments:
# 1. max_jobs: The maximum number of jobs to crawl. If not provided, the script will ask for it.
# 2. data_directory: The directory where the crawled data will be saved. Defaults to './data' if not provided.
# 3. keep_data_directory: A boolean value to indicate whether to keep the existing data directory or not. Defaults to false if not provided.

# Usage:
# ./crawl_sample.sh [max_jobs] [data_directory] [keep_data_directory]

# Examples:
# ./crawl_sample.sh 10 /path/to/data false
# This will crawl a maximum of 10 jobs, will erase the content of the data directory and will save the data in /path/to/data.

# ./crawl_sample.sh
# This will ask for the maximum number of jobs to crawl, will remove the data directory and will save the data in ./data.

# Get max_jobs from command line argument or ask user
MAX_JOBS=${1}
if [ -z "$MAX_JOBS" ]
then
  read -p "Enter the maximum number of jobs to crawl: " MAX_JOBS
fi

# Get data_directory from command line argument or use default
DATA_DIR=${2:-./data}

# Get keep_data_directory from command line argument or use default (true)
KEEP_DATA_DIRECTORY=${3:-true}

# Remove data directory if keep_data_directory is not true
if [ "$KEEP_DATA_DIRECTORY" != true ]
then
  rm -rf $DATA_DIR/*
  echo "🧹 Data directory cleaned."
fi

# Define the spiders to be used
SPIDERS=("bain" "bcg" "goldmansachs" "mckinsey" "meta")

# Crawl the jobs for each spider
for index in "${!SPIDERS[@]}"
do
  SPIDER=${SPIDERS[$index]}
  scrapy crawl $SPIDER -a max_jobs=$MAX_JOBS # -o $DATA_DIR/${SPIDER}_output.json
  echo "🕷️ Crawling completed for $SPIDER. Results are in ${DATA_DIR}/${SPIDER}_jobs.json."

  # Print progress bar
  progress=$(($index + 1))
  printf 'Progress: '
  printf '▓%.0s' $(seq 1 $progress)
  if [ $progress -lt ${#SPIDERS[@]} ]
  then
    printf '░%.0s' $(seq $(($progress + 1)) ${#SPIDERS[@]})
  fi
  printf '\n'
done

# Post-process global_jobs.json to make it valid JSON using Python script
python jobsupdate/fix_json.py $DATA_DIR/global_jobs.json
echo "🔧 global_jobs.json has been fixed."