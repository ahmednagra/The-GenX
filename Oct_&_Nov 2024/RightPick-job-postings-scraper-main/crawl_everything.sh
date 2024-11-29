#!/bin/bash

# This script is used to crawl job postings from various company websites using Scrapy spiders.

# Get data_directory from command line argument or use default
DATA_DIR=${1:-./data}

# Get keep_data_directory from command line argument or use default (true)
KEEP_DATA_DIRECTORY=${2:-true}

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
  scrapy crawl $SPIDER # -o $DATA_DIR/${SPIDER}_output.json
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