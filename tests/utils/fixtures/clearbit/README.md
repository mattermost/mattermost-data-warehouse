# Test files for clearbit integration

This directory and its subdirectories contain data files that are required for running clearbit related tests. 

Here's a short description of the files:

- **enrichments/*.json** - mock responses from clearbit API. Based on [examples from API docs](https://dashboard.clearbit.com/docs#enrichment-api).
- **expectations/*.json** - expected outputs to be loaded as dataframes. Stored as JSON files in order to make tests more readable. 
- **setup/cloud_clearbit_columns.csv** - contains the names of the columns in table `cloud_clearbit`.