# Test files for clearbit integration

This directory and its subdirectories contain data files that are required for running clearbit related tests.  

The top level directories split fixtures between cloud and onprem clearbit test fixtures. Here's a short description of
the files that may exist in any of these directories:

- **api/enrichments/*.json** - mock responses from clearbit enrichment API. Based on [examples from API docs](https://dashboard.clearbit.com/docs#enrichment-api).
- **api/reveal/*.json** - mock responses from clearbit reveal API. Based on [examples from API docs](https://dashboard.clearbit.com/docs#reveal-api).
- **cloud/expectations/*.json** - expected outputs to be loaded as dataframes for cloud clearbit. Stored as JSON files in order to make tests more readable.
- **onprem/expectations/*.json** - expected outputs to be loaded as dataframes for onprem clearbit. Stored as JSON files in order to make tests more readable.
- **cloud/setup/clearbit_columns.csv** - contains the names of the columns in table `cloud_clearbit`.
- **onprem/setup/clearbit_columns.csv** - contains the names of the columns in table `onprem_clearbit`.