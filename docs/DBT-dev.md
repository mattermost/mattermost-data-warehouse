# DBT development on `mattermost-analytics` project

This document describes common development tasks on the new DBT project.

`mattermost-analytics` was kickstarted as a project that follows best-practices defined in [DBT's documentation](https://docs.getdbt.com/guides/best-practices).
The project follows a layered structure for the models:
- **Staging layer**: Basic cleaning of source data.
- **Intermediate layer**: Where reusable building blocks are introduced.
- **Marts layer**: Purpose-specific models, to be used in Looker or other downstream integrations and tools.

Model naming **MUST** follow the conventions defined in [DBT's documentation](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview).

Column naming conventions **MUST** follow the [internal guidelines](https://mattermost.atlassian.net/wiki/spaces/DATAENG/pages/2422603777/DWH+Naming+Conventions). 

## DBT packages

The project is using DBT packages to make development faster and to ensure code quality.

### DBT Utils ([git repo](https://github.com/dbt-labs/dbt-utils))

DBT utils offers macros for easier common day-to-day operations and tests.

### Codegen ([git repo](https://github.com/dbt-labs/dbt-codegen)) 

Codegen offers macros & command line tools for generating DBT code. It is useful when incubating new data sources or adding
documentation.

### DBT Project Evaluator [git repo](https://github.com/dbt-labs/dbt-project-evaluator)

DBT Project Evaluator checks the health of the project in terms of code quality. DBT project evaluator is running after 
every hourly or nightly build. The results are visible in a Looker dashboard.

## Development workflow

1. Create a new branch. The convention is `<JIRA ticket>-<short description>`. For example `MM-123-add-telemetry-source `.
2. Edit the project and implement any changes required (adding sources, adding and/or modifying models etc)
3. Commit the changes and push the branch to github.
4. Open a [Pull Request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request). 
5. As soon as the Pull Request (PR) is created, Cloud DBT will trigger a CI job that will run all new & modified models, as well as their downstream models. When the CI job finishes, all the models will be available in schema `dbt_cloud_pr_<account number>_<pr number>`.
6. Validate the additions or changes and share the PR for review.
7. As soon as there's enough approvers, the PR can be merged!

## Common day-to-day operations

This section describes common operations that might be required during DBT transformation development.

> Note that most of the operations described in this section can also be executed in DBT's Cloud IDE. Please follow 
> Codegen and Cloud DBT documentation. 

### Adding a new source

Codegen's [generate_source](https://github.com/dbt-labs/dbt-codegen#generate_source-source) macro can be used to 
generate the `yaml` definition of a source. Edit the following command to match the details of the source and run it
in a `dbt` shell (i.e. by starting `make dbt-mattermost-analytics`).

```bash
# Make sure to replace <schema_namee> with the name of the Snowflake schema containing the source data
# and <database_name> with the name of the database (usually RAW).
dbt run-operation generate_source --args '{"schema_name": "<schema_name>", "database_name": "<database_name>", "generate_columns": true}'
```

The output can be copy-pasted to file `models/staging/<source  name>/_<source_name>__sources.yml`.

Consider editing the file and adding the following information:
- Source `description`, providing information about the source system/service providing the data.
- `loader`, to indicate the tool used for loading the data to the data warehouse.
- `tags`: use one of `stage`, `rudderstack`, `segment`, `stitch` or a similar one to indicate how the data were loaded to the warehouse.
- Table `description`, providing information about the purpose of the table, its structure and any other useful information.
- Column `description`, providing information about the expectations on the data of each column.
- Introducing tests on data sources. 

Since data sources may come for multiple channels (events, analytics, telemetry or even other databases), it's a good 
idea to introduce tests. These tests may ensure data integrity, conventions, freshness and any assumption made for the
data. It can help catching inconsistencies early in the data pipelines. 

Basic tests can be added in the `_<source_name>__sources.yml` file. These type of checks can assure there's no missing 
values, uniqueness, proper format etc. More complex test cases can be implemented using [dbt_utils' generic tests](https://github.com/dbt-labs/dbt-utils#generic-tests)
or [singular tests](https://docs.getdbt.com/docs/build/tests#singular-tests).

> In case some of the tests are challenging to implement in the sources, consider them moving them to the staging layer.

### Adding a new staging model

Staging models are used for basic cleaning of the data. This might include:
- Renaming columns
- Type casting
- Extracting nested data (i.e. from nested JSON columns),
- Splitting columns with multiple values (i.e. version to major, minor & patch)
- Basic computations (i.e. cents to dollars)
- Categorizing data

A staging model is usually created from a source table. As long as the source and the table are already defined in the 
DBT project, it's possibly to generate a skeleton for a staging model by using Codegen's [generate_base_mode](https://github.com/dbt-labs/dbt-codegen#generate_base_model-source)
operation. The command should look similar to:

```bash
# Make sure to replace <source_name> with the name of the source containing the source table to generate the staging model
# for and and <table_name> with the name of the table.
dbt run-operation generate_base_model --args '{"source_name": "<source_name>", "table_name": "<table_name>", "leading_commas": true}'
```

The output can be copy-pasted to file `models/staging/<source  name>/stgs_<source_name>__<table name>.sql`. The file can
then be edited to perform basic cleaning.

Some extra actions that we've found useful are:

- Group the columns by their meaning. For example common telemetry event info may be moved as the first columns, followed by references (ids), values and finally metadata. This may improve readability of the SQL code a lot.
- Consider performing analysis on the values of each column. This way columns that i.e. always have the same value or are always null can be dropped.
- Introducing data source tests that require the basic transformations performed in the staging layer. 

### Documenting a model

It's also possible to generate he YAML for a model's documentation by using Codegen's [generate_model_yaml](https://github.com/dbt-labs/dbt-codegen#generate_model_yaml-source)
operation. The command looks similar to:

```bash
# Make sure to replace <model_name> with the actual model. 
dbt run-operation generate_model_yaml --args '{"model_names": ["<model_name>"]}'
```

The output can be copy-pasted to the related `*__models.yml` file and edited there.

> Note that the Codegen command will work for all models in any layer (staging, intermediate, mart), as long as the 
> table has already been created in the database. If the table is not there, then it won't be "discoverable" by DBT.
