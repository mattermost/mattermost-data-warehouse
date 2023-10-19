# Running DBT for old transform project

This document describes how to setup and execute common DBT tasks for both DBT projects:
- [snowflake-dbt](../transform/snowflake-dbt) - old DBT project, gradually deprecated
- [mattermost-analytics](../transform/mattermost-analytics) - new DBT project.

This document will present the alternatives for both projects.

> Note that transformations are gradually migrated to `mattermost-analytics` project. New data transformations must be 
> implemented in the `mattermost-analytics` project. 

## Prerequisites

- Docker & docker compose
- Snowflake access with proper roles (ask the team which role to use)

## Setup

Copy `.dbt.env.example` to `.dbt.env`. Edit the file and replace placeholder with appropriate values.

### DBT via docker

There are two `make` targets for starting a shell with `dbt` and its dependencies preinstalled. 

For `snowflake-dbt` project run:

```bash
make dbt-bash
```

For `mattermost-analytics` project run:

```bash
make dbt-mattermost-analytics
```

Both options allow using `dbt` directly from a bash shell.

To test that everything is working as expected, try generating dbt docs:

```bash
dbt docs generate -t prod
```

If the output looks similar to:
```
...

07:28:30  Found 289 models, 10 tests, 0 snapshots, 0 analyses, 407 macros, 0 operations, 15 seed files, 263 sources, 0 exposures, 0 metrics
07:28:30  
07:29:00  Concurrency: 8 threads (target='prod')
07:29:00  
07:32:04  Done.
07:32:04  Building catalog
07:44:19  Catalog written to /usr/app/target/catalog.json
```

then the setup is working.

### Overriding profile

> Profile overriding might be useful if you want to experiment with the project structure. Day-to-day operations 
> shouldn't require it.

All dbt commands that are part of the `Makefile` use by default profiles under [transform/snowflake-dbt/profile](transform/snowflake-dbt/profile).
This directory can be overriden by setting `DBT_PROFILE_PATH` environment variable:

```bash
DBT_PROFILE_PATH=/path/to/profile make dbt-bash
```

> Note that `DBT_PROFILE_PATH` can use either absolute or relative paths. For relative paths, the base is the [build](build) 
> directory of this project.

## Common DBT operations

The following examples describe common operations in the development lifecycle. For more detailed documentation, please
check [DBT's documentation](https://docs.getdbt.com/docs/introduction).

All commands require that they are executed in a properly configured dbt shell. Check the instructions above for details.

### Compile models

To compile the models from DBT to SQL statements, use the command.

```bash
dbt compile -t prod
```

This command doesn't run anything against the database. It should be useful whenever 

The resulting SQL files will be available under `target/compiled/mm_snowflake`.

It is also possible to compile specific models by using the [proper selectors](https://docs.getdbt.com/reference/node-selection/syntax#how-does-selection-work).
The following command will compile only models with tag "nightly":
```bash
dbt compile --select tag:nightly -t prod
```

### Run project against production

> Running against production from your local machine is not advised. Changes performed to production database won't be
> tracked. It also increases the risk of accidentally running code that hasn't been approved. However, there are cases 
> where manual execution is required (i.e. triggering full refresh). Please treat with caution.

To run the project against production, use the `dbt run` command. The following example will run all models tagged with
`nightly` tag.

```bash
dbt run --select tag:nightly -t prod
```

### Building docs and serving them locally

At the home of the repo run:

For `snowflake-dbt` project run:

```bash
make dbt-docs
```

This command will generate the docs and serve them at [http://localhost:8081](http://localhost:8081).

For `mattermost-analytics` project, connect to the shell (i.e. via `make dbt-mattermost-analytics`) and run
the following command:

```bash
dbt docs generate -t prod && dbt docs serve --port 8081
```

> Docs are also available in Cloud DBT as artifacts of each build.