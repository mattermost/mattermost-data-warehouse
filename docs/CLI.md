# Command-line tools

After the python code has been install (usually using Poetry), a list of command-line tools is also installed. This 
document describes these tools. 

> These tools are also installed in the `mattermost-data-warehouse` docker image.

## Rudderstack utilities

`rudder` is the entry level command for common Rudderstack tasks. Simply running `rudder` on your bash script will 
print a help message and a list of sub-commands.

```bash
$ rudder
Usage: rudder [OPTIONS] COMMAND [ARGS]...

  Rudderstack helpers. Offers a variety of subcommands for managing
  rudderstack tables from the command line.

Options:
  --help  Show this message and exit.

Commands:
  list  List Rudderstack event tables for given database and schema.
  move  Move tables to target database/schema.
```

Help is available at any command/sub-command, simply by adding `-h`/`--help` at the end of the command.

Let's go over the subcommands.

### List event tables

Simply run `rudder list ...` providing the proper arguments.

The following example will list all event tables in database `RAW`, schema `mattermost_docs`:

```bash
$ rudder list RAW mattermost_docs -a cca12345.us-east-1 -u joe@doe.com -r role_name -w warehouse_small
Password: 
DOC_EVENT
NEW_TABLE
...
```

The following example will list all event tables in database `RAW`, schema `mattermost_docs` that have been
created in the past two days:

```bash
$ rudder list RAW mattermost_docs -a cca12345.us-east-1 -u joe@doe.com -r role_name -w warehouse_small --max-age 2
Password: 
NEW_TABLE
...
```

### Move tables

Move all tables listed in input file/stdin from source database/schema to target/database schema.

Simply run `rudder move ...` providing the proper arguments.

The following example will move all tables listed in `tables.txt` from `DEV.STAGING` to `ARCHIVE.OLD_STAGING`

```bash
$ rudder move DEV STAGING ARCHIVE OLD_STAGING tables.txt -a cca12345.us-east-1 -u joe@doe.com -r role_name -w warehouse_small 
Password: 
Moving DEV.STAGING.table_to_archive ✓
...
```

If you want to use stdin rather than a file, you can specify `-` as input. Press  `[CTRL] + D` any time to stop.

Example:

```bash
$ rudder move DEV STAGING ARCHIVE OLD_STAGING - -a cca12345.us-east-1 -u joe@doe.com -r role_name -w warehouse_small 
Password: 

table_to_archive
Moving DEV.STAGING.table_to_archive ✓
...
```


### Configuring using environment variables

Options for all subcommands can also be set using environment variables. Here's the possible configuration variables:

| Environment Variable  | Example value       | Required | Description                               |
|-----------------------|---------------------|----------|-------------------------------------------|
| `SNOWFLAKE_ACCOUNT`   | `cca12345.us-east1` | Yes      | The snowflake account to connect to       |
| `SNOWFLAKE_USER`      | `user@example.com`  | Yes      | The username to connect to snowflake with |
| `SNOWFLAKE_PASSWORD`  | `super-secret`      | Yes      | The password to connect to snowflake with |
| `SNOWFLAKE_WAREHOUSE` | `cca12345.us-east1` | No       | The snowflake warehouse to use            |
| `SNOWFLAKE_ROLE`      | `cca12345.us-east1` | No       | The snowflake role to use                 |


## Contributors

Retrieves a list of contributors by iterating over all repos in the organization and checking merged PRs. The results
are stored in the target table (must already exist).

> Note that this script will download the whole contributor list and recreate the table from scratch. This might require
> significant time.

```bash
$ contributors  --help
Usage: contributors [OPTIONS] ORGANIZATION TABLE

  Store github contributors to target table.

Options:
  -a, --account TEXT    the name of the snowflake account  [required]
  -d, --database TEXT   the name of the snowflake database  [required]
  -s, --schema TEXT     the name of the snowflake schema  [required]
  -u, --user TEXT       the name of the user for connecting to snowflake
                        [required]
  -p, --password TEXT   the password for connecting to snowflake  [required]
  -w, --warehouse TEXT  the warehouse to use when connecting to snowflake
  -r, --role TEXT       the role to use when connecting to snowflake
  --token TEXT          the github token to use  [required]
  --help                Show this message and exit.
```

Here's an example on how to run it:

```bash
$ contributors  mattermost merged_prs \
      -a cca12345.us-east-1 \
      -u joe@doe.com \
      -r role_name \
      -w warehouse_small \
      -d RAW \
      -s STG_GITHUB \
      --token github_pat_11...
Password: 
Downloading pull requests...

```

Options can also be set using environment variables. Here's the possible configuration variables:

| Environment Variable  | Example value       | Required | Description                                     |
|-----------------------|---------------------|----------|-------------------------------------------------|
| `SNOWFLAKE_ACCOUNT`   | `cca12345.us-east1` | Yes      | The snowflake account to connect to             |
| `SNOWFLAKE_DATABASE`  | `RAW`               | Yes      | The database to use when connecting to snowflake |
| `SNOWFLAKE_SCHEMA`    | `PUBLIC`            | Yes      | The schema to use when connecting to snowflake  |
| `SNOWFLAKE_USER`      | `user@example.com`  | Yes      | The username to connect to snowflake with       |
| `SNOWFLAKE_PASSWORD`  | `super-secret`      | Yes      | The password to connect to snowflake with       |
| `SNOWFLAKE_WAREHOUSE` | `cca12345.us-east1` | No       | The snowflake warehouse to use                  |
| `SNOWFLAKE_ROLE`      | `cca12345.us-east1` | No       | The snowflake role to use                       |
| `GITHUB_TOKEN`        | `github_pat_11...`  | Yes      | The github token for authenticating             |

> Note that github token **MUST** be a [fine-grained token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token#creating-a-fine-grained-personal-access-token)
> with access to:
>  - Read repository info for organization.
>  - Read access to commit statuses, metadata, and pull requests.