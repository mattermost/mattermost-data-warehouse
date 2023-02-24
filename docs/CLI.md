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
```

Help is available at any command/sub-command, simply by adding `-h`/`--help` at the end of the command.

Let's go over the subcommands.

### List event tables

Simply run `rudder list ...` providing the proper arguments.

Example:
```bash
$ rudder list RAW mattermost_docs -a cca12345.us-east-1 -u joe@doe.com -r role_name -w warehouse_small
Password: 
DOC_EVENT
```

Options can also be set using environment variables. Here's the possible configuration variables:

| Environment Variable  | Example value       | Required | Description                               |
|-----------------------|---------------------|----------|-------------------------------------------|
| `SNOWFLAKE_ACCOUNT`   | `cca12345.us-east1` | Yes      | The snowflake account to connect to       |
| `SNOWFLAKE_USER`      | `user@example.com`  | Yes      | The username to connect to snowflake with |
| `SNOWFLAKE_PASSWORD`  | `super-secret`      | Yes      | The password to connect to snowflake with |
| `SNOWFLAKE_WAREHOUSE` | `cca12345.us-east1` | No       | The snowflake warehouse to use            |
| `SNOWFLAKE_ROLE`      | `cca12345.us-east1` | No       | The snowflake role to use                 |

 



