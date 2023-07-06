# User Defined Functions

Definition of macros that create a user defined function (UDF).

To create a new UDF:
- Add the code that creates the UDF in this directory
- Edit `dbt_project.yml` to call UDF creation macro on run startup. Example:

```shell
on-run-start:
  -  '{{ create_parse_qs_udf()}}'
```