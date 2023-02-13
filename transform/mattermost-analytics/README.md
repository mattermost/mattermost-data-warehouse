# Mattermost Analytics 

Welcome to Mattermost Analytics project. This projects uses DBT to define transformations in data.

## Project Structure

This project follows the structure suggested in
[How we structure our DBT projects](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview).

```
├── dbt_project.yml       <--- Project configuration.
├── packages.yml          <--- Define dependencies to external packages required by the project.
├── README.md             <--- This file.
├── analyses              <--- Any custom analyses.
├── macros                <--- Custom macros defined by us.
├── models                <--- The project's models.
│  ├── staging            <--- Staging models. Subdirectories MUST be split by source system.
│  ├── intermediate       <--- Intermediate models. Subdirectories MUST be split on business groupings.
│  ├── marts              <--- Data marts. Group by department or area of concern.
│  └── utilities          <--- General purpose models that we generate from macros or based on seeds.
├── profile
│  └── profiles.yml       <--- Database connection configurations.
├── seeds                 <--- Seed data and lookup tables.
├── snapshots             <--- Used for creating Type 2 slowly changing dimension records from Type 1 (destructively updated) source data.
└── tests                 <--- USed for testing multiple specific tables simultaneously.
```

## Conventions

### Schema names

The [alternative pattern](https://docs.getdbt.com/docs/build/custom-schemas#an-alternative-pattern-for-generating-schema-names)
is used. A frequently used pattern for generating schema names is to change the behavior based on dbt's environment:
- In prod:
  - If a custom schema is provided, a model's schema name should match the custom schema, rather than being concatenated to the target schema.
  - If no custom schema is provided, a model's schema name should match the target schema.
- In other environments (e.g. dev or qa):
  - Build all models in the target schema, as in, ignore custom schema configurations.

## List of packages

| Package                                                        | Description                                                    |
|----------------------------------------------------------------|----------------------------------------------------------------|
| [dbt_utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) | Useful macros, reused across projects                          |
| [codegen](https://hub.getdbt.com/dbt-labs/codegen/latest/)     | Macros that generate dbt code, and log it to the command line. |

## Resources

- [How we structure our DBT projects](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview)
- [DBT Snowflake best practices](https://www.snowflake.com/wp-content/uploads/2021/10/Best-Practices-for-Optimizing-Your-dbt-and-Snowflake-Deployment.pdf)