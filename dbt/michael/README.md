# Personal Reporting

## Layers

Model layers have been implemented as recommended by [DBT project structure](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

   Layer      |           Description                          |                         Scope
--------------|------------------------------------------------|---------------------------------------------------------
 Staging      | Foundational models organised by source system | Renaming, type casting, basic computations, categorising
 Intermediate | Apply complex transformations by focus area    | Structural simplification, re-graining, isolating complex operations
 Marts        | Entity or concept layer, denormalised          | Standard entity concepts, built wide, and extended thoughtfully


## Tests

- Data Tests: Used to validate conditions of datasets on ingestion or data modelling
  - [Standard Generic tests](https://docs.getdbt.com/docs/build/data-tests): basic validations (unique, not null, relationship)
  - [DBT Expectations package](https://hub.getdbt.com/metaplane/dbt_expectations/latest/): large variety of additional validations
  - [Custom Generic tests](https://docs.getdbt.com/best-practices/writing-custom-generic-tests): for additional logic not available in those above
