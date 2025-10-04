dbt Documentation
=================

This page provides access to the dbt documentation generated from your dbt project.

.. raw:: html
   :file: dbt.html

.. note::
   This documentation is automatically generated from your dbt project. 
   To regenerate it, run::
   
      pipenv run dbt docs generate
      pipenv run dbt docs serve

The dbt documentation includes:

* **Model definitions** - SQL models and their transformations
* **Data lineage** - How data flows through your models
* **Tests** - Data quality tests and their results
* **Schema documentation** - Column descriptions and data types

For more information about dbt development in this project, see :doc:`dbt_models`.
