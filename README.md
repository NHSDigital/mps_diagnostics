# MPS Diagnostics

The Person_ID is a unique patient identifier used by NHS England with the objective of standardizing the approach to patient-level data linkage across different data sets.
Person_IDs are provided in many data sets available in NHS England, and are derived by the Master Person Service (MPS). For security and privacy reasons many users might have visibility of the tokenised version of the Person_ID, which provides an extra level of patient confidentiality.
MPS takes certain demographic information contained in a person’s health and care records and matches it to their unique NHS number to confirm their identity. The collection of all NHS numbers and patients’ demographic information is contained in the Personal Demographics Service (PDS) data set. 

MPS Diagnostics is a new pipeline which takes the contextual information from the MPS response file, and some additional data from PDS, to create 10 columns of meta data explaining in user-friendly terms how each Person ID was derived. MPS Diagnostics pipeline generates the mps_diagnostics data set, which contains record identifiers and the MPS diagnostics columns.
mps_diagnostics is available upon request for internal NHS England analysts via CDAs (clear data agreements), or for external NHS England users via DSAs (data sharing agreements). This document details the 10 columns, how they are derived and how they are to be used.

## Environment
This project runs on the Data Access Environment (DAE) instance of Databricks within NHS Digital. It follows the 'Code Promotion' project structure, which is specific to the NHS Digital 'Code Promotion' process. Code Promotion is a process designed by Data Processing Services (DPS) at NHS Digital to allow users of the DAE to promote code between environments and run jobs on an automated schedule. Within DAE, each Code Promotion project has three jobs which trigger the init_schemas.py, run_tests.py and run_notebooks.py notebooks. Therefore the project structure shown below is a requirement of Code Promotion projects.

> This repository may contain references internal to NHS England that cannot be accessed publicly. We are publishing for transparency only and it is not intended that others will be able to run this code.

## Contact

**This repository is maintained by the [NHS England Data Science Team](mailto:datascience@nhs.net)**.

> To contact us, please raise an issue on GitHub or via email.
>
> See our other work here: [NHS England Analytical Services](https://github.com/NHSDigital/data-analytics-services)

## Project structure

```
.
│   .gitignore                          <- Files and file types automatically removed from version control for security purposes
│   LICENCE                             <- License information for public distribution
│   README.md                           <- Explanation of the project
│   
└───staging
    └───mps_diagnostics
        │   cp_config.py                <- Sets some parameters for Databricks jobs wrapping the top-level entry point notebooks
        │   init_schemas.py             <- Initialises table schemas
        │   run_notebooks.py            <- Runs main notebook
        │   run_tests.py                <- Runs unit tests
        │   
        ├───notebooks
        │       config.py               <- Initialises variables
        │       function_test_suite.py  <- Defines class and functions for registering and running tests
        │       imports.py              <- Imports packages
        │       main.py                 <- Runs overall pipeline
        │       utils.py                <- Defines functions used in pipeline
        │       
        ├───schemas
        │       placeholder.py          <- Placeholder for schemas
        │       
        └───tests
                integration_test.py     <- End-to-end test of pipeline to ensure each component of the codebase interacts correctly
                utils_test.py           <- Unit tests for utility functions
```
## Licence

Unless stated otherwise, the codebase is released under the [MIT License](./LICENCE).

Any HTML or Markdown documentation is [© Crown copyright](https://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/) and available under the terms of the [Open Government 3.0 licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

## Acknowledgements
- [Giulia Mantovani](https://github.com/GiuliaMantovani1)
- [Liliana Valles Carrera](https://github.com/lilianavalles)
- [Kenneth Quan](https://github.com/quan14)
- [Jonathan Laidler](https://github.com/JonathanLaidler)
- Amelia Noonan
- Hilary Chan
- Alice Tapper
- [Xiyao Zhuang](https://github.com/xiyaozhuang)
