# MPS Diagnostics

The **Master Patient Service** (MPS) takes certain demographic information contained in a person’s health and care records and matches it to their unique NHS number to confirm their identity.

The collection of all NHS numbers and patients’ demographic information is contained in the **Personal Demographics Service** (PDS) data set.

>**Warning:** this repository may contain references internal to NHS England that cannot be accessed publicly

## Contact

**This repository is maintained by the [NHS England Data Science Team](datascience@nhs.net)**.

> To contact us, please raise an issue on Github or via email.
>
> See our other work here: [NHS England Analytical Services](https://github.com/NHSDigital/data-analytics-services)

## Description

The Person_ID is a unique patient identifier used by NHS England with the objective of standardizing the approach to patient-level data linkage across different data sets.

Person_IDs are provided in many data sets available in NHS England, and are derived by MPS. For security and privacy reasons many users might have visibility of the tokenised version of the Person_ID, which provides an extra level of patient confidentiality.

The Person_ID handbook for HES users explains in detail the algorithms by which MPS derives a Person_ID from the demographic information of the patient. In that document, we gave several example case studies where the Person_ID returned by MPS may confuse analysts. Such confusion can occur because analysts are presented with only the Person_ID, and not the contextual information about how the Person_ID was derived, for example which demographic data led to finding the match. Some relevant contextual information can be found in the MPS response table, however this table is not shared with users.

MPS Diagnostics is a new pipeline and database which takes the contextual information from the MPS response file, and some additional data from PDS, in order to create 10 columns of meta data explaining in user-friendly terms how each Person_ID was derived.

MPS diagnostics is available via DARS data sharing agreements and internally via CDAs.

## Licence

Unless stated otherwise, the codebase is released under the [MIT License](./LICENCE).

Any HTML or Markdown documentation is [© Crown copyright](https://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/) and available under the terms of the [Open Government 3.0 licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

## Acknowledgements
- Jonathan Laidler
- Amelia Noonan
- Hilary Chan
- Alice Tapper
- [Xiyao Zhuang](https://github.com/xiyaozhuang)
