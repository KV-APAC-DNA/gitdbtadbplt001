<!---
Provide a short summary in the Title above. Examples of good PR titles:
* "Feature: add so-and-so models"
* "Fix: deduplicate such-and-such"
-->

## Description & motivation
<!---
Describe your changes, and why you're making them. Is this linked to an open
issue, or another pull request? Link it here.
-->

## To-do before merge
<!---
Include any notes about things that need to happen before this PR is merged, e.g.:
- [ ] Change the base branch
- [ ] Update dbt Cloud jobs
- [ ] Ensure PR #56 is merged
-->

## Screenshots:
<!---
Include a screenshot of the relevant section of the updated DAG. You can access
your version of the DAG by running `dbt docs generate && dbt docs serve`.
-->

## Validation of models:
<!---
Include any output that confirms that the models do what is expected. This might
be a link to an in-development dashboard in your BI tool, or a query that
compares an existing model with a new one.
-->

## Changes to existing models:
<!---
Include this section if you are changing any existing models. Link any related
pull requests on your BI tool, or instructions for merge (e.g. whether old
models should be dropped after merge, or whether a full-refresh run is required)
-->

## Checklist:
<!---
Put an `x` in all the items that apply, make notes next to any that haven't been
addressed, and remove any items that are not relevant to this PR.
-->
- [ ] I confirmed and have no doubts that there are no confidential data, sensitive details and credentials in my code.
- [ ] I confirmed that I have not modified project root files such as dependencies.yml
- [ ] My pull request represents one logical piece of work.
- [ ] My commits are related to the pull request and look clean.
- [ ] My SQL follows the [dbt Labs style guide](https://docs.getdbt.com/guides/best-practices/how-we-style/0-how-we-style-our-dbt-projects).
- [ ] I have materialized my models appropriately.
- [ ] I have added appropriate tests and documentation to any new models.
- [ ] I have updated the README file.
