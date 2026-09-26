[<- Back to Table of Contents](index.md)

## Potential Development Directions

This section outlines possible directions for further library development. Each of them addresses a specific limitation of the current version and opens up new opportunities for users. Implementation of these improvements is planned in future versions according to their priority.

1. **Moving Installation to Docker**

In earlier chapters, it is described in detail how to install the library into a local development environment.
However, this path is quite lengthy, as it requires sequentially and carefully performing a sufficient number of steps to build Python, install dbt and Airflow, and verify their integration. Moreover, dbt and Airflow run in separate virtual environments that must not be confused.
For testing the library, an environment with a simple installation is more convenient for the end user.
This is exactly the kind of environment that Docker provides, as all installation automation can be placed in a Docker image build script.
Therefore, the next step in the library's development will be to move all installation automation into a Docker image.

2. **DAG Structure Caching**

Testing practice has shown that Airflow can execute DAG generation code multiple times, and this is its standard behavior. For example, when testing the `booksLibraryExample.py` script without running the scheduler, the log will show the DAG code being executed four times according to Airflow's phases. Each such execution is resource-intensive because it involves reading the `manifest.json` file, parsing, and dividing models and tests into tasks and task groups. Such frequent execution of the same code consumes resources and takes time. If the DAG and dbt project code does not change for a long time, it makes sense to implement caching. With caching, parsing and dividing models will be performed during the first execution with the direct DAG structure being written to the cache. On subsequent calls, the cache is rebuilt only when files change. Between file changes, the data of the final Airflow objects is read from the cache.

3. **Automatic dbt Project Compilation**

The library repository contains a 'target' directory and a `manifest.json` file within the dbt project, so the `booksLibraryExample.py` script can be run even without installing dbt and a separate environment for it. However, both this directory and this script are generated and are not required for other projects. When running, dbt creates both the directory and the `manifest.json` file. Therefore, these objects are absent in the repositories of other dbt projects. The library assumes the presence of such a file, which in the library project is created by a separate `dbt compile` command. It is advisable to automatically add this command to the DAG as a task, as well as the verification of the `manifest.json` file as a sensor. This will help avoid runtime errors if the library is used for other dbt projects where these generated files are not present.

4. **Support for Multiple dbt Environments**

A dbt project file can contain multiple environments for various usage scenarios, for example, for a development environment and for a production environment. Also, for different environments, the composition of models and tests may differ, as well as their implementation. In the current implementation of the library, environment filtering is not provided, and the test dbt project contains only the `dev` environment. For flexible use of the library, it is necessary to add the ability to specify an environment, as well as to implement auto-generation of the DAG name taking this parameter into account, so that two scripts that differ only in environment have different DAG identifiers in Airflow.

5. **Grouping Models with Their Tests**

Identifying task groups as chains, where each task has one parent and one child, is a good initial step for automatic group detection. Its advantage is that it allows using relatively simple code, which works in the simple case of running only models. However, when running both models and tests, such grouping no longer reflects the business logic of dividing into groups. If one model has several tests, the algorithm will not combine them into a single group. At the same time, tests are necessary even in the production environment for data integrity. Therefore, the further development of the library will be the ability to combine a model and its associated tests into a single group, which already reflects the business logic. In this regard, the future version will be a lightweight alternative to Astronomer Cosmos, which can already be used in the production environment.
