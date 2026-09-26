![Logo Big](docs/img/Logo_big.png)

# AflDbt

**AflDbt** is a lightweight Python library for the seamless integration of Apache Airflow and dbt, designed to automatically generate Airflow DAGs based on the `manifest.json` file. The tool dynamically transforms dbt project metadata into workflows, intelligently organizing models and tests into Tasks and Task Groups while fully preserving the dependency graph.

Powered by a flexible callback-based architecture and built-in pipeline visualization utilities (e.g., in Mermaid format), AflDbt enables data engineers to quickly build, debug, and maintain data pipelines, offering a minimalist alternative to more heavyweight solutions.

## Documentation

A comprehensive guide to the library's architecture, internal workings, and usage is available in the documentation.

**[Go to full documentation (docs/index.md)](docs/index.md)**

## Installation and Setup

The environment setup process for working with the library is divided into two main stages. Detailed step-by-step instructions, including virtual environment configuration and validation, can be found in the corresponding documentation sections:

1. **[Python Installation, Environment Setup, Running Unit Tests, and dbt Project Verification](docs/chap12.md)**
2. **[Apache Airflow Installation and Final Testing of the Test DAG File in Different Modes](docs/chap13.md)**

## Quick Start

1. Compile your dbt project to generate the `manifest.json` file:
   ```bash
   dbt compile
   ```
2. Specify the path to the dbt project and the `dbt` executable in the `AflDbt` configuration located within the DAG file.
3. Define callback functions for creating Airflow objects.
4. Test the DAG in Airflow.

*(For detailed code examples, see the [documentation](docs/index.md))*

## Version History

* 0.1
    * Initial release

## License

This project is distributed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

* Inspiration: [Building a Scalable Analytics Architecture With Airflow and dbt](https://www.astronomer.io/blog/airflow-dbt-1/)
