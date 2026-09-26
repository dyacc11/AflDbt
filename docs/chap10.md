[<- Back to Table of Contents](index.md)

## Caveats: Imports and Paths

In the DAG example above, imports were omitted. This was done for a clear reason: if you write an import like `from X.Y import Z`, Python must successfully resolve this path.

There are three main ways to solve this problem:
* **Place the library in paths available in `sys.path`.** This option is inconvenient for deployment and complicates simple repository cloning. Moreover, placing third-party code in subdirectories of the `dags` folder will force the Airflow scheduler to constantly and inefficiently parse these files.
* **Use the `PYTHONPATH` environment variable.** This method also requires environment configuration during deployment and may not work if Python is run in isolated or secure mode.
* **Dynamically modify `sys.path` directly in the code before importing.** Among simple and self-contained solutions, this is the most practical option.

It is most convenient to place the library and tests in the root directory of the project, that is, at the same level as the `dags` folder:
```mermaid
---
config:
  treeView:
    showIcons: true
---
treeView-beta
Airflow-root/  ## root Airflow folder
    src/    ## AflDbt library
    dags/      :::highlight ## Airflow DAGs folder
    tests/     ## AflDbt library tests
```
In this case, the code itself will not depend on Airflow, but to import it, you will need to add the root path to `sys.path`.

This approach has its downside: since the Airflow scheduler cyclically parses DAG files, dynamic path computation at the beginning of the script will create a small regular load on the CPU. Nevertheless, this solution is a compromise, as it avoids the need for additional deployment configuration.

Therefore, in the real code, there are additional lines at the top:

```python
from pathlib import Path
import sys
import shutil

# get the ../dags absolute path
projectRoot = Path(__file__).resolve().parent.parent

# append only if it's not in the sys.path
if projectRoot not in sys.path:
    sys.path.append(str(projectRoot))
```
Since `pathlib` returns an object of type `Path`, it must be converted to a string using the `str()` function, otherwise the code will not work correctly.

The second point concerns specifying the path to `dbt` and the dbt project. Since in the repository the dbt project is located at the same level as the `dags` folder, the configuration can be set dynamically using the previously computed `projectRoot` variable.

`BashOperator` runs commands in a separate `bash` session. Because of this, the process will not be able to find the `dbt` executable in its `PATH` environment variable.
In our case, as shown later in the section "Python Installation, Environment Setup, Running Unit Tests, and dbt Project Verification", Airflow and dbt are installed in different virtual environments.
To solve this problem, we create virtual environments at the same level as the AflDbt project, which is cloned from the repository. Then, to compute the path to the `dbt` executable, we need to take the parent directory from `projectRoot`, add the virtual environment directory to it, and add the path to `dbt` inside the virtual environment.
The resulting full path will be: `/home/user/MyProjects/dbtVe/bin/dbt`

Dynamic computation of the path to the dbt project and the `dbt` command in the code looks like this:
```python
# values for dbtData Configuration
dbtRoot = projectRoot / "dbt" / "books_library" 
dbtManifest = str(dbtRoot / "target" / "manifest.json")
dbtRoot = str(dbtRoot)

# Build the full path of the "dbt" executable
dbtCmd = projectRoot.parent / "dbtVe" / "bin" / "dbt"
logger.debug(f"The full path to dbt is: {dbtCmd}")

#  .... some code here .....

dbtData = {
 "DBT_PROJECT_DIR":str(dbtRoot),
 "DBT_COMMAND":dbtCmd,
 "DBT_MANIFEST_PATH":str(dbtManifest),
 "SKIP_DBT_TEST":"True"
}
```
Thus, if dbt is called via `BashOperator` in a temporary directory (for example, `/tmp/SomeRandomFolder`), the following command will be formed for it:

```bash
/home/user/MyProjects/dbtVe/bin/dbt run --project-dir /home/user/MyProjects/AflDbt/dbt/books_library --profile /home/user/MyProjects/AflDbt/dbt/books_library someModel
```
Here `/home/user/MyProjects/AflDbt` is the absolute path to the project in your environment. Thanks to the use of full paths, the command will execute successfully regardless of the current working directory.

The third point concerns the database. Even if you run the command with absolute paths to the project and profile, this is still not enough for `dbt` to work: it will not be able to find the `books_library.db` database file when running from a temporary directory (for example, `/tmp/SomeRandomFolder`).

This problem can be solved using the `AFLDBT_DBT_DB` environment variable, specifying it in the `profiles.yml` file as a prefix to the path. Now the `profiles.yml` configuration will look like this:
```yaml
books_library:
  target: dev
  outputs:
    dev:
      type: sqlite
      threads: 1
      database: 'books_library'
      schema: 'main'
      schema_directory: '.'
      schemas_and_paths:
        main: '{{ env_var("AFLDBT_DBT_DB", "") }}books_library.db'
```
With this approach, flexibility is preserved: if the environment variable is absent, paths are resolved according to the old scheme, which allows you to debug the dbt project locally without Airflow without any problems. However, if the launch occurs inside Airflow, we simply pass this variable in the `env` parameter when creating `BashOperator`:
```python
    task = BashOperator(
        task_id=task_id,
        bash_command= execStr,  # e.g., "dbt run --project-dir ... -s s_task"
        dag=_dag,
        env={
            "AFLDBT_DBT_DB": f"{dbtRoot}/", # Trailing slash included
        },
        task_group=grp,
    )
```
In this case, the `AFLDBT_DBT_DB` environment variable will take the value `/home/user/MyProjects/AflDbt/dbt/books_library/` (including the trailing slash). Then, when calling dbt, the path to the database for the `main` schema in the `profiles.yml` file will be transformed to `/home/user/MyProjects/AflDbt/dbt/books_library/books_library.db`. As a result, the database file will be guaranteed to be found regardless of the current working directory.

The analysis of path and configuration caveats is complete. In the next section, we will examine the organization of library testing.

[Unit Tests, createMermaid.py Utility, and Running DAG as a Python Script ->](chap11.md)
