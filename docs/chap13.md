[<- Back to Table of Contents](index.md)

## Apache Airflow Installation and Final Testing of the Test DAG File in Different Modes

### Installing Apache Airflow 2.11.2 for Local Execution

The Apache Airflow installation process differs from dbt installation. For Airflow, not only the Airflow version is critical, but also the Python version, because the `pip` manager uses a special text file of constraints. It strictly defines the allowed versions of packages installed in the environment as transitive (indirect) dependencies. Since libraries can update over time and cause unpredictable system behavior, Airflow strictly fixes the versions of all components.

The constraints file is stored in the Airflow project repository on GitHub, and the link to it depends on the Airflow and Python versions. The project is constantly evolving, so not all environment combinations work stably. Within the AflDbt project, the compatibility of Apache Airflow 2.11.2 with Python 3.11.16 has been practically tested and confirmed.
When installing Airflow, it is customary to use the `AIRFLOW_VERSION` and `PYTHON_VERSION` environment variables to specify exact versions. The Airflow version is specified completely, including the patch number (for example, `2.11.2`), and the Python version - without the micro version (for example, `3.11`).

**Important note:** AI assistants often suggest an incorrect template for the constraints file, for example: `https://githubusercontent.com${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt`. After substituting the variables, it turns into a non-existent URL (for example, `https://githubusercontent.com2.11.2/constraints-3.11.txt`), which predictably does not work. Also, the incorrect base URL `https://githubusercontent.com` will not work. 
It is highly recommended to verify the correctness of the generated link before running `pip` by opening it in a browser or sending a request via `curl`.

The correct link is built according to the following template:
```text
https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt
```
The Airflow installation steps must include checking this link before running `pip`. If following it returns an error, refer to the installation section in the official Airflow documentation to clarify the current template.

**Important:** Airflow installation must be performed in the activated `aflVe` virtual environment.
The sequence of actions for installation looks as follows:
```bash
# 1. Activate virtual environment
source ~/MyProjects/aflVe/bin/activate

# 2. Set environment variables
AIRFLOW_VERSION=2.11.2
PYTHON_VERSION=3.11

# 3. Define constraint URL
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# 4. Print URL for verification (copy to browser if needed)
echo "$CONSTRAINT_URL"

# 5. Check URL availability (show first lines only)
curl -s "$CONSTRAINT_URL" | head

# 6. Install Airflow
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Let's verify that Airflow is successfully installed:
```bash
airflow version
```

The installation is complete, but to use Airflow, it must be initialized. This process includes creating the folder structure, generating configuration files, and initializing the metadata database. In a local installation, the metadata database is stored by default in a local SQLite file named `airflow.db`, unless another provider and database (e.g., on a PostgreSQL server) is specified.
By default, Airflow uses `~/airflow` as the home directory, where both the metadata database and the `dags` folder for storing DAG files are created. This makes direct use of our cloned project impossible.
Therefore, before initialization, it is necessary to export the `AIRFLOW_HOME` environment variable, specifying the full path to our `AflDbt` project directory. This will allow Airflow to immediately reference the project's `dags` directory and correctly use modules from the library.

It also makes sense to immediately disable the loading of built-in example DAGs during metadata database initialization by setting the `AIRFLOW__CORE__LOAD_EXAMPLES=False` environment variable. 

The initialization sequence is as follows:
```bash
# 1. Set absolute path to project directory
export AIRFLOW_HOME=$(realpath ~/MyProjects/AflDbt)   

# 2. Disable loading of example DAGs
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# 3. Initialize Airflow database
airflow db migrate
```

After initialization, it is recommended to disable the loading of built-in example DAGs directly in the `airflow.cfg` configuration file. This will allow you not to set the `AIRFLOW__CORE__LOAD_EXAMPLES` environment variable on every run in the future.
In the configuration file, this parameter is called `load_examples`. Its value can be changed using the following command:
```bash
sed -i 's/load_examples = True/load_examples = False/g' "$AIRFLOW_HOME/airflow.cfg"
```

The `AIRFLOW_HOME` variable must be set every time you work with Airflow. To avoid exporting it manually in the terminal every time, it is more convenient to add this command to the virtual environment activation script (`activate`).
To automatically update the `activate` script (adding the export on entry and clearing the variable on exit), you can run the following commands:
```bash
# 1. Add export AIRFLOW_HOME on activation
echo 'export AIRFLOW_HOME=$(realpath ~/MyProjects/AflDbt)' >> ~/MyProjects/aflVe/bin/activate

# 2. Unset AIRFLOW_HOME when exiting virtual environment
sed -i '/^deactivate () {$/a\    unset AIRFLOW_HOME' ~/MyProjects/aflVe/bin/activate
```

When working with Airflow 2.11.2, two types of warnings may appear in the logs, which are recommended to be eliminated immediately so they do not interfere with perceiving the main information. The first warning concerns the time format:
```log
/home/user/MyProjects/aflVe/lib/python3.11/site-packages/airflow/metrics/base_stats_logger.py:22 RemovedInAirflow3Warning: Timer and timing metrics publish in seconds were deprecated. It is enabled by default from Airflow 3 onwards. Enable timer_unit_consistency to publish all the timer and timing metrics in milliseconds.
```

To eliminate it, you need to set the `timer_unit_consistency = True` parameter:
```bash
sed -i 's/timer_unit_consistency = False/timer_unit_consistency = True/g' "$AIRFLOW_HOME/airflow.cfg"
```

The second warning concerns the `graphviz` library:
```log
/home/user/MyProjects/aflVe/lib/python3.11/site-packages/airflow/cli/commands/dag_command.py:48 UserWarning: Could not import graphviz. Rendering graph to the graphical format will not be possible.
```

To eliminate it, you need to install this library: first at the operating system level, and then as a Python module:
```bash
# 1. Install system library
sudo apt-get install graphviz

# 2. Install Python module
pip install graphviz
```

The installation and basic configuration of Airflow are complete. You can proceed to run the test DAG.

### Testing the Test DAG File Without Running the Scheduler and Web Interface

The test DAG file `booksLibraryExample.py` is implemented as an executable script, which allows it to be run without a running Airflow scheduler and web interface.
To run in this mode, it is enough to execute this script from the root directory of the project `~/MyProjects/AflDbt`:
```bash
# 1. Navigate to project directory
cd ~/MyProjects/AflDbt

# 2. Run test DAG
python dags/booksLibraryExample.py
```

In this case, Airflow executes the DAG in local mode (using `SequentialExecutor`), emulating the task execution process without a background scheduler.

The execution log will look approximately as follows:
```log
[<some-timestamp>] {booksLibraryExample.py:134} INFO - create AMain class instance
[<some-timestamp>] {booksLibraryExample.py:138} INFO - running AMain.Process
[<some-timestamp>] {booksLibraryExample.py:146} INFO - running AMain.Process ... Done
[<some-timestamp>] {executor_loader.py:258} INFO - Loaded executor: SequentialExecutor
[<some-timestamp>] {taskinstance.py:2632} INFO - Dependencies all met for dep_context=None ti=<TaskInstance: dbt_books_library.run_books backfill__XXXX-XX-XXTXX:XX:XX.XXXXXX+XX:XX [scheduled]>
[<some-timestamp>] {base_executor.py:169} INFO - Adding to queue: ['airflow', 'tasks', 'run', 'dbt_books_library', 'run_books', 'backfill__XXXX-XX-XXTXX:XX:XX.XXXXXX+XX:XX', '--depends-on-past', 'ignore', '--local', '--pool', 'default_pool', '--subdir', 'DAGS_FOLDER/booksLibraryExample.py', '--cfg-path', '/tmp/tmpjzozqhap']
[.................................]
[<some-timestamp>] {dagrun.py:854} INFO - Marking run <DagRun dbt_books_library @ XXXX-XX-XX XX:XX:XX.XXXXXX+XX:XX: backfill__XXXX-XX-XXTXX:XX:XX.XXXXXX+XX:XX, state:running, queued_at: None. externally triggered: False> successful
[<some-timestamp>] {dagrun.py:905} INFO - DagRun Finished: dag_id=dbt_books_library, execution_date=XXXX-XX-XX XX:XX:XX.XXXXXX+XX:XX, run_id=backfill__XXXX-XX-XXTXX:XX:XX.XXXXXX+XX:XX, run_start_date=XXXX-XX-XX XX:XX:XX.XXXXXX+XX:XX, run_end_date=XXXX-XX-XX XX:XX:XX.XXXXXX+XX:XX, run_duration=28.793708, state=success, external_trigger=False, run_type=backfill, data_interval_start=XXXX-XX-XX XX:XX:XX.XXXXXX+XX:XX, data_interval_end=XXXX-XX-XX XX:XX:XX.XXXXXX+XX:XX, dag_hash=None
[<some-timestamp>] {backfill_job_runner.py:464} INFO - [backfill progress] | finished run 1 of 1 | tasks waiting: 0 | succeeded: 4 | running: 0 | failed: 0 | skipped: 0 | deadlocked: 0 | not ready: 0
[<some-timestamp>] {backfill_job_runner.py:1051} INFO - Backfill done for DAG <DAG: dbt_books_library>. Exiting.
```
where `XXXX-XX-XXTXX:XX:XX.XXXXXX+XX:XX` and `XXXX-XX-XX XX:XX:XX.XXXXXX+XX:XX` are placeholders for timestamps in various formats, which in reality look like, for example, `2025-01-01T01:01:01.040000+00:00` or `2025-01-01 00:04:40.240000+00:00`.

From the log, it is clear that the DAG executed successfully: `DagRun Finished: dag_id=dbt_books_library` and `state=success`. Also, all tasks of this DAG executed successfully: `[backfill progress] | finished run 1 of 1 | tasks waiting: 0 | succeeded: 4`.

Execution logs are saved in the `logs` directory and have the following structure:
```mermaid
---
config:
  treeView:
    showIcons: true
---
treeView-beta
AflDbt/  ## Project root folder
    logs/  ## Airflow logs
        dag_id=dbt_books_library/    ## "dbt_books_library" DAG logs
            run_id=backfill__XXXX-XX-XXTXX:XX:XX.XXX+XXXX/  ## specific DAG execution logs
                task_id=run_books/  ## task "run_books" execution
                    attempt=1.log  ## task "run_books" attempt
                .../  ## other tasks
```

For example, the `attempt=1.log` file for the `run_books` task contains the log of a successful `dbt` call:
```log
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:88} INFO - Running command: ['/usr/bin/bash', '-c', '/home/user/MyProjects/dbtVe/bin/dbt run --project-dir /home/user/MyProjects/AflDbt/dbt/books_library --profiles-dir /home/user/MyProjects/AflDbt/dbt/books_library -s books']
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:99} INFO - Output:
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX  Running with dbt=1.11.14
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX  Registered adapter: sqlite=1.10.0
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX  Found 4 models, 7 data tests, 416 macros
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX  Concurrency: 1 threads (target='dev')
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX  1 of 1 START sql view model main.books ......................................... [RUN]
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX  1 of 1 OK created sql view model main.books .................................... [OK in 0.05s]
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX  Finished running 1 view model in 0 hours 0 minutes and 0.26 seconds (0.26s).
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX  Completed successfully
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:106} INFO - XX:XX:XX  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {subprocess.py:110} INFO - Command exited with return code 0
```
Testing the test DAG by directly running the script is complete.

### Testing DAG Execution via Scheduler in the Command Line

The next step is to test the launch of the test DAG using the scheduler, but without the web interface - in the command line.
To do this, you need to start the scheduler, which is easiest to do as a background task:
```bash 
airflow scheduler &
```
The scheduler outputs logs to the same terminal, so you should wait for the initialization to complete and a pause in the output to appear before executing subsequent commands.

Before running the DAG, you must ensure that it exists in the Airflow metadata database:
```bash
airflow dags list
```
Command output:
```log
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {plugins.py:37} INFO - setup plugin alembic.autogenerate.schemas
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {plugins.py:37} INFO - setup plugin alembic.autogenerate.tables
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {plugins.py:37} INFO - setup plugin alembic.autogenerate.types
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {plugins.py:37} INFO - setup plugin alembic.autogenerate.constraints
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {plugins.py:37} INFO - setup plugin alembic.autogenerate.defaults
[XXXX-XX-XXTXX:XX:XX.XXX+XXXX] {plugins.py:37} INFO - setup plugin alembic.autogenerate.comments
dag_id            | fileloc                                                 | owners  | is_paused
==================+=========================================================+=========+==========
dbt_books_library | /home/user/MyProjects/AflDbt/dags/booksLibraryExample.py | airflow | None 
```

As you can see, the `dbt_books_library` DAG exists in the database. Now it can be launched:
```bash
airflow dags trigger dbt_books_library
```

The command will output approximately the following result:
```log
[YYYY-YY-YYTYY:YY:YY.YYY+YYYY] {__init__.py:43} INFO - Loaded API auth backend: airflow.api.auth.backend.session
 conf | dag_id          | dag_run_id                 | data_interval_start      | data_interval_end        | end_date | external_trigger | last_scheduling_decision | logical_date             | run_type | start_date | state  
======+=================+============================+==========================+==========================+==========+==================+==========================+==========================+==========+============+========
 {}   | dbt_books_library | manual__YYYY-YY-YYTYY:YY | YYYY-YY-YY YY:YY:YY+YY:Y | YYYY-YY-YY YY:YY:YY+YY:Y | None     | True             | None                     | YYYY-YY-YY YY:YY:YY+YY:Y | manual   | None       | queued
```

No further actions will be performed at this point: the `manual__YYYY-YY-YYTYY:YY:YY+YY:YY` task was simply put in the queue (`queued`).

You can verify this with the following command, which shows the current state of this DAG:
```bash
airflow dags list-runs --dag-id dbt_books_library
```

Output of this command:
```log
dag_id            | run_id                               | state   | execution_date                | start_date                    | end_date                       
==================+======================================+=========+===============================+===============================+================================
dbt_books_library | manual__YYYY-YY-YYTYY:YY:YY+YY:YY    | queued  | YYYY-YY-YYTYY:YY:YY+YY:YY     |                               |                                
dbt_books_library | backfill__XXXX-XX-XXTXX:XX:XX.XXXXXX | success | XXXX-XX-XXTXX:XX:XX.XXXXXX+XX | XXXX-XX-XXTXX:XX:XX.XXXXXX+XX | XXXX-XX-XXTXX:XX:XX.XXXXXX+XX
```

To execute the task, you need to unpause the DAG, as it is in the `paused` state:
```bash
airflow dags unpause dbt_books_library
```

After that, the scheduler will start executing the DAG and output the log to the terminal. At the end, the following lines will be output:
```log
[YYYY-YY-YYTYY:YY:YY.YYY+YYYY] {dagrun.py:854} INFO - Marking run <DagRun dbt_books_library @ YYYY-YY-YY YY:YY:YY+YY:YY: manual__YYYY-YY-YYTYY:YY:YY+YY:YY, state:running, queued_at: YYYY-YY-YY YY:YY:YY.YYYYYY+YY:YY. externally triggered: True> successful
[YYYY-YY-YYTYY:YY:YY.YYY+YYYY] {dagrun.py:905} INFO - DagRun Finished: dag_id=dbt_books_library, execution_date=YYYY-YY-YY YY:YY:YY+YY:YY, run_id=manual__YYYY-YY-YYTYY:YY:YY+YY:YY, run_start_date=YYYY-YY-YY YY:YY:YY.YYYYYY+YY:YY, run_end_date=YYYY-YY-YY YY:YY:YY.YYYYYY+YY:YY, run_duration=28.076293, state=success, external_trigger=True, run_type=manual, data_interval_start=YYYY-YY-YY YY:YY:YY+YY:YY, data_interval_end=YYYY-YY-YY YY:YY:YY+YY:YY, dag_hash=59572beaebd8273ecf2aaed6fa51bae9
```

The test DAG executed successfully.

In the Airflow logs directory `logs/dag_id=dbt_books_library`, an additional directory `run_id=manual__YYYY-YY-YYTYY:YY:YY+YY:YY` will appear, where the execution logs from the command line launch will be stored.

Testing the test DAG using this method is complete. 

### Final DAG Execution Check in the Airflow Web Interface

After testing in script mode and in the command line, checking in the web interface is straightforward.
But to use it, you first need to create an account to log into the web interface.
The easiest way is to immediately create an administrator account for local testing:
```bash
airflow users create --username admin --password admin --firstname Anonymous --lastname Admin --role Admin --email admin@example.org
``` 

When working with the web interface, the scheduler must be running, which is already running in the background. 
The Airflow web server can be started in this same terminal session. In this case, this session will be temporarily unavailable for other commands, but for the duration of testing, this is not critical - control will be carried out through the web interface.

The web server is started with the command:
```bash
airflow webserver
```

Upon completion of initialization, the Airflow web interface will be available at `http://localhost:8080`. 
After logging in, a list of registered DAGs is displayed:

<img src="img/afl_01_dags_list.png" alt="DAGs list" style="border: 2px solid #333333; padding: 5px;">

As you can see, there is only one DAG in the list, `dbt_books_library`, it is active, has been executed twice, and both times successfully.

If you open this DAG, you can see both the execution history and the details:

<img src="img/afl_02_dag_details.png" alt="DAG details" style="border: 2px solid #333333; padding: 5px;">

As you can see, two successful runs were performed. In the upper right corner, the run schedule status is shown: `Schedule: None` and `Next Run ID: None`. After the second entry is the DAG run icon. Since the DAG is intended for manual execution, both the schedule and the next run are absent.

If you go to the `Graph` tab, you can see the graph itself for the DAG:

<img src="img/afl_03_dag_graph.png" alt="DAG graph" style="border: 2px solid #333333; padding: 5px;">

The graph reflects the logic of the library: it builds a sequence of dbt tasks, two of which are combined into a group.
To run the DAG, you need to click the run icon in the upper right corner. After that, on the `Graph` tab, you can observe the sequential execution of tasks: 

<img src="img/afl_04_dag_running.png" alt="DAG run" style="border: 2px solid #333333; padding: 5px;">

Since `SequentialExecutor` is used by default in a local installation, tasks will be executed one at a time. Ultimately, the execution should complete successfully, and now there will be three successful runs in the history. 
 
Final testing of the DAG in the web interface is complete.

To stop the web server in the terminal session, you need to press `Ctrl-C` and wait for it to stop.
After it stops, you can execute commands in the session again. To stop the scheduler, you must first activate the background process with the `fg` command, and then also press `Ctrl-C`.

Testing the DAG in all modes is complete.
Successful passing of tests in all three modes confirms the correct operation of the AflDbt library.
In the next section, potential directions for its improvement are considered.

[Potential Development Directions ->](chap14.md)
