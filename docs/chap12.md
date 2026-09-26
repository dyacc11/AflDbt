[<- Back to Table of Contents](index.md)

## Python Installation, Environment Setup, Running Unit Tests, and dbt Project Verification

For security reasons, it is strictly not recommended to modify or replace the system Python, as the stable operation of the entire operating system depends on it. Interfering with the pre-installed version may disrupt its correct functioning. Therefore, for the development environment, we will use a separate, newer version of Python installed in the `/opt` directory.

The Python version to install is determined by the requirements of **Airflow** and **dbt**. In this case, the minimum requirements for both packages are identical:
* For **dbt** (version 1.11), Python `3.10` or higher is required.
* For **Airflow** (version 2.11.2), Python `3.10` or higher is also required.
The optimal choice is to install a newer minor version of Python, such as `3.11.16`.

The process of installing and testing the library consists of the following sequential steps:
1. Installing Python 3.11.16.
2. Creating isolated virtual environments based on Python 3.11.16.
3. Cloning the repository from GitHub and running unit tests.
4. Verifying the library's operation using the `createMermaid` utility.
5. Installing the `dbt-core` (version 1.11) and `dbt-sqlite` packages.
6. Verifying the correctness of the dbt project.
7. Installing Apache Airflow 2.11.2 for local execution.
8. Testing the DAG file without starting the scheduler and web interface.
9. Testing the DAG operation with the scheduler running in the command line.
10. Final verification of the DAG operation in the Airflow web interface.

This section covers steps 1 through 6, while steps 7 through 10 will be discussed in the next part.

### Installing Python 3.11.16 from Source

Python is built from source code. After building, the package is installed in the `/opt` directory via `altinstall` to isolate it from the system Python. Below is the sequence of actions for Debian/Ubuntu systems.

First, using `sudo` privileges, we will install the packages required for building:

```bash
sudo apt-get update && sudo apt-get install -y build-essential libssl-dev zlib1g-dev \
libncurses-dev libgdbm-dev libnss3-dev libsqlite3-dev libreadline-dev libffi-dev curl uuid-dev
```

Next, we will download the archive from the official Python server to the `/tmp` directory and extract it. The source code for different versions is located in the server's FTP directory and has a URL like `https://www.python.org/ftp/python/VERSION/Python-VERSION.tgz`. In our case, `VERSION=3.11.16`. For convenience, we will save this number in the `P_VER` environment variable:

```bash
cd /tmp
P_VER=3.11.16
wget https://www.python.org/ftp/python/$P_VER/Python-$P_VER.tgz
tar -xf Python-$P_VER.tgz
cd Python-$P_VER
```

After that, we will run the configuration script, passing the target installation path in the `--prefix` parameter. Two other flags activate the installation of `pip` and enable performance optimization for the final binary file:
```bash
./configure --prefix=/opt/python$P_VER --enable-optimizations --with-ensurepip=install
```

The next step is to build the project, utilizing all available processor cores:
```bash
make -j$(nproc)
```

The built package is installed in the target `/opt/` directory via `altinstall` under `sudo` privileges. This guarantees no conflicts with system paths:
```bash
sudo make altinstall
```

Upon completion of the installation, Python 3.11.16 will be located in the `/opt/python3.11.16` directory. Let's verify the correctness of the installation by checking the `python` and `pip` versions using the `--version` parameter:
```bash
/opt/python3.11.16/bin/python3.11 --version
/opt/python3.11.16/bin/pip3.11 --version
``` 

Now the `/tmp` directory can be cleared of the downloaded distribution and temporary build files:
```bash
sudo rm -rf Python-$P_VER.tgz Python-$P_VER
unset P_VER
``` 

At this point, the preparation of Python 3.11.16 is complete, and it is ready for use.

### Creating Isolated Virtual Environments Based on Python 3.11.16

To isolate our project dependencies, we will create `venv` virtual environments based on the newly installed Python 3.11.16. The environments will be located in the user's home directory in the `~/MyProjects` folder, which needs to be created beforehand:
```bash
mkdir -p ~/MyProjects
``` 

The expected directory structure will look like this:
```mermaid
---
config:
  treeView:
    showIcons: true
---
treeView-beta
~/  ## Home folder
    MyProjects/
        AflDbt/    ## AflDbt project with Apache Airflow - will be cloned from GitHub
        aflVe/  ## virtual Python environment for Airflow
        dbtVe/  ## virtual Python environment for dbt
```

Instead of the `MyProjects` directory, you can use any other path within the home directory; however, all subsequent commands will be described with respect to the structure in the diagram above.

Separate virtual environments are created for Airflow and dbt, as these products are incompatible in the same environment due to dependency library requirements:
* `protobuf`: Airflow requires version `4.25.8`, while dbt requires `6.0` to `7.0`.
* `pathspec`: Airflow requires version `1.0.4`, while dbt requires `0.9` to `0.13`.

To create the two virtual environments, we will navigate to the created `MyProjects` directory and create them there as `aflVe` and `dbtVe` using the `venv` module:
```bash
cd ~/MyProjects

# create virtual environment for Airflow
/opt/python3.11.16/bin/python3.11 -m venv aflVe

# create virtual environment for dbt
/opt/python3.11.16/bin/python3.11 -m venv dbtVe
```

We will consider the further process using the `aflVe` environment as an example, as it is specifically intended for running the AflDbt library. To use the created environment, it must be activated:
```bash
# current directory now is ~/MyProjects
source aflVe/bin/activate
```

Let's ensure that the `python` and `pip` commands now point to our new environment, and also update `pip` to the latest version:

```bash
# Check the path and version of python
which python
python --version

# Upgrade pip inside the aflVe venv
pip install --upgrade pip
```

Now the isolated development environment for Airflow is fully ready for the library installation. The virtual environment for dbt will be discussed below.

### Cloning the Repository from GitHub and Running Unit Tests

At this step, we will download the library's source code from the remote GitHub repository to our working directory `~/MyProjects` and verify its functionality. Before starting, ensure that Git is installed on your system and your `aflVe` virtual environment is activated. You can check for Git using the command:
```bash
git --version
```
_Note: If the command returns an error, install Git using your system's package manager (e.g., `sudo apt install git`)._

To test the project, we will need the `pytest` framework, as well as the `pandas` module for the library. Let's install them inside the virtual environment:
```bash
pip install pytest pandas
```

We will clone the AflDbt project into the `~/MyProjects` directory and navigate into it:
```bash
# Navigate to MyProjects directory
cd ~/MyProjects

# Clone the project repository from GitHub
git clone https://github.com/dyacc11/AflDbt.git

# Navigate to the project directory
cd AflDbt
```

Now, let's run the unit tests with the command:
```bash
python -m pytest tests/
```
Upon successful execution, the command will output a report to the terminal. If all tests are marked as passed, this confirms the correctness of the library code in the current environment.

### Verifying Library Operation Using the createMermaid Utility

To verify the correct operation of the AflDbt library, run the two test commands from the `~/MyProjects/AflDbt` directory, which were previously mentioned in the "Unit tests and createMermaid.py utility" section:

**1. Outputting the diagram to the console without dbt tests for the synthetic example used in unit tests:**
```bash
python utils/createMermaid.py tests/simpleJsonTest.json
```

**2. Outputting the full diagram to the console along with dbt tests for the extended synthetic example:**
```bash
python utils/createMermaid.py -t tests/advancedJsonTest.json
```
Both commands should execute without errors. If you copy the text obtained from the console and paste it into any editor or online service for **Mermaid** visualization, the generated diagrams should exactly match the examples provided in the "Unit tests and createMermaid.py utility" section.

### Installing dbt-core (version 1.11) and dbt-sqlite Packages

To install dbt, we need to switch the virtual environment to `dbtVe`. Since we are currently in the `aflVe` virtual environment, we must first exit it:
```bash
# leave aflVe virtual environment
deactivate
```

Now, let's activate `dbtVe`:
```bash
# current directory now is ~/MyProjects/AflDbt, so use full path from home 
source ~/MyProjects/dbtVe/bin/activate
```

Let's ensure that the `python` and `pip` commands now point to our new environment, and also update `pip` to the latest version:
```bash
# Check the path and version of python
which python
python --version

# Upgrade pip inside the dbtVe venv
pip install --upgrade pip
```

We will install `dbt-core` and the `dbt-sqlite` adapter using the `pip` package manager. The `dbt-core==1.11.*` specification guarantees compatibility and the installation of a stable minor release:
```bash
pip install "dbt-core==1.11.*" dbt-sqlite
```

To ensure that the utilities installed correctly and the sqlite adapter is successfully registered in the system, run the command:
```bash
dbt --version
```

The command output should display the versions of the installed packages. Ensure that the sqlite plugin is present in the list of registered adapters:
```text
Core:
  - installed: 1.11.x
Plugins:
  - sqlite: x.xx.x
```
> [!NOTE]
> The command output may also contain the following lines:
> ```text
> Core:
>   - latest:    1.xx.x  - Update available!
>   Your version of dbt-core is out of date!
>   You can find instructions for upgrading here:
>   https://docs.getdbt.com/docs/installation
> ```
> For the AflDbt library, having a later version of dbt is not required. If necessary, you can update dbt by following the instructions at the provided URL.

### Verifying the Correctness of the dbt Project

To verify the correct operation of the `books_library` dbt project, navigate to its directory and run the two test commands previously mentioned in the "A simple dbt project to test the library's functionality" section:

**Navigating to the dbt project directory:**
```bash
cd ~/MyProjects/AflDbt/dbt/books_library
```

**1. Running the models:**
```bash
dbt run
```

**2. Running the tests:**
```bash
dbt test
```

Both commands should execute without errors, and the console logs should match the logs from the commands in the "A simple dbt project to test the library's functionality" section. After the verification is complete, exit the virtual environment:
```bash
# leave dbtVe virtual environment
deactivate
```

In the next section, we will move on to the practical setup of Apache Airflow.

[Apache Airflow Installation and Final Testing of the Test DAG File in Different Modes ->](chap13.md)
