[<- Back to Table of Contents](index.md)

## Core Concept

dbt generates a `manifest.json` file. This file is created in the `target` directory of your dbt project and contains a complete representation of the project. It provides all the necessary metadata (models, sources, tests, dependencies, etc.) needed to create an Airflow DAG for dbt.

The architecture and logic are designed for dynamic generation of Apache Airflow DAGs based on the dbt manifest. They automatically transform dbt project metadata into Airflow workflows.

- The **Configuration** object stores the path to the dbt manifest file.
- The **Processing Logic** functionality is initialized with this Configuration object in the constructor, which provides it access to the manifest path and other settings.
- **Callback functions** are passed as parameters to the `Process` method of the Processing Logic instance. They define simple rules for creating Airflow components based on the provided arguments.

These callback functions create the main types of Airflow components:
- The DAG object itself
- Task groups
- Tasks
- Dependencies between tasks (e.g., `task_a >> task_b`)

The `Processing Logic` functionality transforms metadata from the manifest file into input parameters for these callback functions. The term "functionality" is used because there is no actual Python class named `Processing Logic`, and the internal structure will be detailed further.

The Python DAG file using this library includes the following objects:
- The Configuration object (which is a dictionary)
- Callback functions for creating Airflow components
- An instance of the `Processing Logic` functionality

This can be illustrated with the following simplified diagram:
```mermaid
block-beta
    columns 14
%% ====== INPUT DATA ==========
    block:dbtfile:4
        columns 1
    DBT_T["Input data"]
    space:1
        DBT("dbt manifest file")
        space:1
    end

    space:1
%% ====== LOGIC ==========
    block:Acts:4
        columns 1
        Actions_T["Processing Logic"]
        space
        ACT_INIT["1.Constructor (Configuration)"]
        ACT_DO["2.Process (Callbacks) method"] 
        ACT_CFG["3.Convert metadata\n from the manifest file"]
        ACT_CALL["4.Call callbacks with params"]
        space:3
    end

    space:1 
%% ====== DAG ==========
    block:DAG:6
       columns 1
       DAG_T["Airflow DAG py file"]

       CFG("Configuration\n(has dbt full path)")
       space
       CLS_I["Processing Logic instance"]
       space 
       CLBKS["Callbacks for Airflow \nObjects Creation"]
       space:1

       dagobj_T1[/"Airflow Objects"/]
       dagobj_DAG["DAG"]
       dagobj_TG["Task Groups"] 
       dagobj_TSK["Tasks"] 
       dagobj_SEQ["Task dependencies"] 

    end

%% ====== links ==========
CLS_I --- Acts
CLBKS-- "5.Create" -->dagobj_T1
ACT_CFG --> DBT
ACT_CFG --> CFG
ACT_INIT -.- CFG
ACT_DO -.- CLBKS
ACT_CALL --> CLBKS

%% ====== styles ==========
classDef blckDash stroke:#000000 ,stroke-width:2px,stroke-dasharray: 5 5;
classDef n0Fill fill:none,stroke-width:0px;
classDef dblBrd stroke:#000000 ,stroke-width:2px,border: 6px
classDef prcLgc stroke:#094782,fill:#95C8F0
classDef aflObj stroke:#094782,fill:#58f21b

class Acts blckDash
class DBT_T,Actions_T,DAG_T,clsInst_T,dagobj_T n0Fill 
class AJP dblBrd
class Acts,CLS_I prcLgc 
class dagobj_T1,dagobj_DAG,dagobj_TG,dagobj_TSK,dagobj_SEQ aflObj 
style dbtfile fill:#c6acf2;
style DAG fill:#f5b982;
```

The `Processing Logic` implementation and auxiliary modules are located in a separate directory.

In general, to create a DAG file based on the dbt manifest, you need to:
- Set up the configuration
- Define callback functions for creating Airflow components
- Create an instance of the class for implementing `Processing Logic` and pass the configuration during creation
- Run the `Process` method and pass the callback functions as input parameters

In the next section, the internal architecture of `Processing Logic` is examined in more detail.

[Basic Internal Architecture of the Processing Logic Class ->](chap2.md)
