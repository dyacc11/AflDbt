[<- Back to Table of Contents](index.md)

## Airflow Objects Generation

After we have completed the final data transformation, we can now use it to create Airflow objects.
But first, let us examine the `AData` class in its entirety in relation to the `ATask` class.

The `AData` class can be represented by the following class diagram:

```mermaid
classDiagram
    %% === OUTER CLASS ===
    class AData {
        +baseData : ABaseData
        +taskMap : dict~str, ATask~
        +taskSequence : dict~str, tuple~
        +groupsData : AGroupsData
        +__init__(strDbtProcessTests : str = "True")
        +CreateAirflowObjects(dgClbk : callable, grClbk : callable, tskClbk : callable, seqClbk : callable)
    }

    %% === INNER CLASS 1 ===
    class ABaseData {
        +projectName : str
        +dbtProcessTests : bool
        +fullModels : dict~str, str~
        +fullModelsRev : dict~str, str~
        +chldCount : dict~str, int~
        +prntCount : dict~str, int~
        +__init__(strDbtProcessTests : str = "True")
    }

    %% === INNER CLASS 2 ===
    class AGroupsData {
        +taskGroupNameMap : defaultdict~list~
        +taskGroupRefMap : defaultdict~list~
        +__init__()
    }

    %% === REFERENCED CLASS ===
    class ATask {
        +taskName : str
        +aflOperator : object
        +CreateOperator(tskClbk : callable, groupId : str)
        +GetOperator() object
    }

    %% === NESTING (inner classes) ===
    AData *-- ABaseData : inner class(self.baseData)
    AData *-- AGroupsData : inner class(self.groupsData)

    %% === REFERENCES ===
    AData ..> ATask : taskMap values
    ATask ..> AData : used in CreateAirflowObjects

    %% === STYLING ===
    style AData fill:#e1f5fe,stroke:#0288d1,stroke-width:3px
    style ABaseData fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style AGroupsData fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style ATask fill:#f3e5f5,stroke:#9c27b0,stroke-width:2px
```

The `AData` class has two inner classes: `ABaseData` and `AGroupsData`.

The `ABaseData` class instance, discussed earlier, is stored in the `baseData` variable, while the `AGroupsData` class instance is stored in the `groupsData` variable.
After the final transformation, all the data we need is located in the following fields of the `AData` class:

* project name in `baseData.projectName`
* tasks, organized by group, in `groupsData.taskGroupRefMap`
* sequences in `taskSequence`
* task dictionary in `taskMap`

The last three fields, which are dictionary containers, use a reference to an instance of the `ATask` class as their value.

The `CreateAirflowObjects` method of the `AData` class is used to create Airflow objects.
It takes four callbacks as arguments:
* `dgClbk`: DAG creation callback
* `grClbk`: Task Group creation callback
* `tskClbk`: Task creation callback
* `seqClbk`: Task sequence creation callback

The process of creating Airflow objects can be graphically represented with the following schematic diagram:

```mermaid
block-beta
    columns 9

    %% ===  INPUT DATA ===
    block:input_data:3
        columns 1
        ID_T["AData - Input Data Structures"]
        
        block:baseData_block
            columns 1
            BD_T["baseData"]
            BD_PN["projectName"]
        end
        block:groupsData_block
            columns 1
            GD_T["groupsData - taskGroupRefMap "] 
            GD_K(["group (Key)"]) GD_V[/"[ATask list]"/]
        end
        block:taskSequence_block
            columns 1
            TS_T["taskSequence"] TS_V[/"(ref1, ref2)"/]
        end
        block:taskMap_block
            TM_T["taskMap"] 
            columns 1
            TM_V[/"ATask"/]
        end

    end

    space:1

    %% === PROCESSING  ===
    block:processing_output:3
        columns 1
        PO_T["CreateAirflowObjects - Processing "]
        block:phase1
            columns 1
            P1_T["Phase 1: DAG Creation"] 
            P1_IN["Input: projectName"]
            P1_CB["dgClbk(projectName)"]

        end
        block:phase2
            columns 1
            P2_T["Phase 2: Groups "] 
            P2_IN["Input: taskGroupRefMap keys"]
            P2_CB["grClbk(groupId)"]

        end
        block:phase3
            columns 1
            P3_T["Phase 3: Tasks"] 
            P3_IN["Input: taskGroupRefMap values"]
            P3_TSK["task.CreateOperator(tskClbk, groupId)"]
        end
        block:phase4
            columns 1
            P4_T["Phase 4: Sequences"] 
            P4_IN["Input: taskSequence"]
            P4_OPUSE["Use operators for tasks"]
            P4_SEQ["seqClbk(ref1.GetOperator(),\n         ref2.GetOperator())"]

        end
    end
    %% === OUTPUTS ===
    space:1
    block:output:4
       columns 1
       O_T["CreateAirflowObjects - Output"]
          P1_OUT["DAG object"]
          space:1  
          OO_TG["TaskGroups"]
          space:1
          OO_OP["Operators"]
          space:1
          OO_DEP["Dependencies"]
    end
    %% === DATA FLOW ARROWS (Top to Bottom) ===
    BD_PN --> P1_IN
    GD_K --> P2_IN
    GD_V --> P3_IN
    TS_V --> P4_IN
    TM_V <--> P4_OPUSE

    P1_CB --> P1_OUT
    P2_CB --> OO_TG
    P3_TSK <--> OO_OP
    P3_TSK -->  TM_V 
    P4_SEQ --> OO_DEP

    %% === STYLING ===
    style input_data fill:#EAF4FF,stroke:#0288d1,stroke-width:3px
    style ID_T fill:none,stroke:#0288d1,stroke-width:0px

    style processing_output fill:#fff3e0,stroke:#f57c00,stroke-width:3px
    
    classDef n1col fill:#bbdefb,stroke:#1976d2,stroke-width:2px
    class baseData_block,groupsData_block,taskSequence_block,taskMap_block n1col

    classDef n1colNoFill fill:none,stroke:#1976d2,stroke-width:0px
    class BD_T,GD_T,TS_T,TM_T n1colNoFill

    classDef n2col fill:#ffe0b2,stroke:#f57c00,stroke-width:2px
    class phase1,phase2,phase3,phase4 n2col 
    
    classDef n2colNoFill fill:none,stroke:#f57c00,stroke-width:0px;
    class PO_T,P1_T,P2_T,P3_T,P4_T n2colNoFill

    style output fill:#B9E0A5,stroke:#009900,stroke-width:3px
    style O_T fill:none,stroke:#009900,stroke-width:0px
```

Based on the number of callbacks, the process can be divided into four phases:
* creation of the DAG object itself
* creation of task groups
* creation of tasks
* creation of dependencies

Obviously, the phases are interdependent. This is why the DAG is created first, then the task group is created. If we want to add a task to the group, it must first be created so that it can be specified as an argument during creation. The final phase is the creation of dependencies, since the tasks must be created first.

The user implements callbacks in their DAG code and passes references to methods. Using callbacks allows for flexible functionality, as this library uses only the following callback signatures:

* `dgClbk`: has only one argument, projectName. The function returns nothing.
* `grClbk`: has only one argument, groupId. In Airflow, the name of the task group is its identifier. The function returns nothing.
* `tskClbk`: has three arguments: taskName for the task name, execStr for the execution string, and groupId for the task group to which the task belongs. The function must return a reference to the created operator object.
* `seqClbk`: has two arguments: a reference to the Airflow task for the parent and a reference of the same type for the dependent task. The function returns nothing.

Creating a DAG requires more arguments, such as a schedule, start date, and possibly additional arguments beyond the project name. This is implemented in the custom code of this callback. Based on the `dbtProcessTests` flag, the project name is passed in its original form or suffixed with "with Tests" if the flag specifies the use of both models and tests.

When creating a task group, the groupId is also sufficient.
However, creating the task itself is a bit more involved: the callback call is delegated to the `CreateOperator` method of the `ATask` class, which invokes the callback, passing in the arguments. The callback for the task returns the created Airflow object, and a reference to it is stored in the `aflOperator` field of the `ATask` class. This is done to implement dependencies between Airflow objects: the `taskSequence` dictionary contains tuples with references to `ATask` instances. The `ATask` class stores references to Airflow objects during phase 3, so it can invoke the callback for sequences with arguments in the form of references to Airflow objects and define dependencies between them.

We can visualize this logic in a sequence diagram:

```mermaid
sequenceDiagram
    participant Caller as Airflow objects creation
    participant AD as AData
    participant GD as groupsData
    participant AT as ATask
    participant TS as taskSequence
    participant CB as Callbacks

    Caller->>AD: CreateAirflowObjects(dgClbk, grClbk, tskClbk, seqClbk)

    %% === PHASE 1: DAG CREATION ===
    Note over AD: Phase 1: Create DAG
    AD->>CB: dgClbk(projectName)
    Note over CB: Creates Airflow DAG
    CB-->>AD: DAG created

    %% === PHASE 2: GROUPS AND TASKS ===
    Note over AD,GD: Phase 2: Create Groups
    loop for each group in taskGroupRefMap
        AD->>CB: grClbk(groupId)
        Note over CB: Creates TaskGroup
        CB-->>AD: TaskGroup created 

        loop for each task in group
            Note over AD,AT: Phase 3: Create Tasks per a group
            AD->>AT: CreateOperator(tskClbk, groupId)
            AT->>CB: tskClbk(taskName, execStr, groupId)
            Note over CB: Creates Operator
            CB-->>AT: operator reference
            AT-->>AD: operator stored
        end
    end

    %% === PHASE 3: SEQUENCES ===
    Note over AD,TS: Phase 4: Create Dependencies
    loop for each sequence in taskSequence
        AD->>AT: GetOperator() from both tasks
        AT-->>AD: operator1, operator2
        AD->>CB: seqClbk(operator1, operator2)
        Note over CB: Execute: operator1 >> operator2
        CB-->>AD: Sequence created
    end

    AD-->>Caller: All Airflow objects created
```

In the first phase, an Airflow DAG is created.
The second phase is a loop over the keys of the `taskGroupRefMap`: this is how we select a group. First, we invoke a callback to create an Airflow task group object. For the `None` group, a task group should not be created in the callback.
After the group is created, a nested loop for the third phase begins: iterating over the `ATask` instances of this group. Here, we create tasks, specifying which group they belong to, and store a reference to them.
In the final, fourth phase, we iterate over the `taskSequence` values, obtaining references to the tasks from each element of the pair that makes up the tuple. It is this pair of task references that is passed to the callback to create dependencies after the tasks themselves have been created.

### AMain Class

Since the entire logic of the solution has already been described in sufficient detail, the description of the `AMain` class can be limited to a class diagram:

```mermaid
classDiagram
    %% === MAIN ORCHESTRATOR ===
    class AMain {
        +data : AData
        +jProc : AJsonProcessor
        +gProc : AGraphPathProcessor
        +__init__(dbtData: dict)
        +Process(dgClbk : callable, grClbk : callable, tskClbk : callable, seqClbk : callable)
    }
    
    %% === DATA CONTAINER ===
    class AData {
      +CreateAirflowObjects(dgClbk, grClbk, tskClbk, seqClbk)
    }
    
    %% === PROCESSORS ===
    class AJsonProcessor {
        +Process()
    }
    
    class AGraphPathProcessor {
        +ProcessPath()
    }
    
    
    %% === AGGREGATION (shared references) ===
    AMain *-- AData : data
    AMain *-- AGraphPathProcessor : gProc
    AMain *-- AJsonProcessor: jProc
    
    %% === DEPENDENCIES ===
    AMain ..> AJsonProcessor : uses for parsing

    AMain ..> AGraphPathProcessor : uses for group/task building

    AJsonProcessor ..> AData : populates
    AGraphPathProcessor ..> AData : populates
    AJsonProcessor ..> AGraphPathProcessor : path

    
    %% === CALLBACKS (external) ===
    class Callbacks {
        <<DAG-specific>>
        +dgClbk(projectName : str)
        +grClbk(groupId : str)
        +tskClbk(taskName : str, execStr : str, groupId : str)
        +seqClbk(fromTask, toTask)
    }
    
    AMain ..> AData : CreateAirflowObjects
    AData ..> Callbacks : CreateAirflowObjects
    
    %% === STYLING ===
    style AMain fill:#ffebee,stroke:#c62828,stroke-width:3px
    style AData fill:#e1f5fe,stroke:#0288d1,stroke-width:3px
    style AJsonProcessor fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style AGraphPathProcessor fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style Callbacks fill:#f3e5f5,stroke:#9c27b0,stroke-width:2px,stroke-dasharray:5 5
```

The `AMain` class acts as the central coordinator of the previously described workflow phases. It contains:
* an `AData` class instance for storing data as it's transformed
* an `AJsonProcessor` class instance for reading the project's dbt file
* an `AGraphPathProcessor` class instance for parsing the read paths

The `AMain` class constructor accepts a configuration reference as its only parameter.
The `Process` method accepts four callbacks for creating Airflow objects.

Internally, it reads the dbt project using the `AJsonProcessor` class. After processing, the output list of graph paths is passed in a loop to the `AGraphPathProcessor` class, which completes the final transformation. After that, the Airflow object creation method is called on `AData`.
Thus, the Python file for DAG, excluding imports, becomes quite simple, as illustrated by the following example:

```python
# ============================================================================
# Global variables to hold Airflow objects created by callbacks
# ============================================================================
_dag = None
_groups = {}

# ============================================================================
# CALLBACK FUNCTIONS
# ============================================================================

# *** DAG creation ***
def CreateDagCallback(projectName):
    global _dag

    _dag = DAG(
        dag_id=f"dbt_{projectName}",
        start_date=datetime(2026,1,1),
        schedule= None,
        catchup=False,
        description=f"Auto-generated DAG from dbt project: {projectName}"
    )

# *** Task Group creation ***
def CreateTaskGroupCallback(groupId):
    global _dag
    global _groups

    # add only a group that is not the default group
    if groupId is not None:
        grp = TaskGroup(
            group_id=groupId,
            dag=_dag,
            tooltip=f"Task group: {groupId}"
        )
        _groups[groupId] = grp

# *** Task creation ***
def CreateTaskCallback(taskName, execStr, groupId):
    global _dag
    global _groups

    # Clean task name for Airflow (remove special characters)
    task_id = taskName.replace(" ", "_").replace("(", "").replace(")", "")

    # Get the TaskGroup for this task, if one exists.
    grp = None
    if groupId is not None:
        grp = _groups.get(groupId)

    task = BashOperator(
        task_id=task_id,
        bash_command=execStr,
        dag=_dag,
        task_group=grp,
    )

    return task
    
# *** Sequence creation ***
def CreateSequenceCallback(from_task, to_task):
    from_task >> to_task

# ============================================================================
# MAIN DAG DEFINITION
# ============================================================================

# *** Configuration ***
dbtData = {
 "DBT_PROJECT_DIR":"/my/project/dir",
 "DBT_COMMAND":"/place/of/dbt",
 "DBT_MANIFEST_PATH":"/my/project/dir/target/manifest.json",
 "SKIP_DBT_TEST":"True"
}

# *** AMain instance ***
mainProcessor = AMain(dbtData)

# *** Run process ***
mainProcessor.Process(
  dgClbk=CreateDagCallback,
  grClbk=CreateTaskGroupCallback,
  tskClbk=CreateTaskCallback,
  seqClbk=CreateSequenceCallback 
)
```

Then we will look at an example dbt project used to illustrate the operation of this simple library.
Some caveats of creating a DAG file will be discussed further in a separate section.

[A Simple dbt Project to Test the Library's Functionality ->](chap9.md)
