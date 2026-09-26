[<- Back to Table of Contents](index.md)

## Final Data Transformation

We previously mentioned three fields of the `AData` structure into which the data is ultimately transformed: `taskMap`, `taskSequence`, and `groupsData`.
Let us take a closer look at them:

```mermaid
block-beta
    columns 11
    %% === SEQUENCE ===
    block:seq_data:3
    columns 1
    SEQ_N["<b>taskSequence</b>"]

        block:seq_tuple
          columns 2
          SEQ_K(["Key"])  SEQ_T["Tuple"]
          space
          SEQ_T1[/"refATask1"/] space SEQ_T2[/"refATask2"/]
        end
    end
    %% === SPACE === 
    space:1

    %% === TASKS ===
    block:tasks:3
        columns 1
        TSK_T["<b>taskMap</b>"]
        space
        TSK_K(["Key"]) TSK_RF[/"ATask"/]
    end

    %% === SPACE === 
    space:1

    %% === GROUPS ===
    block:groups:4
        columns 1
        GRP_T[" <b>groupsData</b> "] space

        GRP_NM["taskGroupNameMap"]
        GRP_NM_K(["key"])
        GRP_NM_V[/"taskName"/]
        space
        GRP_RM["taskGroupRefMap"]
        GRP_RMK(["key"])
        GRP_RMV[/"refATask list"/]

    end
%% links
SEQ_T1 --> TSK_RF
SEQ_T2 --> TSK_RF
GRP_RMV --> TSK_RF
GRP_NM_V --> TSK_K
%% styles
style seq_data fill:#c6acf2;
style tasks fill:#95C8F0;
style groups fill:#f5b982;
style SEQ_N stroke:#094782,fill:#58f21b
style TSK_T stroke:#094782,fill:#58f21b
style GRP_T stroke:#094782,fill:#58f21b

style GRP_NM fill:#faa5df;
style GRP_RM fill:#faa5df;

style SEQ_T1 fill:#e6f5e6,stroke:#a6d19a,stroke-width:2px
style SEQ_T2 fill:#e6f5e6,stroke:#a6d19a,stroke-width:2px
style TSK_RF fill:#e6f5e6,stroke:#a6d19a,stroke-width:2px
style GRP_RMV fill:#e6f5e6,stroke:#a6d19a,stroke-width:2px

style TSK_K fill:#f3e5f5,stroke:#ba68c8,stroke-width:2px
style GRP_NM_V fill:#f3e5f5,stroke:#ba68c8,stroke-width:2px
```

Task information is stored in `taskMap`, which is a dictionary where the key is the `taskName` and the value is an instance of the `ATask` class. This class will be discussed later. 
A `taskName` can be used to find its corresponding instance.
These instances are referenced by elements of two other dictionaries: `taskSequence` and `taskGroupRefMap`.

The `taskSequence` dictionary has a pair of task names as a key, represented as "|task1name|>>|task2name|", and a tuple of two references to `ATask` class instances: `refATask1` and `refATask2`, as its value.
The `taskSequence` dictionary is then used to create dependencies between Airflow operators, since these dependencies are created using an overloaded bit shift operation.

The second dictionary, `taskGroupRefMap`, has the group name as a key and a list of references to instances of the `ATask` class as a value. It is intended to specify task groups for a set of operators.

In addition to `taskGroupRefMap`, the `groupsData` structure contains another dictionary, `taskGroupNameMap`. This dictionary contains task names rather than references to `ATask` instances, i.e., it actually refers to the `taskMap` keys. It is intended for debugging and search purposes.

To describe this structure in entity-relationship terms, we need to introduce an additional entity `taskGroup` for the group name, which does not exist in the actual classes, but is needed to resolve relationships for `taskGroupNameMap` and `taskGroupRefMap`:

```mermaid
erDiagram
    direction RL
%% === TASK SEQUENCE ===
    taskSequence {
        string sequence_key PK "Format: |name1|>>|name2|"
        object task1_ref FK "Parent ATask object reference"
        object task2_ref FK "Dependent ATask object reference"
    }

    %% === TASK OBJECT ===
    taskMap {
    string name  PK "Task name (Primary identifier)"
        object task_ref UK  "ATask object reference (Value in taskMap)"
    }


    %% === TASK GROUP ===
    taskGroup {
        string group_name PK "Task Group name (Primary identifier)"
    }

    %% === GROUP TO TASK NAME MAPPING ===
    taskGroupNameMap {
        string group_name PK,FK "References taskGroup"
        string name PK,FK "References task (task name)"
    }

    %% === GROUP TO TASK OBJECT REF MAPPING ===
    taskGroupRefMap {
        string group_name PK,FK "References taskGroup"
        object task_ref PK,FK "References ATask object reference"
    }

    %% === RELATIONSHIPS ===
    %% RULE: Optional Sequence (Zero or More)
    %% A task might not have dependencies, so it can exist without being in any sequence.
    taskMap ||--o{ taskSequence : "is parent in (0 or more)"
    taskMap ||--o{ taskSequence : "is dependent in (0 or more)"

    %% RULE: EXACTLY ONE Group Membership for Tasks
    %% Every task MUST belong to exactly one default group (not multiple).
    taskMap ||--|| taskGroupNameMap : "MUST 1:1 taskNames"
    taskMap ||--|| taskGroupRefMap : "MUST 1:1 taskRefs"

    %% RULE: Mandatory Task Membership for Groups (One or More)
    %% A TaskGroup MUST contain at least one task (multiple tasks can belong to one group).
    taskGroup ||--|{ taskGroupNameMap : "has one or more taskNames"
    taskGroup ||--|{ taskGroupRefMap : "has one or more taskRefs"
```

As you can see, the number of rows in the `taskGroupRefMap` and `taskGroupNameMap` tables will correspond to the number of rows in `taskMap`, based on the 1:1 relationship, but in reality, it will be the total number of entries across all groups in the Python structure. There must be at least one group, since a task must belong to a group. A task cannot belong to two or more groups, but multiple tasks can belong to a single group. If there is only one task, there will be no entries in `taskSequence` since a parent-child relationship requires at least two tasks.

Examining how the structure would look using a practical example of a simple dbt project with the `(run)` prefix omitted in `taskName` items:

```mermaid
block-beta
    columns 14
    %% === SEQUENCE ===
    block:seq_data:4
    columns 1
    SEQ_N["<b>taskSequence</b>"]

    block:seq_tuple1
        columns 2
        SEQ_K1(["|books| &gt;&gt; |child_books|"])  SEQ_T1["Tuple"]
        space
        SEQ_T1_1[/"refATask1"/] space SEQ_T2_1[/"refATask2"/]
    end
    block:seq_tuple2
        columns 2
        SEQ_K2(["|child_books|&gt;&gt;|recent_child_books|"])  SEQ_T2["Tuple"]
        space
        SEQ_T1_2[/"refATask2"/] space SEQ_T2_2[/"refATask3"/]
    end  
    block:seq_tuple3
        columns 2
        SEQ_K3(["|books|&gt;&gt;|adult_books|"])  SEQ_T3["Tuple"]
        space
        SEQ_T1_3[/"refATask1"/] space SEQ_T2_3[/"refATask4"/]
        end
    end
    %% === SPACE === 
    space:1
    
    %% === TASKS ===
    block:tasks:2
        columns 1
        TSK_T["<b>taskMap</b>"]
        space
        TSK_K1(["books"]) TSK_RF1[/"ATask1"/]
        TSK_K2(["child_books"]) TSK_RF2[/"ATask2"/]
        TSK_K3(["recent_child_books"]) TSK_RF3[/"ATask3"/]
        TSK_K4(["adult_books"]) TSK_RF4[/"ATask4"/]
    end
    
    %% === SPACE === 
    space:1
    
    %% === GROUPS ===
    block:groups:2
        columns 1
        GRP_T[" <b>groupsData</b> "] 
    space
        GRP_NM["taskGroupNameMap"]
        GRP_NM_K1(["None"])
        GRP_NM_V1_1[/"books"/]  
        GRP_NM_V2_1[/"adult_books"/]
    space
        GRP_NM_K2(["child_books"])
        GRP_NM_V1_2[/"child_books"/]  
        GRP_NM_V2_2[/"recent_child_books"/]  
    space
        GRP_RM["taskGroupRefMap"]
        GRP_RMK1(["None"])
        GRP_RMV1_1[/"refATask1"/]
        GRP_RMV1_2[/"refATask4"/]
    space
        GRP_RMK2(["child_books"]) 
        GRP_RMV2_1[/"refATask2"/]
        GRP_RMV2_2[/"refATask3"/]
    end

%% ---- links ------
SEQ_T1_1 --> TSK_RF1
SEQ_T2_1 --> TSK_RF2
SEQ_T1_2  --> TSK_RF2
SEQ_T2_2 --> TSK_RF3
SEQ_T1_3 --> TSK_RF1
SEQ_T2_3 --> TSK_RF4
GRP_RMV1_1 --> TSK_RF1
GRP_RMV1_2 --> TSK_RF4
GRP_RMV2_1 --> TSK_RF2
GRP_RMV2_2 --> TSK_RF3
GRP_NM_V1_1 --> TSK_K1
GRP_NM_V2_1 --> TSK_K4
GRP_NM_V1_2 --> TSK_K2
GRP_NM_V2_2 --> TSK_K3

%% ---- styles ------
style seq_data fill:#c6acf2;
style tasks fill:#95C8F0;
style groups fill:#f5b982;
style SEQ_N stroke:#094782,fill:#58f21b
style TSK_T stroke:#094782,fill:#58f21b
style GRP_T stroke:#094782,fill:#58f21b

style GRP_NM fill:#faa5df;
style GRP_RM fill:#faa5df;

%% style task 1
style TSK_K1 fill:#f3e5f5,stroke:#ba68c8,stroke-width:2px
style GRP_NM_V1_1 fill:#f3e5f5,stroke:#ba68c8,stroke-width:2px

style SEQ_T1_1 fill:#e6f5e6,stroke:#a6d19a,stroke-width:2px
style SEQ_T1_3 fill:#e6f5e6,stroke:#a6d19a,stroke-width:2px
style TSK_RF1 fill:#e6f5e6,stroke:#a6d19a,stroke-width:2px
style GRP_RMV1_1 fill:#e6f5e6,stroke:#a6d19a,stroke-width:2px

%% style task 2
style TSK_K2 fill:#f3e5f5,stroke:#ba68c8,stroke-width:2px,stroke-dasharray:5 5
style GRP_NM_V1_2 fill:#f3e5f5,stroke:#ba68c8,stroke-width:2px,stroke-dasharray:5 5

style TSK_RF2 fill:#e6f5e6,stroke:#387d24,stroke-width:2px,stroke-dasharray:5 5
style SEQ_T2_1 fill:#e6f5e6,stroke:#387d24,stroke-width:2px,stroke-dasharray:5 5
style GRP_RMV2_1 fill:#e6f5e6,stroke:#387d24,stroke-width:2px,stroke-dasharray:5 5
style SEQ_T1_2 fill:#e6f5e6,stroke:#387d24,stroke-width:2px,stroke-dasharray:5 5

%% style task 3
style TSK_K3 fill:#f3e5f5,stroke:#ba68c8,stroke-width:2px,stroke-dasharray:2 2
style GRP_NM_V2_2 fill:#f3e5f5,stroke:#ba68c8,stroke-width:2px,stroke-dasharray:2 2

style TSK_RF3 fill:#e6f5e6,stroke:#387d24,stroke-width:2px,stroke-dasharray:2 2
style GRP_RMV2_2 fill:#e6f5e6,stroke:#387d24,stroke-width:2px,stroke-dasharray:2 2
style SEQ_T2_2 fill:#e6f5e6,stroke:#387d24,stroke-width:2px,stroke-dasharray:2 2

%% style task 4
style TSK_K4 fill:#f3e5f5,stroke:#ba68c8,stroke-width:4px
style GRP_NM_V2_1 fill:#f3e5f5,stroke:#ba68c8,stroke-width:4px

style TSK_RF4 fill:#e6f5e6,stroke:#387d24,stroke-width:4px
style GRP_RMV1_2 fill:#e6f5e6,stroke:#387d24,stroke-width:4px
style SEQ_T2_3 fill:#e6f5e6,stroke:#387d24,stroke-width:4px

%% group names
style GRP_NM_K1 fill:#9fcad4;
style GRP_NM_K2 fill:#9fcad4;
style GRP_RMK1 fill:#9fcad4;
style GRP_RMK2 fill:#9fcad4;

%% sequences keys
style SEQ_K1 fill:#eaf268;
style SEQ_K2 fill:#eaf268;
style SEQ_K3 fill:#eaf268;
```

The `taskMap` dictionary contains four items, each consisting of a `taskName` key and an `ATask` instance as its value.
These `ATask` instances are referenced by values in the `taskGroupRefMap` and `taskSequence` dictionaries, and the `taskGroupNameMap` dictionary references the `taskMap` keys. In total, `taskGroupRefMap` contains four references to `ATask` instances: two in the `None` group and two in the `child_books` group, which equals the number of elements in `taskMap`. Similarly, `taskGroupNameMap` contains the same keys as the `taskMap` dictionary.

Consider the structure of the helper classes using a class diagram:

```mermaid
classDiagram
    %% === OUTER CLASS ===
    class ATaskGroupProcessor {
        +taskGroupNameMap : dict~str, list~
        +taskGroupRefMap : dict~str, list~
        +taskProcessor : ATaskProcessor
        +__init__(taskMap : dict, groupHldr : AGroupsData, opConf : dict)
        +AddTaskToGroup(taskName : str, groupName : str)
    }
    
    %% === INNER CLASS 1 ===
    class ATaskProcessor {
        +taskMap : dict~str, ATask~
        +opConf : dict
        +__init__(taskMap : dict, opConf : dict)
        +CreateOrGetTask(taskName : str) ATask
    }
    
    %% === INNER CLASS 2 (nested inside ATaskProcessor) ===
    class ATask {
        +taskName : str
        +aflOperator : object
        +projDir : str
        +dbtCmd : str
        +__init__(tName : str, opConf : dict)
        +CreateOperator(tskClbk : callable, groupId : str)
        +GetOperator() object
    }
    
    %% === SUPPORTING CLASSES ===
    class AGroupsData {
        +taskGroupNameMap : dict~str, list~
        +taskGroupRefMap : dict~str, list~
    }
    
    %% === COMPOSITION RELATIONSHIPS (nesting) ===
    ATaskGroupProcessor *-- ATaskProcessor : inner class
    ATaskProcessor *-- ATask : inner class
    
    %% === AGGREGATION (shared references) ===
    ATaskGroupProcessor o-- AGroupsData : references(NameMap,RefMap)
    
    
    %% === DEPENDENCIES ===
    ATask ..> ATaskProcessor : created by CreateOrGetTask()
    ATaskGroupProcessor ..> ATaskProcessor : uses for task creation
    ATaskGroupProcessor ..> ATaskProcessor : pass opConf
    ATaskProcessor ..> ATask : pass opConf
    
    %% === STYLING ===
    style ATaskGroupProcessor fill:#e1f5fe,stroke:#0288d1,stroke-width:3px
    style ATaskProcessor fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style ATask fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style AGroupsData fill:#f3e5f5,stroke:#9c27b0,stroke-width:2px
```
Within the `ATaskGroupProcessor` helper class, there is another nested helper class, `ATaskProcessor`, and within that, another nested helper class, `ATask`, mentioned earlier.
The classes are defined as nested because they are not intended to be used separately as library classes.

The diagram shows that the configuration reference is passed sequentially from the `ATaskGroupProcessor` class to the `ATaskProcessor` class and ultimately to the `ATask` class.

It makes sense to start with the `ATask` class when considering the purpose of this entire set of helper classes.
The purpose of the `ATask` class can be outlined roughly as follows:

```mermaid
block-beta
    columns 5
    block:In_data
        columns 1
        IPN["<b>Input parameters</b>"]
        IP1(["task Name"]) 
        IP2(["Configuration"]) 
    end
    block:Arrow_Left
        columns 1
        space
        arrowLD<["Pass to __init__"]>(right) 
        space
    end
    block:AT
        columns 1
        AT_c["<b>ATask</b>"]
        AT_d["dbt command"]
        space
        AT_a["aflOperator"]

    end
    block:Arrow_Right
        columns 1
        space
        arrowR1<["CreateOperator"]>(right)
        space
        arrowR2<["operator reference"]>(left)
    end
    
    block:Afl
        columns 1
        AFL_T1["<b>Airflow </b>"]
        AFL_T2["<b>Bash </b>"]
        AFL_T3["<b>Operator</b>"]
    end  

classDef n0Fill fill:none,stroke-width:0px;
class IPN,Arrow_Left,Arrow_Right,AT_c n0Fill
class AFL_T1,AFL_T2,AFL_T3 n0Fill

style In_data fill:#c6acf2;
style arrowLD fill:#c6acf2;

style AT fill:#95C8F0;
style arrowR1 fill:#95C8F0;

style Afl fill:#f5b982;
style arrowR2 fill:#f5b982;
style AT_a fill:#f5b982;

```

The class takes a task name and configuration as input and generates a `dbt` call string for a model or test.
This string is then used in the `BashOperator` as a command.
The class creates a `BashOperator` by calling the `CreateOperator` method, which accepts a callback and task group ID as parameters. It stores a reference to the created Airflow operator object (which was passed the command to execute) in the `aflOperator` field. This reference to the Airflow operator object is returned by the `GetOperator` method.
Using the `CreateOperator`/`GetOperator` methods of the `ATask` class will be discussed in the next section.

The `ATask` class instance itself is created without being associated with a group or sequence, and only reflects the execution of a command.
However, it is not created through standard initialization, but by calling a method of the class factory: the helper class `ATaskProcessor`.

The `ATaskProcessor` class takes two parameters in its constructor: `taskMap` for a dictionary of `ATask` instances and `opConf`, a reference stored in a class field, to be passed to the `ATask` constructor when instantiating it.

The `CreateOrGetTask` method of the `ATaskProcessor` class ensures that tasks in the `taskMap` container are unique. This means that if we attempt to create a task with the same name again, the method does not write anything to the `taskMap` container and returns the already created task by name. This check is necessary because if a DAG has branching, different paths in the graph may contain the same tasks and even task groups, but within the DAG itself, tasks are unique.

Now that the underlying helper classes have been described, we can examine the logic of the `AddTaskToGroup` method of the `ATaskGroupProcessor` class:

```mermaid
sequenceDiagram
    participant Caller as AGraphPathProcessor
    participant ATGP as AddTaskToGroup()
    participant TP as ATaskProcessor
    participant TGNM as taskGroupNameMap
    participant TGRM as taskGroupRefMap
    
    Caller->>ATGP: AddTaskToGroup(taskName, groupName)
    
    Note over ATGP: Get or create task reference
    ATGP->>TP: CreateOrGetTask(taskName)
    TP-->>ATGP: taskRef
    
    Note over ATGP: Check if group exists
    ATGP->>TGNM: get(groupName)
    TGNM-->>ATGP: retGroupList (or None)
    
    alt retGroupList is None (group doesn't exist)
        Note over ATGP: Create new group
        ATGP->>TGNM: [groupName] = []
        ATGP->>TGRM: [groupName] = []
    end
    
    Note over ATGP: Check if task already in group
    ATGP->>ATGP: if taskName is in group
    
    alt taskName not in group
        Note over ATGP: Add task to group
        ATGP->>TGNM: [groupName].append(taskName)
        ATGP->>TGRM: [groupName].append(taskRef)
    end
    
    ATGP-->>Caller: return
```

As you can see, the method delegates task creation to the `ATaskProcessor` factory and first creates the task passed in as an argument or obtains a reference to an existing one. Then, the group's existence is checked; if it doesn't exist, it is created.
For this check, one of the dictionaries, `taskGroupNameMap`, is sufficient. It is also used to verify the presence of a task in the list by the group name key.

New keys for the group name, or `None` for the default group, are created synchronously in both dictionaries, `taskGroupNameMap` and `taskGroupRefMap`. Elements are also added to both dictionaries simultaneously: the task name in `taskGroupNameMap` and a reference to the `ATask` class instance in `taskGroupRefMap`.

The `taskGroupNameMap` dictionary here serves the additional function of checking the existence of a task in a group, since a similar check using `taskGroupRefMap` would require retrieving the task name by accessing a class field, which would complicate the code.

Now we can examine the `_AddSequence` method of the `AGraphPathProcessor` class, which was not described in the previous section.
Since `AGraphPathProcessor` contains a reference to the `taskMap` dictionary, we can write data to the `taskSequence` dependency dictionary. The key is a string composed of the parent and dependent task names, and the value is a tuple of two `ATask` instance references. The references to the `ATask` instances are obtained using the task names as keys in the `taskMap` dictionary.

The `_AddSequence` method accepts the names of two tasks as arguments: the parent `task1name` and the dependent `task2name`.
Inside the `_ProcessTasksList` method, the name of the previous task is stored, which is passed to `task1name`, and the current task is passed to `task2name`. Then, at the end of each iteration, the previous task is assigned the value of the current task, allowing the dependency to be built. 
The previous task is initialized as `None`. Therefore, the first iteration is skipped by the `_AddSequence` method, which requires both arguments to be non-`None`, since dependencies are built only between real tasks.

Similarly, uniqueness is maintained in the `taskSequence` dictionary; if the same dependency already exists for a given key, nothing is added.

In the next section, we will review the use of this final data in callback invocations.

[Airflow Objects Generation ->](chap8.md)
