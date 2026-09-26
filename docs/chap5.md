[<- Back to Table of Contents](index.md)

## AGraphPathProcessor Class Overview

Previously, we looked at the `AJsonProcessor` class, which receives a dbt `manifest.json` file and converts it into an `ABaseData` structure and a list of graph paths.
The next step in the data transformation pipeline is the `AGraphPathProcessor` class, which identifies groups and members in a graph path and outputs the results to an `AData` structure:

```mermaid
block-beta
    columns 5
    block:dbtfile
        columns 1
        DBT["dbt manifest"] 
        space
        CFG(["Configuration"])
    end

    block:JS_ARW
        columns 1
        arrowDBT<["JSON data"]>(right)
        space:2
    end
    
    block:AJP
       columns 1
       JS_P["<b>AJsonProcessor</b>"]
       space
       ABDT["ABaseData"]
    end

    block:AJP_ARW
        columns 1
        arrowPD<["Path data"]>(right)
        space:2
    end

    block:APP
       columns 1
       PP["<b>AGraphPathProcessor</b>"]
       space
       ADT["AData"]
    end

classDef n0Fill fill:none,stroke-width:0px;
class JS_ARW,AJP_ARW n0Fill 
style dbtfile fill:#c6acf2;
style AJP fill:#95C8F0;
style APP fill:#f5b982;
style arrowDBT fill:#c6acf2;
style arrowPD fill:#95C8F0;

%% links
CFG --> JS_P
JS_P --> ABDT
PP --> ADT
CFG --> PP
ABDT --> PP
```

To accomplish this, the class has a `ProcessPath` method that accepts a single path as an input parameter.
The class itself uses only two dictionaries from the `ABaseData` structure: `chldCount` and `prntCount`.
The output consists of three containers stored in the `AData` structure: `taskMap`, `groupsData`, and `taskSequence`.

```mermaid
block-beta
    columns 5
    block:In_data
       columns 1
       IP["Single Graph Path"]
       
       block:In_ABdata
           columns 1 
            ABD["<b>ABaseData</b> subclass"]
            ABD_v["chldCount<br/>prntCount"]
       end 
    end
    block:Arrow_Left
       columns 1
       arrowLU<["pass as a call argument"]>(right)
       space
       arrowLD<["Read-Only reference"]>(right)
    end
    block:AGP
       columns 1
       AGP_c["<b>AGraphPathProcessor</b>"]
       AGP_m["ProcessPath method"]
    end
    block:Arrow_Right
     columns 1
     space
     arrowR1b<["calculated data"]>(right)
    end
    block:AData_Left
       columns 1
       ADT["<b>AData</b>"]
       Recie["taskMap<br/>groupsData<br/>taskSequence"]
    end

classDef n0Fill fill:none,stroke-width:0px;
class ABD,Arrow_Left,Arrow_Right,ADT,AGP_c n0Fill

style In_data fill:#95C8F0;
style arrowLU fill:#95C8F0;
style arrowLD fill:#95C8F0;
style AGP fill:#d5e8d4;
style arrowR1b fill:#d5e8d4;
style AData_Left fill:#f5b982;
```

Let us consider the general problem of identifying groups from a path.

Assume that we have the same example as before:

```mermaid
flowchart LR
    A["(run) books"]
    B["(run) child_books"]
    C["(run) adult_books"]
    D["(run) recent_child_books"]
    A-->B
    A-->C
    B-->D
```

In this case, a group can be defined as a sequence of tasks executed without branching, namely, the tasks `(run) child_books` and `(run) recent_child_books`, which can be combined into a group and named after the first task in the execution chain:

```mermaid
flowchart LR
    A["(run) books"]
    C["(run) adult_books"]
    subgraph child_books group
        B["(run) child_books"]
        D["(run) recent_child_books"]
    end
    A-->B
    A-->C
    B-->D
```

Indeed, in many Airflow examples, a group is defined by a sequence of tasks performed without branching, i.e., sharing a common logical purpose:

```mermaid
flowchart LR
    CT1["common task start"]
    CT2["common task end"]
    subgraph "New implementation case"
        N1["run new action 1"]
        N2["run new action 2"]
    end

    subgraph "Old implementation case"
        O1["run old action 1"]
        O2["run old action 2"]
    end

    CT1-->N1
    N1-->N2
    N2-->CT2
    CT1-->O1
    O1-->O2
    O2-->CT2
```

This project uses exactly this logic of subgroup selection, namely, a sequence of tasks executed without branching.
How can groups be identified on a given graph path? To do this, we simply need to add the number of parents to the left of each task and the number of children to the right of each task to our diagram:

```mermaid
flowchart LR
    IF@{ shape: sm-circ, label: "" }
    E1@{ shape: sm-circ, label: "" }
    E2@{ shape: sm-circ, label: "" }
    A["(run) books"]
    C["(run) adult_books"]
    subgraph child_books group
        B["(run) child_books"]
        D["(run) recent_child_books"]
    end
    A-->|" chld 2 >  < 1 prnt "| B
    A-->|" chld 2 >  < 1 prnt "| C
    B--> |"chld 1 >  < 1 prnt"| D
    IF--- |" < 0 prnt "| A
    D--- |" chld 0 > "| E1
    C---|" chld 0 > "| E2
```

The notation |"chld 2 > < 1 prnt"| means that the node to the left of the connecting line has two children, and the node to the right has one parent.
This diagram clearly shows the condition under which we combine tasks into a group: the task on the left has only one child, and the task on the right has only one parent.

In fact, this DAG contains two paths. Therefore, the `ProcessPath` method will first be called for this path:

```mermaid
flowchart LR
    IF@{ shape: sm-circ, label: "" }
    E1@{ shape: sm-circ, label: "" }
    A["(run) books"]
    subgraph child_books group
        B["(run) child_books"]
        D["(run) recent_child_books"]
    end
    A-->|" chld 2 >  < 1 prnt "| B
    B--> |"chld 1 >  < 1 prnt"| D
    IF--- |" < 0 prnt "| A
    D--- |" chld 0 > "| E1
```

Then, for this path:

```mermaid
flowchart LR
    IF@{ shape: sm-circ, label: "" }
    E2@{ shape: sm-circ, label: "" } 
    A["(run) books"]
    C["(run) adult_books"]
    A-->|" chld 2 >  < 1 prnt "| C
    IF--- |" < 0 prnt "| A
    C---|" chld 0 > "| E2
```

Within each path, each task formally has only one child and one parent.
But we use the number of parents and children from the entire DAG, which allows us to identify real chains with one parent node and one child node.

Thus, the first path contains a subchain with a group, and such a subchain can only exist within a single path. The second path, however, does not contain a subchain with a group.

In the example above, the group ends at the end of the path.
Let us add two more tasks to that example: filter recent children's books to identify unique authors, and then look for the same authors across both children's and adult books.
The diagram will then look like this:

```mermaid
flowchart LR
    IF@{ shape: sm-circ, label: "" }
    E1@{ shape: sm-circ, label: "" }
    A["(run) books"]
    C["(run) adult_books"]
    subgraph child_books group
        B["(run) child_books"]
        D["(run) recent_child_books"]
        I["(run) recent_child_author"]
    end
    F["(run) same_author"]
    A-->|" chld 2 >  < 1 prnt "| B
    A-->|" chld 2 >  < 1 prnt "| C
    B--> |"chld 1 >  < 1 prnt"| D
    IF--- |" < 0 prnt "| A
    D--> |"chld 1 >  < 1 prnt"| I
    I--- |" chld 1 >  < 2 prnt "| F
    C---|" chld 1 >  < 2 prnt "| F
    F--- |" chld 0 > "| E1
```

As can be clearly seen, the start of a group can be identified as the first task with one child and the following task with one parent. The end of a group is identified as the last task after which this condition is false or the path ends.

Such an algorithm can be implemented as a two-state finite-state machine. A transition to the identified group state occurs when the specified condition is true, and a transition to the opposite state occurs when the inverse condition is true.

```mermaid
stateDiagram
  direction LR
    [*] --> NoActiveGroup : Initial state<br/>(No active group)
    NoActiveGroup: No group identified 
    ActiveGroup: Has active group 

    NoActiveGroup --> NoActiveGroup : Condition not met 

    NoActiveGroup --> ActiveGroup: 1:1
    ActiveGroup --> NoActiveGroup : NOT 1:1

    ActiveGroup --> ActiveGroup : Condition not met   

    style NoActiveGroup fill:#a5b2f0;
    style ActiveGroup fill:#95C8F0;  
```
To implement this state machine, we need a current task and a subsequent task. More precisely, we only need to know the current task's number of children. If it has no children, then considering the subsequent task is unnecessary: we can simply take the number of parents of the subsequent task as zero. If a subsequent task exists, then we take its number of parents. 
The current group name, `curGroup`, serves as a variable that stores the state of the state machine between iterations. If the current state is "No group identified", the variable is `None`; otherwise, it is set to the group name. The group name can be formed from the task name by removing the operation prefix, such as `(test) ` or `(run) `.
The `curGroup` variable is defined before the loop over the path elements to store state between iterations.
Since the transition "No group identified" -> "Has active group" is only meaningful when there is no current group, i.e., `curGroup` is `None`, this condition should be added to the transition condition. Similarly, the inverse condition on `curGroup` should be added to the inverse transition condition "Has active group" -> "No group identified".

To ensure that a task, which determines a group based on the data of a subsequent task, is included in the group, a state switch must be made after identification but before the group-task pair is selected. And to ensure that the second, subsequent task is included in the group, if there is no further group, a switch to the opposite state must be made after the group-task pair is selected.

This results in the following diagram for a single iteration:

```mermaid
flowchart LR
    S@{ shape: circle, label: "Start" }

    subgraph IF_B["if-b"]
        direction TB
        IF_B_T{"check<br/>curGroup is None<br/>and one child<br/>and the following task<br/>with one parent"}
        CG_S["set curGroup"]
    end

    subgraph SEL["action"]
        direction TB
        SEL_T["select group/task"]
        OUT["out data"]
    end

    subgraph IF_A["if-a"]
        direction TB
        IF_A_T{"check<br/>NOT curGroup is None<br/>and one child<br/>and the following task<br/>with one parent"}
        CG_R["reset curGroup"]
    end

    E@{ shape: circle, label: "End" }

    S --> IF_B
    IF_B --> SEL
    SEL --> IF_A
    IF_A --> E
    SEL_T --> OUT
    IF_B_T --> |  true | CG_S
    IF_A_T --> |  true | CG_R

style IF_A fill:#95C8F0;
style IF_B fill:#a5b2f0;
style SEL fill:#d5e8d4;
style S fill:#e0a567;
style E fill:#e0a567;
```

Here, we mark "if-b" as a test for the transition condition "if-before-action" or "No group identified" -> "Has active group".
Similarly, we mark "if-a" as a test for the transition condition "if-after-action" or "Has active group" -> "No group identified".

Let us discuss how the algorithm works for different paths.

Example 1:

```mermaid
flowchart LR
    IF@{ shape: sm-circ, label: "" }
    E1@{ shape: sm-circ, label: "" } 
    A["(run) books"]
    subgraph child_books
        B["(run) child_books"]
        D["(run) recent_child_books"]
    end 
    A-->|" chld 2 >  < 1 prnt "| B
    B--> |"chld 1 >  < 1 prnt"| D
   IF--- |" < 0 prnt "| A
   D--- |" chld 0 > "| E1
```

The sequence in the algorithm can be represented by the following table with the operator (`(run)`) omitted:

|Step| task | chld | prnt | curGroup | if-b | curGroup | action | if-a | curGroup | 
|---| --- | --- | --- | --- | --- | --- | --- | --- | --- |
|1| books | 2 |  1 | None |false| None| select None/books| false | None |
|2| child_books | 1 | 1 | None | true | child_books | select child_books/child_books | false |  child_books |
|3| recent_child_books | 0 | 0 | child_books | false | child_books | select child_books/recent_child_books | true |  None |

The table columns are as follows:
- Step - iteration step
- task - current task in the path
- chld - number of children of the current task
- prnt - number of parents of the subsequent task, or zero if there is no subsequent task
- curGroup - current group from the previous step
- if-b - whether the transition condition "No group identified" -> "Has active group" is met
- curGroup - current group after the `if-b` check
- action - group and task selection, has the form of `group/task`
- if-a - whether the transition condition "Has active group" -> "No group identified" is met
- curGroup - current group after the `if-a` check

Note: The `curGroup` column name is repeated to fit the table.

Example 2:

```mermaid
flowchart LR
    IF@{ shape: sm-circ, label: "" }
    E1@{ shape: sm-circ, label: "" }
    A["(run) books"]

    subgraph child_books
        B["(run) child_books"]
        D["(run) recent_child_books"]
        I["(run) recent_child_author"]
    end
    F["(run) same_author"]
    A-->|" chld 2 >  < 1 prnt "| B
    B--> |"chld 1 >  < 1 prnt"| D
    IF--- |" < 0 prnt "| A
    D--> |"chld 1 >  < 1 prnt"| I
    I--- |" chld 1 >  < 2 prnt "| F
    F--- |" chld 0 > "| E1
```

|Step| task | chld | prnt | curGroup | if-b | curGroup | action | if-a | curGroup | 
|---| --- | --- | --- | --- | --- | --- | --- | --- | --- |
|1| books | 2 |  1 | None |false| None| select None/books| false | None |
|2| child_books | 1 | 1 | None | true | child_books | select child_books/child_books | false | child_books |
|3| recent_child_books | 1 | 1 | child_books | false | child_books | select child_books/recent_child_books | false | child_books |
|4| recent_child_author | 1 | 2 | child_books | false | child_books | select child_books/recent_child_author | true | None |
|5| same_author | 0 | 0 | None | false | None | select None/same_author | false |  None |

The algorithm produces a `group/task` sequence. For Example 1, the output is:

|output| 
|---|
| None/(run) books|
| child_books/(run) child_books |
| child_books/(run) recent_child_books |

In the next section, we will examine the `AGraphPathProcessor` class in detail.

[Detailed Overview of the AGraphPathProcessor Class ->](chap6.md)
