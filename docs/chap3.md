[<- Back to Table of Contents](index.md)

## Overview of the ABaseData and AJsonProcessor Classes

It makes sense to first take a quick look at the structure and functioning of these classes and how data is parsed and transferred, and then examine the class structure in more detail.

Consider the following diagram:

```mermaid
block-beta
    columns 8
%% ====== INPUT DATA ==========
    block:dbtfile:2
		columns 4
		DBT_T["maniifest.json"]:4
		space:4
			DBT_META[/"metadata"/]:2 space:2
				space:1 DBT_PRJ("project_name"):2 space:1
			DBT_ND[/"nodes"/]:2 space:2
				space:1 DBT_ND_IK("item-key"):2 space:1
					space:2 DBT_ND_NM(["name"]):2 
			DBT_PM[/"parent_map"/]:2 space:2
				space:1 DBT_PM_IK("item-key"):2 space:1
					space:2 DBT_PM_I1(["parent1"]):2
					space:2 DBT_PM_I2(["parent2"]):2
			DBT_CM[/"child_map"/]:2 space:2
				space:1 DBT_CM_IK("item-key"):2 space:1
					space:2 DBT_CM_I1(["child1"]):2
					space:2 DBT_CM_I2(["child2"]):2

    end
    space:1
%% ====== CLASSES ==========
	block:CLSS:3
		columns 1
		CLSS_T["Classes"]
		space:1  
		
		ABD_T("ABaseData")
		
		ABD_PN["projectName: string"]
		ABD_FM["fullModels: map{taskName:full_name}"]
		ABD_FMR["fullModelsRev: reverse map {full_name:taskName}"]
		ABD_PC["prntCount: map{taskName:count}"]
		ABD_CC["chldCount: map{taskName:count}"]
		ABD_PT["dbtProcessTests: boolean"]
		ABD_INI["_init_(strDbtProcessTests: string)"]

		space:1 

		JP_T("AJsonProcessor")
		JP_INI["_init_( baseData: ABaseData, opConf: Configuration )"]
		JP_PRC["Process()  : list [graph path]"]
	end
  space:1
%% ====== Comments ==========	
    block:COMM:2
		columns 1
		COMM_T["Comments"]
		COMM_FN["full_name = nodes.item-key"]
		COMM_TN["taskName gets from name \nand type"]
		COMM_FNR["reverse map"]
		COMM_PC["Count of parent1,parent2 ..."]
		COMM_CC["Count of child1,child2 ..."]
		COMM_PT["filter on types"]
		COMM_JP_CFG["file to process"]
		COMM_JP_CFG_RET["return list of grapth paths"]
    end
%% ====== links ==========

%% project name
DBT_PRJ -->ABD_PN

%% full map
DBT_ND_IK --> ABD_FM
DBT_ND_IK --> ABD_FMR

%% parents
DBT_PM_IK --> ABD_PC

%% childs
DBT_CM_IK --> ABD_CC

%% AJsonProcessor -> ABaseData
JP_T -.-> ABD_T

%% commants
COMM_FN -.-> ABD_FM 
COMM_TN -.-> ABD_FM
COMM_FNR -.-> ABD_FMR
COMM_PC -.-> ABD_PC
COMM_CC -.-> ABD_CC
COMM_PT -.-> ABD_PT
COMM_JP_CFG -.-> JP_INI
COMM_JP_CFG_RET -.-> JP_PRC
%% ====== styles ==========

%% Level 1: Title - no border
style DBT_T fill:#c6acf2,stroke:none,stroke-width:0px,color:#333

%% Level 2: Sections - blue (shared)
classDef lev2 fill:#dae8fc,stroke:#6c8ebf,stroke-width:2px;
class DBT_META,DBT_ND,DBT_PM,DBT_CM lev2
    style DBT_CM fill:#dae8fc,stroke:#6c8ebf,stroke-width:2px
	
%% metadata branch - orange family
style DBT_PRJ fill:#ffe6cc,stroke:#d79b00,stroke-width:2px
style ABD_PN fill:#ffe6cc,stroke:#d79b00,stroke-width:2px

%% nodes branch - green family
style DBT_ND_IK fill:#d5e8d4,stroke:#82b366,stroke-width:2px
style DBT_ND_NM fill:#e6f5e6,stroke:#a6d19a,stroke-width:2px
style ABD_FM fill:#d5e8d4,stroke:#82b366,stroke-width:2px
style ABD_FMR fill:#d5e8d4,stroke:#82b366,stroke-width:2px

%% parent_map branch - purple family
style DBT_PM_IK fill:#e1d5e7,stroke:#9673a6,stroke-width:2px
style DBT_PM_I1 fill:#f3e5f5,stroke:#ba68c8,stroke-width:2px
style DBT_PM_I2 fill:#f3e5f5,stroke:#ba68c8,stroke-width:2px
style ABD_PC fill:#e1d5e7,stroke:#9673a6,stroke-width:2px

%% child_map branch - pink/rose family
style DBT_CM_IK fill:#f8cecc,stroke:#b85450,stroke-width:2px
style DBT_CM_I1 fill:#fce4ec,stroke:#e57373,stroke-width:2px
style DBT_CM_I2 fill:#fce4ec,stroke:#e57373,stroke-width:2px
style ABD_CC fill:#f8cecc,stroke:#b85450,stroke-width:2px

classDef blckDash stroke:#000000 ,stroke-width:2px,stroke-dasharray: 5 5;
classDef n0Fill fill:none,stroke-width:0px;
classDef dblBrd stroke:#000000 ,stroke-width:2px,border: 6px 
classDef prcLgc stroke:#094782,fill:#95C8F0
classDef aflObj stroke:#094782,fill:#58f21b
class CLSS_T,COMM,COMM_T n0Fill
class CLSS prcLgc 
class JP_T,ABD_T aflObj
class COMM blckDash 
style dbtfile fill:#e3daeb
classDef commFill fill:#85f2ed
class COMM_FN,COMM_TN,COMM_FNR,COMM_PC,COMM_CC,COMM_PT,COMM_JP_CFG,COMM_JP_CFG_RET commFill
```
First of all, a dbt manifest file has several top-level keys:
* `metadata`, which is a dictionary of properties
* `nodes`, which is a dictionary of all analyses, models, seeds, snapshots, and tests
* `parent_map`, which is a dictionary that contains the first-order parents of each resource
* `child_map`, which is a dictionary that contains the first-order children of each resource

We use the `project_name` property from the `metadata`.
From `nodes`, we use the `name` property, which is the resource name.
The `unique_id` property in `nodes` is the same as the dictionary key for each node.
The `depends_on` property in `nodes` refers to parents, but using `parent_map` is easier for retrieving first-order parents.
The `unique_id` has the pattern `resource_type.package.resource_name`, where `package` is the `project_name`, `resource_type` is the type of the model, test, etc., and `resource_name` may differ from the actual filename, at least for tests.
Therefore, to execute the `dbt` command, we need to use the value of the `name` property.

The `parent_map` and `child_map` establish first-order relationships between resource `unique_id`s and are values of type list. If an element has no parent or child, the list is empty, or the key may be missing in `parent_map` or `child_map`.
Therefore, we need to use `nodes` as the main resource list and add dependencies from `parent_map` or `child_map` when possible.

Let us examine how `AJsonProcessor` transforms data from the JSON manifest file into the fields of the `ABaseData` class:

First, the `ABaseData` class has a `dbtProcessTests` field with a Boolean value for filtering nodes. 
Typically, a model node has a unique identifier such as `model.myProject.model1`, while a test node might look like `test.myProject.unique_model1_id.16e066b321`, where `myProject` is the project name and `model1` is the model name.
If `dbtProcessTests` is `True`, `AJsonProcessor` collects both models and tests, and if it is `False`, it collects only models.
The `dbtProcessTests` field is initialized when the `ABaseData` class instance is instantiated, and the constructor has only one argument of type string for initializing `dbtProcessTests`, which is converted to a Boolean value.

Having created an instance of the `ABaseData` class and the configuration object, we can now create an instance of the `AJsonProcessor` class.
The `Process` method reads the `manifest.json` file provided in the configuration and populates the fields of the `ABaseData` instance.

The keys and `name` properties of the `nodes` dictionary are passed to the `fullModels` and `fullModelsRev` maps for models only or for both models and tests. 
However, since the `name` property doesn't contain resource type information, this information can be added using a specific prefix: `(run)` for models and `(test)` for tests. This resulting string, such as `(run) model1`, is hereafter called `taskName`. The resulting data in `ABaseData.fullModels` looks like this:

<table>
 <tr><td colspan=2><p align="center"><b>manifest.json "nodes" dictionary </b></p></td></tr>
 <tr><td><b>key</b></td><td><b>name</b></td></tr>
 <tr><td>model.myProject.model1</td><td>model1</td></tr>
 <tr><td>test.myProject.unique_model1_id.16e066b321</td> <td>unique_model1_id</td></tr> 
 <tr></tr>
 <tr><td colspan=2><p align="center"><b>ABaseData.fullModels</b></p></td></tr>
 <tr> <td><b>key (taskName)</b></td><td><b>value (full_name)</b></td></tr>
 <tr><td>(run) model1</td> <td>model.myProject.model1</td></tr>
 <tr><td>(test) unique_model1_id</td> <td>test.myProject.unique_model1_id.16e066b321</td></tr>
</table>

To populate the `prntCount` and `chldCount` maps of the `ABaseData` class, the `parent_map` and `child_map` dictionaries from the JSON manifest are used. Additionally, the same `taskName` as in `fullModels` is used as the key.
The following shows the data in `prntCount` and `chldCount` for the same example:

<table>
 <tr><td colspan=2><p align="center"><b>prntCount dictionary </b></p></td></tr>
 <tr><td><b>key (taskName)</b></td><td><b>value</b></td></tr>
 <tr><td>(run) model1</td> <td>0</td></tr>
 <tr><td>(test) unique_model1_id</td> <td>1</td></tr>
 <tr></tr>
 <tr><td colspan=2><p align="center"><b>chldCount dictionary</b></p></td></tr>
  <tr><td><b>key (taskName)</b></td><td><b>value</b></td></tr>
 <tr><td>(run) model1</td> <td>1</td></tr>
 <tr><td>(test) unique_model1_id</td> <td>0</td></tr>
</table>

A test always depends on a model:
```mermaid
flowchart LR
    A["(run) model1"]-->B["(test) unique_model1_id"]
```	

Thus, `model1` has one child, and the test has one parent. Accordingly, `model1` has no parent and the test has no children.
The `Process` method returns a list of graph paths, and this will be discussed in the next section.

[AJsonProcessor Class Discussion ->](chap4.md)
