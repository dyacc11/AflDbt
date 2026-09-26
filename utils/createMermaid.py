import logging
logging.basicConfig(level=logging.INFO)

# ============================================================================
# Dynamic paths calculations
# ============================================================================

from pathlib import Path
import sys
import os
    
projectRoot = Path(__file__).resolve().parent.parent

# append only if it's not in the sys.path
if projectRoot not in sys.path:
    sys.path.append(str(projectRoot))    

from src.AflDbt.AMain import AMain

# ============================================================================
# *** global counters *** 
# ============================================================================
_operatorOrder = 1
_groupOrder = 0

# ============================================================================
# *** Group ***
# ============================================================================
class ADiagramGroup:
    GROUP_STYLE = "style {nodeName} fill:#e8f5e9,stroke:#388e3c"

    def __init__(self, groupName):
        global _groupOrder

        groupNode = f"G{_groupOrder}"
        nodeStyle = self.__class__.GROUP_STYLE.format(nodeName=groupNode)
        self.groupPre = ""
        self.groupPost = ""
		
        # form subgraph for Non-default groups only
        if groupName is not None:
            self.groupPre += f"\tsubgraph {groupNode}[\"{groupName}\"]\n"
            self.groupPost += f"\tend \n{nodeStyle}\n"
			
		# increment global groups counter
        _groupOrder += 1

# ============================================================================		
# *** Operator ***
# ============================================================================
class ADiargamOperator:
  # run task colors	
  RUN_TASK_STYLE = "style {nodeName} fill:#e1f5fe,stroke:#0288d1"	
   
  # test task colors
  TEST_TASK_STYLE = "style {nodeName} fill:#fff3e0,stroke:#f57c00"	
  
  def __init__(self, taskName ):
    global _operatorOrder
    self.taskNode = f"T{_operatorOrder}"
    self.taskStr = self.taskNode + f"[\"{taskName}\"]\n" 
	
    # set appropriate color of the node
    if taskName.startswith("(run) "):
        self.taskStr += self.__class__.RUN_TASK_STYLE.format(nodeName=self.taskNode) + "\n"
           
    if taskName.startswith("(test) "):
        self.taskStr += self.__class__.TEST_TASK_STYLE.format(nodeName=self.taskNode) + "\n"
	
	# increment global task counter	
    _operatorOrder += 1

# ============================================================================
# globals
# ============================================================================
_tasks = {}   # list of tasks 
_groups = {}   # list of groups

# ============================================================================
# Output diagram
# ============================================================================
# *** Output diagram ***
_outString = ""

# *** String for task sequences collecting ***
_taskSeqStr = ""

# ============================================================================
# C A L L B A C K S
# ============================================================================

# DAG
def createDagCallback( param ):
  global _outString
  
  logging.debug(f"Call createDagCallback with \"{param}\"")

  _outString += "---\n"
  _outString += "title: " + param + "\n"
  _outString += "---" + "\n"
  _outString += "flowchart LR \n"
  
  logging.debug(f"_outString after createDagCallback : \"{_outString}\"")	

# TaskGroup
def createTaskGroupCallback(tgName):
  global _groups
  global _tasks
  logging.debug(f"Call createTaskGroupCallback with \"{tgName}\"")
  
  _groups[tgName] = ADiagramGroup(tgName)
  _tasks[tgName] = []

# Task/Operator
def createTaskCallback (tName, bashCmd, tGroup):
  global _tasks 
  logging.debug(f"Call createTaskCallback with tName=\"{tName}\", bashCmd=\"{bashCmd}\", tGroup=\"{tGroup}\"")
  
  t = ADiargamOperator(tName)
  _tasks[tGroup].append(t)

  return t
  
# Sequence
def createTaskSequnce (tFirst, tSecond):
  global _taskSeqStr
  logging.debug(f"Call createTaskSequnce with {tFirst.taskNode} >> {tSecond.taskNode}")
  
  _taskSeqStr += f"{tFirst.taskNode} --> {tSecond.taskNode}\n"
  
# ============================================================================
# Command-line parsing
# ============================================================================

# globals for arguments
_in_file = None
_out_file = None
_isSkipDbtTest = True

# parsing function
def CmdParse():
    global _in_file
    global _out_file
    global _isSkipDbtTest
    
    shortName = os.path.basename(sys.argv[0])
    args = sys.argv[1:]  # skip the script name itself
    
    if len(args) < 1 or len(args) > 3:
        print(f"Error: Invalid argument count. "
              f"Expected 1 to 3 arguments, but got {len(args)}.")
        print(f"Usage: {shortName} [-t] <input-file> [output-file]")
        sys.exit(1)
        
    # Validate and extract the optional -t flag
    flag = False
    idx = 0  # current position in args list

    if args[0].startswith("-"):
        if args[0] != "-t":
            print(f"Error: Invalid flag '{args[0]}'. Only '-t' is supported.")
            sys.exit(1)
        _isSkipDbtTest = False
        idx = 1  # move past the flag

    # Check arguments if no flag provided 
    if idx == 0 and len(args) > 2 :
        print(f"Error: Invalid argument count. "
              f"Expected 1 to 2 arguments without the '-t' flag usage, but got {len(args)}.")
        print(f"Usage: {shortName} [-t] <input-file> [output-file]")
        sys.exit(1)
        
    
    # Extract the mandatory input file
    if idx >= len(args):
        print("Error: Missing input file.")
        print("Usage: {shortName} [-t] <input-file> [output-file]")
        sys.exit(1)

    _in_file = args[idx]
    idx += 1

    # Extract the optional output file (default: "output.txt")
    if idx < len(args):
        _out_file = args[idx]
    
# ============================================================================
#         M A I N
# ============================================================================
def main():

   CmdParse()
   logging.debug(f"Running with:")
   logging.debug(f"  flag     = {_isSkipDbtTest}")
   logging.debug(f"  in_file  = {_in_file}")
   logging.debug(f"  out_file = {_out_file}")
   
   
   dbtData = {
     "DBT_PROJECT_DIR":"",
     "DBT_COMMAND":"",
     "DBT_MANIFEST_PATH":_in_file,
     "SKIP_DBT_TEST":str(_isSkipDbtTest)
   }
   
   # "../tests/simpleJsonTest.json",
   # "../dbt/books_library/target/manifest.json" 
   logging.debug("create AMain class instance ")
   
   mn = AMain(dbtData)
   
   logging.debug(f"dbtData = {dbtData}")
   
   
   logging.debug("run process of an AMain class instance ")
   mn.Process(
      createDagCallback,
      createTaskGroupCallback,
      createTaskCallback,
      createTaskSequnce 
   )
   
   logging.debug("end of AMain.Process")
   
   global _tasks
   global _groups
   global _outString
   global _taskSeqStr
   
   for groupId, grpClass in _groups.items():
      # print group prefix
      _outString += grpClass.groupPre 

      # then create operators per group       
      for task in _tasks[groupId]:
        _outString += task.taskStr
        
      # print group postfix
      _outString += grpClass.groupPost

	   
   _outString += _taskSeqStr
   
   if _out_file is None:
      print(_outString)
   else: 
      with open(_out_file, "w", encoding="utf-8") as file:
         file.write(_outString)


if __name__ == "__main__":
    main()

