import logging
logging.basicConfig(level=logging.INFO)


class AMockOperator:
  def __init__(self, task_id, bash_command, task_group):
    self.taskId = task_id
    self.bashCmd = bash_command
    self.taskGroup = task_group


tasks = []   # list of tasks 
groups = []   # list of groups
taskSeq = [] # list of sequnces
dagName = None

def ClearGlobals():
  logging.info("Call ClearGlobals")
  global dagName
  global groups
  global tasks
  global taskSeq  
  tasks.clear()   # list of tasks 
  groups.clear()   # list of groups
  taskSeq.clear() # list of sequnces
  dagName = None

def createDagCallback( param ):
  global dagName
  logging.info(f"Call createDagCallback with \"{param}\"")
  dagName = param

def createTaskGroupCallback(tgName):
  global groups
  logging.info(f"Call createTaskGroupCallback with \"{tgName}\"")
  groups.append(tgName)
   
def createTaskCallback (tName, bashCmd, tGroup):
  global tasks 
  logging.info(f"Call createTaskCallback with tName=\"{tName}\", bashCmd=\"{bashCmd}\", tGroup=\"{tGroup}\"")
  t = AMockOperator(task_id=tName, bash_command=bashCmd, task_group=tGroup)
  tasks.append(t)
  return t
  
def createTaskSequnce (tFirst, tSecond):
  global taskSeq
  logging.info(f"Call createTaskSequnce with {tFirst.taskId} >> {tSecond.taskId}")
  t = (tFirst,tSecond)
  taskSeq.append(t)
  