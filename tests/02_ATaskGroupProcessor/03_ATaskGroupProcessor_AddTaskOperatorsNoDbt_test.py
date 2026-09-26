from src.AflDbt.AData import AData
from src.AflDbt.ATaskGroupProcessor import ATaskGroupProcessor
import tests.AMockOperator as mo

import logging
logging.basicConfig(level=logging.INFO)

def test_ATaskGroupProcessor_AddTaskOperatorsNoDbt():

  aData = AData()
  dbtData = {}
  
  grProc = ATaskGroupProcessor(aData.taskMap, aData.groupsData, dbtData)
  
  logging.info("Test tasks adding")
  grProc.AddTaskToGroup( "(run) Task0_1", None )
  grProc.AddTaskToGroup( "(test) Task1_2", "group1" )
  
  logging.info("check that groups are \"group1\" and None")
  _lstGroups = list(aData.groupsData.taskGroupNameMap)
  assert len(_lstGroups) == 2
  
  assert _lstGroups[0] is None
  assert _lstGroups[1] == "group1"
  
  logging.info("check tasks in aData.taskMap ")
  
  _lstTasks = list(aData.taskMap)
  assert len(_lstTasks) == 2
  
  assert _lstTasks[0] == "(run) Task0_1"
  assert _lstTasks[1] == "(test) Task1_2"
  
  # -------------- Check Operators ------------------------
  
  logging.info("run ClearGlobals() ")
  mo.ClearGlobals()

  logging.info("check operators : ")
  
  aData.baseData.projectName = "Test project Name"
  aData.CreateAirflowObjects(
     mo.createDagCallback, 
     mo.createTaskGroupCallback, 
     mo.createTaskCallback, 
     mo.createTaskSequnce 
  )
  logging.info(f"returned dagName = \"{mo.dagName}\"")
  
  # 2. check that groups are None and "group1"
  logging.info(f"groups are {mo.groups}")
  logging.info("check that groups are None and \"group1\"")
  
  assert len(mo.groups) == 2
  assert mo.groups[0] == None
  assert mo.groups[1] == "group1"
  
  # 3. check that we have no operators in _tasks since no dbt has been specified
  logging.info(f"_tasks are {mo.tasks}")
  logging.info("check that no items are in _tasks")
  assert len(mo.tasks) == 0
  
  
  
