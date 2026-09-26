from src.AflDbt.AData import AData
from src.AflDbt.ATaskGroupProcessor import ATaskGroupProcessor

import logging
logging.basicConfig(level=logging.INFO)


def test_ATaskGroupProcessor_AddTask():

  taskMap = {}
  aData = AData()
  dbtData = {
     "DBT_PROJECT_DIR":"/test/proj/dir",
     "DBT_COMMAND":"/test/dbt"
  }
  
  grProc = ATaskGroupProcessor(taskMap, aData.groupsData, dbtData)
  
  logging.info("Test tasks adding")
  grProc.AddTaskToGroup( "(run) Task0_1", None )
  grProc.AddTaskToGroup( "(run) Task0_2", None )
  
  grProc.AddTaskToGroup( "(run) Task1_1", "group1" )
  grProc.AddTaskToGroup( "(test) Task1_2", "group1" )
  
  logging.info("check that groups are \"group1\" and None")
  _lstGroups = list(aData.groupsData.taskGroupNameMap)
  assert len(_lstGroups) == 2
  
  assert _lstGroups[0] is None
  assert _lstGroups[1] == "group1"
  
  logging.info("check tasks in taskMap ")
  
  _lstTasks = list(taskMap)
  assert len(_lstTasks) == 4
  
  assert _lstTasks[0] == "(run) Task0_1"
  assert _lstTasks[1] == "(run) Task0_2"
  assert _lstTasks[2] == "(run) Task1_1"
  assert _lstTasks[3] == "(test) Task1_2"

  logging.info("check tasks in taskGroupNameMap ")
  
  _lstTasks = aData.groupsData.taskGroupNameMap[None]
  assert len(_lstTasks) == 2
  assert _lstTasks[0] == "(run) Task0_1"
  assert _lstTasks[1] == "(run) Task0_2"
  
  _lstTasks = aData.groupsData.taskGroupNameMap["group1"]
  assert len(_lstTasks) == 2
  assert _lstTasks[0] == "(run) Task1_1"
  assert _lstTasks[1] == "(test) Task1_2"
  
  logging.info("check tasks in taskGroupRefMap ")
  
  _lstTasks = aData.groupsData.taskGroupRefMap[None]
  assert len(_lstTasks) == 2
  assert _lstTasks[0].taskName == "(run) Task0_1"
  assert _lstTasks[1].taskName == "(run) Task0_2"
  
  _lstTasks = aData.groupsData.taskGroupRefMap["group1"]
  assert len(_lstTasks) == 2
  assert _lstTasks[0].taskName == "(run) Task1_1"
  assert _lstTasks[1].taskName == "(test) Task1_2"
  

