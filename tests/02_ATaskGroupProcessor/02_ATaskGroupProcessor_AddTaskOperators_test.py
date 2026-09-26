from src.AflDbt.AData import AData
from src.AflDbt.ATaskGroupProcessor import ATaskGroupProcessor
import tests.AMockOperator as mo

import logging
logging.basicConfig(level=logging.INFO)

def test_ATaskGroupProcessor_AddTaskOperators():

  aData = AData()
  dbtData = {
     "DBT_PROJECT_DIR":"/test/proj/dir",
     "DBT_COMMAND":"/test/dbt"
  }
  
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

  logging.info("check operators : ")
  
  logging.info("run ClearGlobals() ")
  mo.ClearGlobals()

  
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
  
  # 3. check that we have 2 operators in mo.tasks
  assert len(mo.tasks) == 2
  
  # 4. check that operators are ok
  logging.info(f" mo.tasks[0].taskId = {mo.tasks[0].taskId}")
  logging.info(f" mo.tasks[0].bashCmd = {mo.tasks[0].bashCmd}")
  logging.info(f" mo.tasks[0].taskGroup = {mo.tasks[0].taskGroup}")
  
  assert mo.tasks[0].taskId == "(run) Task0_1"
  assert mo.tasks[0].bashCmd == "/test/dbt run --project-dir /test/proj/dir --profiles-dir /test/proj/dir -s Task0_1"
  assert mo.tasks[0].taskGroup == None
  
  
  logging.info(f" mo.tasks[1].taskId = {mo.tasks[1].taskId}")
  logging.info(f" mo.tasks[1].bashCmd = {mo.tasks[1].bashCmd}")
  logging.info(f" mo.tasks[1].taskGroup = {mo.tasks[1].taskGroup}")
  
  assert mo.tasks[1].taskId == "(test) Task1_2"
  assert mo.tasks[1].bashCmd == "/test/dbt test --project-dir /test/proj/dir --profiles-dir /test/proj/dir -s Task1_2"
  assert mo.tasks[1].taskGroup == "group1"

  