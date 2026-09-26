from src.AflDbt.AData import AData
from src.AflDbt.ATaskGroupProcessor import ATaskGroupProcessor
from src.AflDbt.AGraphPathProcessor import AGraphPathProcessor
import tests.AMockOperator as mo

import logging
logging.basicConfig(level=logging.INFO)


def test_AParsedRowProcessor_Advanced():
   aData = AData()
   aData.baseData.projectName = "Test project Name"
   dbtData = {
     "DBT_PROJECT_DIR":"/test/proj/dir",
     "DBT_COMMAND":"/test/dbt"    
   }
   
   aData.baseData.chldCount = {
     "(run) s_task":2,
     "(run) b_task":1,
     "(run) c_task1":1,
     "(run) c_task2":1,
     "(run) e_task":0
   }

   aData.baseData.prntCount = {
     "(run) s_task":0,
     "(run) b_task":1,
     "(run) c_task1":1,
     "(run) c_task2":1,
     "(run) e_task":2
   }
   
   logging.info(f"!!!groups are {mo.groups}")
   
   logging.info("create row processor")
   rProc =  AGraphPathProcessor (
     aData.taskMap,
     aData.groupsData,
     aData.taskSequence,
     aData.baseData.chldCount,
     aData.baseData.prntCount,
     dbtData
   )
   
   logging.info("Start AParsedRowProcessor processing")
   rProc.ProcessPath("(run) s_task,(run) c_task1,(run) c_task2,(run) e_task")
   
   logging.info("check that there are 2 groups")
   
   _lstGroups = list(aData.groupsData.taskGroupNameMap)
   logging.info(f"groups are {_lstGroups}")
   assert len(_lstGroups) == 2
   
   logging.info("check that there are 4 tasks")

   _lstTasks = list(aData.taskMap)
   logging.info(f"tasks are {_lstTasks}")
   assert len(_lstTasks) == 4
      
   logging.info("check that there are 3 sequnces")
   
   _lstSeq = list(aData.taskSequence)
   logging.info(f"sequences are {_lstSeq}")
   assert len(_lstSeq) == 3
   
   # -------------- Check Operators ------------------------

   logging.info("check operators : ")
  
   aData.baseData.projectName = "Test project Name"
   
   logging.info("run ClearGlobals() ")
   mo.ClearGlobals()

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
   assert mo.groups[1] == "c_task1"
  
   # 3. check that we have no operators in mo.tasks since no dbt has been specified
   logging.info(f"mo.tasks are {mo.tasks}")
   logging.info("check that no items are in mo.tasks")
   assert len(mo.tasks) == 4
   
   # 4. check that operators are ok
   logging.info(f" mo.tasks[0].taskId = {mo.tasks[0].taskId}")
   logging.info(f" mo.tasks[0].bashCmd = {mo.tasks[0].bashCmd}")
   logging.info(f" mo.tasks[0].taskGroup = {mo.tasks[0].taskGroup}")
  
   assert mo.tasks[0].taskId == "(run) s_task"
   assert mo.tasks[0].bashCmd == "/test/dbt run --project-dir /test/proj/dir --profiles-dir /test/proj/dir -s s_task"
   assert mo.tasks[0].taskGroup == None
   
   logging.info(f" mo.tasks[1].taskId = {mo.tasks[1].taskId}")
   logging.info(f" mo.tasks[1].bashCmd = {mo.tasks[1].bashCmd}")
   logging.info(f" mo.tasks[1].taskGroup = {mo.tasks[1].taskGroup}")
   
   assert mo.tasks[1].taskId == "(run) e_task" 
   assert mo.tasks[1].bashCmd == "/test/dbt run --project-dir /test/proj/dir --profiles-dir /test/proj/dir -s e_task"
   assert mo.tasks[1].taskGroup == None
   
   
   logging.info(f" mo.tasks[2].taskId = {mo.tasks[2].taskId}")
   logging.info(f" mo.tasks[2].bashCmd = {mo.tasks[2].bashCmd}")
   logging.info(f" mo.tasks[2].taskGroup = {mo.tasks[2].taskGroup}")
   
   assert mo.tasks[2].taskId == "(run) c_task1" 
   assert mo.tasks[2].bashCmd == "/test/dbt run --project-dir /test/proj/dir --profiles-dir /test/proj/dir -s c_task1"
   assert mo.tasks[2].taskGroup == "c_task1"
   
   logging.info(f" mo.tasks[3].taskId = {mo.tasks[3].taskId}")
   logging.info(f" mo.tasks[3].bashCmd = {mo.tasks[3].bashCmd}")
   logging.info(f" mo.tasks[3].taskGroup = {mo.tasks[3].taskGroup}")
  
   assert mo.tasks[3].taskId == "(run) c_task2"
   assert mo.tasks[3].bashCmd == "/test/dbt run --project-dir /test/proj/dir --profiles-dir /test/proj/dir -s c_task2"
   assert mo.tasks[3].taskGroup == "c_task1"
   
   
   
   # 5. check that sequnces are ok
   logging.info(f"sequences are {mo.taskSeq}")
   assert len(mo.taskSeq) == 3
   
   assert mo.taskSeq[0][0].taskId == "(run) s_task"
   assert mo.taskSeq[0][1].taskId == "(run) c_task1"
   
   assert mo.taskSeq[1][0].taskId == "(run) c_task1"
   assert mo.taskSeq[1][1].taskId == "(run) c_task2"
   
   assert mo.taskSeq[2][0].taskId == "(run) c_task2"
   assert mo.taskSeq[2][1].taskId == "(run) e_task"
   
   
   

