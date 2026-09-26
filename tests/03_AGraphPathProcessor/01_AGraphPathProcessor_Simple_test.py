from src.AflDbt.AData import AData
from src.AflDbt.ATaskGroupProcessor import ATaskGroupProcessor
from src.AflDbt.AGraphPathProcessor import AGraphPathProcessor

import logging
logging.basicConfig(level=logging.INFO)


def test_AParsedRowProcessor_Simple():
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
   
   logging.info("check that groups are \"c_task1\" and None")
   _lstGroups = list(aData.groupsData.taskGroupNameMap)
   logging.info(f"groups are {_lstGroups}")
   assert len(_lstGroups) == 2
   
   assert _lstGroups[0] is None
   assert _lstGroups[1] == "c_task1"
   
   logging.info("check that there are 4 tasks")
   
   _lstTasks = list(aData.taskMap)
   logging.info(f"tasks are {_lstTasks}")
   assert len(_lstTasks) == 4
   
   assert _lstTasks[0] == "(run) s_task"
   assert _lstTasks[1] == "(run) c_task1"
   assert _lstTasks[2] == "(run) c_task2"
   assert _lstTasks[3] == "(run) e_task"
   
   logging.info("check that there are 3 sequnces")
   
   _lstSeq = list(aData.taskSequence)
   logging.info(f"sequences are {_lstSeq}")
   assert len(_lstSeq) == 3
   
   assert _lstSeq[0] == "|(run) s_task|>>|(run) c_task1|"
   assert _lstSeq[1] == "|(run) c_task1|>>|(run) c_task2|"
   assert _lstSeq[2] == "|(run) c_task2|>>|(run) e_task|"
