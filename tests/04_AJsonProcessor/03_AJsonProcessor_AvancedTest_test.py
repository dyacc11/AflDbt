from collections import Counter
from src.AflDbt.AData import AData
from src.AflDbt.AJsonProcessor import AJsonProcessor

import logging
logging.basicConfig(level=logging.INFO)


def test_AJsonProcessor_AvancedTest():
  dbtData = {
    "DBT_PROJECT_DIR":"/test/proj/dir",
    "DBT_COMMAND":"/test/dbt",
    "DBT_MANIFEST_PATH":"tests/advancedJsonTest.json",
    "SKIP_DBT_TEST":"False"
  }
  
  bData = AData.ABaseData( dbtData.get("SKIP_DBT_TEST","True") )
  jProc = AJsonProcessor (bData,dbtData )

  logging.info(f"baseData.dbtProcessTests = {bData.dbtProcessTests}")
  
  retList = jProc.Process()
  logging.info(f"AJsonProcessor returns {retList}")
  
  logging.info("Check list being returned")
  # required list to be returned
  _retListReq = ["(run) s_task,(run) b_task,(run) e_task,(test) e_task_test",
     "(run) s_task,(run) c_task1,(run) c_task2,(run) e_task,(test) e_task_test",
     "(run) s_task,(run) c_task1,(test) c_task1_test1",
     "(run) s_task,(run) c_task1,(test) c_task1_test2",
     "(run) s_task,(test) s_task_test"
  ]
    
  assert Counter(_retListReq) == Counter(retList) 
  
  logging.info("Check list of nodes")
  logging.info(f"AData fullModels = {bData.fullModels}")
  _fullModelsReq = {
     "(run) s_task": "model.dbt_advanced_test.s_task",
     "(run) b_task": "model.dbt_advanced_test.b_task",
     "(run) c_task1": "model.dbt_advanced_test.c_task1",
     "(run) c_task2": "model.dbt_advanced_test.c_task2",
     "(run) e_task": "model.dbt_advanced_test.e_task",
     "(test) c_task1_test1": "test.dbt_advanced_test.c_task1_test1",
     "(test) c_task1_test2": "test.dbt_advanced_test.c_task1_test2",
     "(test) s_task_test": "test.dbt_advanced_test.s_task_test",
     "(test) e_task_test": "test.dbt_advanced_test.e_task_test"
  }
  
  assert _fullModelsReq == bData.fullModels
  
  _fullModelsRevReq = {
     "model.dbt_advanced_test.s_task": "(run) s_task",
     "model.dbt_advanced_test.b_task": "(run) b_task",
     "model.dbt_advanced_test.c_task1": "(run) c_task1",
     "model.dbt_advanced_test.c_task2": "(run) c_task2",
     "model.dbt_advanced_test.e_task": "(run) e_task",
     "test.dbt_advanced_test.c_task1_test1": "(test) c_task1_test1",
     "test.dbt_advanced_test.c_task1_test2": "(test) c_task1_test2",
     "test.dbt_advanced_test.s_task_test": "(test) s_task_test",
     "test.dbt_advanced_test.e_task_test": "(test) e_task_test"
  }
 
  assert _fullModelsRevReq == bData.fullModelsRev
  
  logging.info("Check project name")
  logging.info(f"AData projectName = {bData.projectName}")
  
  assert "dbt_advanced_test" == bData.projectName
  
  logging.info("Check lists of parents and children")
  logging.info(f"AData chldCount = {bData.chldCount}")
  
  _chldCountReq = {
     "(run) s_task": 3,
     "(run) b_task": 1,
     "(run) c_task1": 3,
     "(run) c_task2": 1,
     "(run) e_task": 1,
     "(test) c_task1_test1": 0,
     "(test) c_task1_test2": 0,
     "(test) e_task_test": 0,
     "(test) s_task_test": 0
  }	
  assert _chldCountReq == bData.chldCount
  
  logging.info(f"AData prntCount = {bData.prntCount}")
  
  _prntCountReq = { 
     "(run) s_task": 0,
     "(run) b_task": 1,
     "(run) c_task1": 1,
     "(run) c_task2": 1,
     "(run) e_task": 2,
     "(test) c_task1_test1": 1,
     "(test) c_task1_test2": 1,
     "(test) e_task_test": 1,
     "(test) s_task_test": 1
  }
  
  assert _prntCountReq == bData.prntCount
