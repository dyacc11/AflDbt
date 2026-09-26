from collections import Counter
from src.AflDbt.AData import AData
from src.AflDbt.AJsonProcessor import AJsonProcessor

import logging
logging.basicConfig(level=logging.INFO)


def test_AJsonProcessor_Simple():
  dbtData = {
    "DBT_PROJECT_DIR":"/test/proj/dir",
    "DBT_COMMAND":"/test/dbt",
    "DBT_MANIFEST_PATH":"tests/simpleJsonTest.json",
    "SKIP_DBT_TEST":"True"
  }
  bData = AData.ABaseData()
  jProc = AJsonProcessor (bData,dbtData )

  logging.info(f"baseData.dbtProcessTests = {bData.dbtProcessTests}")
  
  retList = jProc.Process()
  logging.info(f"AJsonProcessor returns {retList}")
  
  logging.info("Check list being returned")
  # required list to be returned
  _retListReq = [ "(run) s_task,(run) c_task1,(run) c_task2,(run) e_task",
    "(run) s_task,(run) b_task,(run) e_task"]
    
  assert Counter(_retListReq) == Counter(retList) 
  
  logging.info("Check list of nodes")
  logging.info(f"AData fullModels = {bData.fullModels}")
  _fullModelsReq = { 
     "(run) s_task": "model.dbt_simple_test.s_task",
     "(run) b_task": "model.dbt_simple_test.b_task",
     "(run) c_task1": "model.dbt_simple_test.c_task1",
     "(run) c_task2": "model.dbt_simple_test.c_task2",
     "(run) e_task": "model.dbt_simple_test.e_task"
  }
  
  assert _fullModelsReq == bData.fullModels
  
  _fullModelsRevReq = {
     "model.dbt_simple_test.s_task": "(run) s_task",
     "model.dbt_simple_test.b_task": "(run) b_task",
     "model.dbt_simple_test.c_task1": "(run) c_task1",
     "model.dbt_simple_test.c_task2": "(run) c_task2",
     "model.dbt_simple_test.e_task": "(run) e_task"
  }
  
  assert _fullModelsRevReq == bData.fullModelsRev
  
  logging.info("Check project name")
  logging.info(f"AData projectName = {bData.projectName}")
  
  assert "dbt_simple_test" == bData.projectName
  
  logging.info("Check lists of parents and children")
  logging.info(f"AData chldCount = {bData.chldCount}")
  
  _chldCountReq = {
     "(run) b_task": 1,
     "(run) c_task1": 1,
     "(run) c_task2": 1,
     "(run) e_task": 0,
     "(run) s_task": 2
  }
  
  assert _chldCountReq == bData.chldCount
  
  logging.info(f"AData prntCount = {bData.prntCount}")
  
  _prntCountReq = {
     "(run) b_task": 1,
     "(run) c_task1": 1,
     "(run) c_task2": 1,
     "(run) e_task": 2,
     "(run) s_task": 0
  }
  
  assert _prntCountReq == bData.prntCount
