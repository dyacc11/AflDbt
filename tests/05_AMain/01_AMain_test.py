from collections import Counter
from src.AflDbt.AMain import AMain
import tests.AMockOperator as mo

import logging
logging.basicConfig(level=logging.INFO)

    
def test_AMain_Simple():

   dbtData = {
     "DBT_PROJECT_DIR":"/test/proj/dir",
     "DBT_COMMAND":"/test/dbt",
     "DBT_MANIFEST_PATH":"tests/simpleJsonTest.json",
     "SKIP_DBT_TEST":"True"
   }
   logging.info("create AMain class instance ")
   
   mn = AMain(dbtData)
   
   logging.info(f"baseData.dbtProcessTests = {mn.aData.baseData.dbtProcessTests}")
   
   logging.info("run ClearGlobals() ")
   mo.ClearGlobals()
   
   logging.info("run process of an AMain class instance ")
   mn.Process(
      mo.createDagCallback,
      mo.createTaskGroupCallback,
      mo.createTaskCallback,
      mo.createTaskSequnce 
   )
   
   logging.info("end of test_AMain_Simple()")
