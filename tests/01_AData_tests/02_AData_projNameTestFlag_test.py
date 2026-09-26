import logging
logging.basicConfig(level=logging.INFO)

from src.AflDbt.AData import AData
import tests.AMockOperator as mo


def test_AData_projNameTestFlag():
  aData = AData("False")
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
  assert mo.dagName == "Test project Name with tests"
