import logging
logging.basicConfig(level=logging.INFO)

from src.AflDbt.AData import AData
import tests.AMockOperator as mo


def test_AData_projNameSimple():

  aData = AData()
  logging.info(f"Check empty project name first")
  
  logging.info("run ClearGlobals() ")
  mo.ClearGlobals()
  aData.CreateAirflowObjects(
     mo.createDagCallback,
     mo.createTaskGroupCallback,
     mo.createTaskCallback,
     mo.createTaskSequnce 
  )
  assert mo.dagName is None

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
  assert mo.dagName == "Test project Name"
