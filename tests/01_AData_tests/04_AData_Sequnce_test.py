import logging
logging.basicConfig(level=logging.INFO)

from src.AflDbt.AData import AData
import tests.AMockOperator as mo
    
class AMockTask:
  def __init__(self, taskName):
    self.taskName = taskName
    self.aflOperator = None
  def CreateOperator(self,tskClbk, groupId ):
    execStr = "Bash " + self.taskName
    self.aflOperator = tskClbk( self.taskName, execStr, groupId )
  def GetOperator(self):
    return self.aflOperator    
    

def test_AData_Sequnce():

  aData = AData()
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
  
  # 1. - check DAG name
  logging.info("1. - check DAG name")
  assert mo.dagName == "Test project Name"
  
  # prepare mock tasks and groups
  logging.info("prepare mock tasks and groups")
  t1 = AMockTask("task2_1")
  t2 = AMockTask("task2_2")
  aData.groupsData.taskGroupRefMap["group2"] = [t1,t2]
  seq = (t1,t2)
  aData.taskSequence["|task2_1|>>|task2_2|"] = seq
  
  # do a creation of mock aiflow objects
  logging.info("do a creation of mock aiflow objects")
  aData.CreateAirflowObjects(
     mo.createDagCallback,
     mo.createTaskGroupCallback,
     mo.createTaskCallback,
     mo.createTaskSequnce 
  )
  
  # 2. check that groups are "group1" and "group2"
  logging.info("check that groups are \"group1\" and \"group2\"")
  assert len(mo.groups) == 1
  assert mo.groups[0] == "group2"
  
  # 3. check that we have 3 operators in mo.tasks
  assert len(mo.tasks) == 2
  
    
  logging.info(f" mo.tasks[0].taskId = {mo.tasks[0].taskId}")
  logging.info(f" mo.tasks[0].bashCmd = {mo.tasks[0].bashCmd}")
  logging.info(f" mo.tasks[0].taskGroup = {mo.tasks[0].taskGroup}")
  
  assert mo.tasks[0].taskId == "task2_1"
  assert mo.tasks[0].bashCmd == "Bash task2_1"
  assert mo.tasks[0].taskGroup == "group2"
  
  logging.info(f" mo.tasks[1].taskId = {mo.tasks[1].taskId}")
  logging.info(f" mo.tasks[1].bashCmd = {mo.tasks[1].bashCmd}")
  logging.info(f" mo.tasks[1].taskGroup = {mo.tasks[1].taskGroup}")
  
  assert mo.tasks[1].taskId == "task2_2"
  assert mo.tasks[1].bashCmd == "Bash task2_2"
  assert mo.tasks[1].taskGroup == "group2"
  
  seq = list(aData.taskSequence)
  assert len(seq) == 1
  logging.info(f" aData.taskSequence key = {seq}")
  
  assert len(mo.taskSeq) == 1
  
  assert mo.taskSeq[0][0].taskId == "task2_1"
  assert mo.taskSeq[0][1].taskId == "task2_2"


