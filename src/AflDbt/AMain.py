from src.AflDbt.AData import AData
from src.AflDbt.AJsonProcessor import AJsonProcessor
from src.AflDbt.AGraphPathProcessor import AGraphPathProcessor


class AMain:
  def __init__(self, dbtData): 
    self.aData = AData( dbtData.get("SKIP_DBT_TEST","True") ) 
    self.jProc = AJsonProcessor (self.aData.baseData,dbtData )
    self.gProc = AGraphPathProcessor (
      self.aData.taskMap,
      self.aData.groupsData,
      self.aData.taskSequence,
      self.aData.baseData.chldCount,
      self.aData.baseData.prntCount,
      dbtData
   )
    
  def Process(self, dgClbk, grClbk, tskClbk, seqClbk): # pass callbacks
    strSeqList = self.jProc.Process()
    for seqStr in strSeqList :
      self.gProc.ProcessPath(seqStr)
      
    self.aData.CreateAirflowObjects(dgClbk, grClbk, tskClbk, seqClbk)
