from src.AflDbt.ATaskGroupProcessor import ATaskGroupProcessor

class AGraphPathProcessor:
  def __init__(self, taskMap, groupsData, taskSequence, chldCount, prntCount, opConf):
        
    self.groupProc = ATaskGroupProcessor(taskMap, groupsData, opConf)
    self.taskSequence = taskSequence
    self.taskMap = taskMap
    self.chldCount = chldCount
    self.prntCount = prntCount

  def _AddSequence(self,task1name,task2name):
    if task1name is not None:
      seqName = f"|{task1name}|>>|{task2name}|"
      if ( self.taskSequence.get(seqName) is None 
          and self.taskMap.get(task1name) is not None
          and self.taskMap.get(task2name) is not None
      ):
        self.taskSequence[seqName] = (self.taskMap[task1name],self.taskMap[task2name])
    

  def _ProcessTasksList(self,list):
    curGroup = None
    prevTaskName = None
    curPos = 0  # current Position
    maxPos = len(list) - 1 # max Position

    while curPos <= maxPos :
      curParentNo, curTaskName, curChildsNo = list[curPos]
      nextParentNo = nextChildsNo = 0

      if curPos < maxPos and curChildsNo != 0 : # has next item
        nextParentNo, nextTaskName, nextChildsNo = list[curPos+1]
        
      if curGroup is None and curChildsNo == 1 and nextParentNo == 1: # start the task
        curGroup = curTaskName
        if curGroup.startswith("(run) "):
           curGroup = curGroup.replace("(run) ","")

        if curGroup.startswith("(test) "):
           curGroup = curGroup.replace("(test) ","")

      self.groupProc.AddTaskToGroup(curTaskName,curGroup)
      self._AddSequence(prevTaskName,curTaskName)
        
      if curGroup is not None and not ( curChildsNo == 1 and nextParentNo == 1 ): # end the task
        curGroup = None

      curPos += 1 # move to next
      prevTaskName = curTaskName

  def ProcessPath(self,seqStr): # seqStr - comma separated list "task1,task2,...,taskN"
    # convert comma separated string to list with prnt/chld counts
    tskList = []
    for tsk in seqStr.split(","):
      chldCnt = self.chldCount.get(tsk,0)
      prntCnt = self.prntCount.get(tsk,0)
      tskList.append ( (prntCnt,tsk,chldCnt) )

    self._ProcessTasksList(tskList)