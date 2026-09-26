class ATaskGroupProcessor:

  # inner class ATaskProcessor
  class ATaskProcessor:	

    # inner class ATask 
    class ATask:
      def __init__(self,tName,opConf):
         self.taskName = tName # short tast name = task_id
         self.aflOperator = None # Airflow task which is returned by a callback
         self.projDir = opConf.get("DBT_PROJECT_DIR",None)
         self.dbtCmd = opConf.get("DBT_COMMAND",None)
 
      def CreateOperator(self, tskClbk, groupId ):
         if self.projDir is None or self.dbtCmd is None:
          return

         cmd = ""
         selectItem = ""
         cmdOpt = f"--project-dir {self.projDir} --profiles-dir {self.projDir}"
         if self.taskName.startswith("(run) "):
           cmd = "run"
           selectItem = self.taskName.replace("(run) ","")
           
         if self.taskName.startswith("(test) "):
           cmd = "test"
           selectItem = self.taskName.replace("(test) ","")
           
         execStr = f"{self.dbtCmd} {cmd} {cmdOpt} -s {selectItem}"
         self.aflOperator = tskClbk( self.taskName, execStr, groupId )
         
      def GetOperator(self):
         return self.aflOperator
    # end of inner class ATask 

    def __init__(self, taskMap, opConf): # save ref on taskMap
        self.taskMap = taskMap
        self.opConf = opConf
      
    def CreateOrGetTask(self, taskName): # create a new task or return existing 
      retTask = self.taskMap.get(taskName)
      
      if retTask is None:
        retTask = self.ATask(taskName, self.opConf)
      self.taskMap[taskName] = retTask
      
      return retTask
  # end of inner class ATaskProcessor

  #  ---- >  ATaskGroupProcessor members  <-------
  def __init__(self,taskMap, groupHldr, opConf):  # save ref on taskMap
    self.taskGroupNameMap = groupHldr.taskGroupNameMap  # {"task_group":[TaskName1,TaskName2,...,TaskNameN] }
    self.taskGroupRefMap = groupHldr.taskGroupRefMap
    self.taskProcessor = self.ATaskProcessor(taskMap,opConf)
	
  def AddTaskToGroup( self, taskName, groupName ): 
    taskRef = self.taskProcessor.CreateOrGetTask( taskName )
    retGroupList = self.taskGroupNameMap.get(groupName)

    if retGroupList is None:  # groupName does not exist
      retGroupList = self.taskGroupNameMap[groupName] = [] # create an empty list for names
      self.taskGroupRefMap[groupName] = [] # create an empty list for ref

    if taskName not in retGroupList:
      self.taskGroupNameMap[groupName].append(taskName)
      self.taskGroupRefMap[groupName].append(taskRef)
