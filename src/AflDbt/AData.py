from collections import defaultdict

class AData: 
  class AGroupsData:
    def __init__(self):
      #  Task Group Holder ( defaultdict )
      self.taskGroupNameMap = defaultdict(list) # {} # { "task_group" : [TaskName1,TaskName2,...,TaskNameN] }
      self.taskGroupRefMap  = defaultdict(list) # {} # { "task_group_ref" : [refTask1,refTask2,...,refTaskN] }
  class ABaseData:
    def __init__(self, strDbtProcessTests = "True" ):
      # Project name
      self.projectName = None
      
      # flag that we use proceess models and tests too
      self.dbtProcessTests = strDbtProcessTests != "True"
      
      # models list
      self.fullModels = {} #  map {name:full_name}
      self.fullModelsRev  = {} #  reverse map {full_name:name}
    
      # Parent / children count 
      self.chldCount = {} # {"taskName" : count }
      self.prntCount = {} # {"taskName" : count }
      
  def __init__(self, strDbtProcessTests = "True" ):
    # base data 
    self.baseData = self.ABaseData(strDbtProcessTests)
    
    # task map
    self.taskMap = {} # { "taskName" : refTask }
  
    #  task sequnce 
    self.taskSequence = {} # { "|task1|>>|task2|" : (refTask1,refTask2) }
    
    # group data
    self.groupsData = self.AGroupsData()
    
  def CreateAirflowObjects(self, dgClbk, grClbk, tskClbk, seqClbk ):
    if self.baseData.projectName is None:
       return # exit if no project name

    # create a DAG first
    dgParams = self.baseData.projectName
    dgParams += " with tests" if self.baseData.dbtProcessTests else ""
    dgClbk(dgParams) # pass dag creation params
    
    # create groups and then tasks for a group
    for groupId, tasks in self.groupsData.taskGroupRefMap.items():
       # create Aiflow group first
       grClbk(groupId) 
       
       # then create operators per group
       for task in tasks:
        # pass task Callback and GroupId
        # that stores task operator in ATask
        task.CreateOperator(tskClbk, groupId )
    
    # we've got airflow opertators now so we may add sequences
    for value in self.taskSequence.values():
      if isinstance(value, tuple) and len(value) == 2:
        fromT, toT = value
        if fromT is not None and toT is not None:
          seqClbk( fromT.GetOperator(), toT.GetOperator() ) 
