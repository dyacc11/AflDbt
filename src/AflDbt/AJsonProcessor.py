import json
import pandas as pd
import numpy as np

class AJsonProcessor:
  def __init__( self, baseData, opConf ): # get constants from config, read data into holder

    self.manifest = None
    self.chkModel = None
    self.chkTst = None
    
    # Externals refrerences
    self.baseData = baseData

    # Config Data
    self.dbtFilePath = opConf.get("DBT_MANIFEST_PATH","")
    
  
  def _ReadJson(self): # just read file into the variable
     
     with open(self.dbtFilePath, 'r') as f:
       self.manifest = json.loads(f.read())
       self.baseData.projectName = self.manifest["metadata"]["project_name"] # get project name
       self.chkModel = f"model.{self.baseData.projectName}."
       self.chkTst = f"test.{self.baseData.projectName}."

  def _GetModelsList(self):
    for key, value in self.manifest["nodes"].items():
      k = key
      v = value["name"]
      if self.chkModel in key:
        v = "(run) " + v
        self.baseData.fullModels[v] = key
        self.baseData.fullModelsRev[key] = v
		
      if self.chkTst in key and self.baseData.dbtProcessTests:
        v = "(test) " + v
        print(v) 
        self.baseData.fullModels[v] = key
        self.baseData.fullModelsRev[key] = v

  # section is:  "parent_map" or "child_map" 
  # return DataFrame "level_1","level_2" columns for hierarchy in the section
  def _GetHierarchyBySection(self, section ): 
  
    df = pd.DataFrame()  

    for key, val in self.baseData.fullModels.items():
      lst = self.manifest[section][val]
      if (lst):
        bProcessed = False
        for item in lst:
          if self.chkModel in item:
            item = self.baseData.fullModelsRev[item]
            res = {'level_1':key, 'level_2':item}
            df = pd.concat([df, pd.DataFrame([res])])
            bProcessed = True
            continue
        
          if self.chkTst in item :
            if self.baseData.dbtProcessTests:
              item = self.baseData.fullModelsRev[item]
              res = {'level_1':key, 'level_2':item}
              df = pd.concat([df, pd.DataFrame([res])])
              bProcessed = True
            continue

          # do not match model or test
          if self.baseData.dbtProcessTests:
            res = {'level_1':key, 'level_2':item}
            df = pd.concat([df, pd.DataFrame([res])])
            bProcessed = True

        if bProcessed == False:
          # case when we have a section with items and no any item being handled
          res = {'level_1':key, 'level_2':np.nan}
          df = pd.concat([df, pd.DataFrame([res])])
      else:
        res = {'level_1':key, 'level_2':np.nan}
        df = pd.concat([df, pd.DataFrame([res])])
      
    df_sorted = df.sort_values(by='level_1')
    return df

  def _MakeSequences(self, df):
    lid = 2
    mrgDF = df
    rwsOrig = len(df)
    while True:
      mrgDF = pd.merge(mrgDF, df, left_on=[f"level_{lid}"], right_on=["level_1"], how="left")
      mrgDF.rename(columns={"level_1_x": "level_1","level_2_x": "level_2", "level_2_y": f"level_{lid+1}"}, inplace=True)
      
      mrgDF = mrgDF.drop(columns="level_1_y")
      flag = bool(mrgDF[f"level_{lid+1}"].isna().all())
      if flag is True:
        mrgDF = mrgDF.drop(columns=f"level_{lid+1}")
        break # 
      if lid > rwsOrig: # data frame has a cycle - breake iteration and clean
        mrgDF = pd.DataFrame()
        break
      lid = lid+1
    return mrgDF

  def _RemoveUsedSeqences(self, df):
    lid = len(df.columns)
    if lid == 0:
      return pd.DataFrame()

    for i in range(lid , 2, -1):
      chkDF = (
        df[[f"level_{i-1}", f"level_{i}"]]
        .rename(columns={f"level_{i-1}": "level_1", f"level_{i}": "level_2"})
      )
      merged = df.merge(chkDF, on=['level_1', 'level_2'], how='left', indicator=True)
      chkDF = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    return chkDF

  def Process(self):
    # read files and get all models
    self._ReadJson()
    self._GetModelsList()
  
    # fill up parents map 
    df = self._GetHierarchyBySection("parent_map" )
    self.baseData.prntCount.update(df.groupby("level_1")["level_2"].count().to_dict())

    # fill up children map 
    df = self._GetHierarchyBySection("child_map" )
    self.baseData.chldCount.update(df.groupby("level_1")["level_2"].count().to_dict())

    # use children map to build up sequences
    df = self._MakeSequences(df)
    df = self._RemoveUsedSeqences(df)

    lvlCols =  df.columns.tolist() 
    strSeqList = df[lvlCols].stack().dropna().astype(str).groupby(level=0).agg(",".join).tolist()
    strSeqList.sort()
    
    return strSeqList