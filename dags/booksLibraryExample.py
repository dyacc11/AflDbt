# ============================================================================
# Logging initialization
# ============================================================================
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = True

# ============================================================================
# Dynamic paths calculations
# ============================================================================
from pathlib import Path
import sys

logger.debug("sys.path before: %s",sys.path)

# get the ../dags absolute path
projectRoot = Path(__file__).resolve().parent.parent

# append only if it's not in the sys.path
if projectRoot not in sys.path:
    sys.path.append(str(projectRoot))
	
logger.debug("sys.path after: %s",sys.path)

# values for dbtData Configuration
dbtRoot = projectRoot / "dbt" / "books_library" 
dbtManifest = str(dbtRoot / "target" / "manifest.json")
dbtRoot = str(dbtRoot)

# Build the full path of the "dbt" executable
dbtCmd = projectRoot.parent / "dbtVe" / "bin" / "dbt"
logger.debug(f"The full path to dbt is: {dbtCmd}")
#dbtCmd = shutil.which("dbt")
#if dbtCmd:
#    logger.debug(f"The full path to dbt is: {dbtCmd}")
#else:
#    sys.exit("Error: 'dbt' executable not found in the current environment. Aborting.")	

# ============================================================================    
# Main imports
# ============================================================================
from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator

from src.AflDbt.AMain import AMain

# ============================================================================
# Global variables to hold Airflow objects created by callbacks
# ============================================================================
_dag = None
_groups = {}

# ============================================================================
# CALLBACK FUNCTIONS
# ============================================================================
def CreateDagCallback(projectName):
    global _dag
    
    dagId = f"dbt_{projectName}"
    
    _dag = DAG(
        dag_id=dagId,
        start_date=datetime(2026,1,1),
        schedule= None,
        catchup=False,
        description=f"Auto-generated DAG from dbt project: {projectName}"
    )
    
    logger.debug(f"Created DAG: {dagId}")

def CreateTaskGroupCallback(groupId):
    global _dag
    global _groups
    
    if groupId is not None:
        grp = TaskGroup(
            group_id=groupId,
            dag=_dag,
            tooltip=f"Task group: {groupId}"
        )

        _groups[groupId] = grp
        logger.debug(f"Created TaskGroup: {groupId}")

def CreateTaskCallback(taskName, execStr, groupId):
    global _dag
    global _groups
    
    # Clean task name for Airflow (remove special characters)
    task_id = taskName.replace(" ", "_").replace("(", "").replace(")", "")
    
    # Get the TaskGroup for this task
    grp = None
    if groupId is not None:
        grp = _groups.get(groupId)

    task = BashOperator(
        task_id=task_id,
        bash_command= execStr,  # e.g., "dbt run --project-dir ... -s s_task"
        dag=_dag,
        env={
            "AFLDBT_DBT_DB": f"{dbtRoot}/", # Trailing slash included
        },
        task_group=grp,
    )

    envVal = f"{dbtRoot}/"
    logger.debug(f"Created task: {task_id} in group: {groupId}")
    logger.debug(f" (Would execute: {execStr})")
    logger.debug(f" (env: {envVal})")

    return task

def CreateSequenceCallback(from_task, to_task):
    from_task >> to_task
    logger.debug(f"Created sequence: {from_task.task_id} >> {to_task.task_id}")

# ============================================================================
# INSTANTIATE AMain WITH CONFIGURATION
# ============================================================================

dbtData = {
 "DBT_PROJECT_DIR":str(dbtRoot),
 "DBT_COMMAND":dbtCmd,
 "DBT_MANIFEST_PATH":str(dbtManifest),
 "SKIP_DBT_TEST":"True"
}

logger.info("create AMain class instance")
mainProcessor = AMain(dbtData)

# Run process 
logger.info("running AMain.Process")
mainProcessor.Process(
  dgClbk=CreateDagCallback,
  grClbk=CreateTaskGroupCallback,
  tskClbk=CreateTaskCallback,
  seqClbk=CreateSequenceCallback 
)

logger.info("running AMain.Process ... Done")
# ============================================================================
#  MAIN PROCEDURE - FOR DEBUGGING
# ============================================================================

if __name__ == "__main__":
    from airflow.utils import timezone
    from airflow.utils.state import State
    from airflow.timetables.simple import OnceTimetable

    # 1. Get current time
    now = timezone.utcnow()
    logger.debug(f"running DAG '{_dag.dag_id}' in isolated local mode...")

    # 2.HACK: Temporarily change the schedule from None to Once
    # This will cause BackfillJobRunner to see the start_date 
    _dag.timetable = OnceTimetable()

    # 3. Clear previous run if we have one
    _dag.clear(
        start_date=now,
        end_date=now,
        dag_run_state=State.QUEUED
    )

    # 4. Run local Airflow runner
    _dag.run(
        start_date=now,
        end_date=now,
        ignore_first_depends_on_past=True,
        verbose=True
    )
    logger.debug(f"running DAG '{_dag.dag_id}' ... completed")
