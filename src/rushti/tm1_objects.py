"""TM1 object definitions for RushTI.

All TM1 objects (dimensions, cube, process, sample data) are defined here
as Python constants. This eliminates the need for external JSON asset files
and ensures everything ships naturally with `pip install rushti`.

Object Definitions:
    - MEASURE_ELEMENTS: List of measure element names
    - MEASURE_ATTRIBUTES: Dict mapping element name to (inputs, results) flags
    - TASK_ID_ELEMENT_COUNT: Number of elements to generate for task_id dimension (1..N)
    - RUN_ID_SEED_ELEMENTS: Seed elements for run_id dimension
    - WORKFLOW_SEED_ELEMENTS: Seed elements for workflow dimension
    - PROCESS_DEFINITION: Full TI process definition for }rushti.load.results
    - SAMPLE_DATA: Sample cube data for demonstration taskfiles
"""

# ---------------------------------------------------------------------------
# Measure dimension
# ---------------------------------------------------------------------------

MEASURE_ELEMENTS = [
    "instance",
    "process",
    "parameters",
    "status",
    "start_time",
    "end_time",
    "duration_seconds",
    "retries",
    "retry_count",
    "error_message",
    "predecessors",
    "stage",
    "safe_retry",
    "timeout",
    "cancel_at_timeout",
    "require_predecessor_success",
    "succeed_on_minor_errors",
    "wait",
]

# Maps element name -> {"inputs": "Y"|"", "results": "Y"|""}
MEASURE_ATTRIBUTES = {
    "instance": {"inputs": "Y", "results": "Y"},
    "process": {"inputs": "Y", "results": "Y"},
    "parameters": {"inputs": "Y", "results": "Y"},
    "status": {"inputs": "", "results": "Y"},
    "start_time": {"inputs": "", "results": "Y"},
    "end_time": {"inputs": "", "results": "Y"},
    "duration_seconds": {"inputs": "", "results": "Y"},
    "retries": {"inputs": "Y", "results": ""},
    "retry_count": {"inputs": "", "results": "Y"},
    "error_message": {"inputs": "", "results": "Y"},
    "predecessors": {"inputs": "Y", "results": "Y"},
    "stage": {"inputs": "Y", "results": "Y"},
    "safe_retry": {"inputs": "Y", "results": "Y"},
    "timeout": {"inputs": "Y", "results": "Y"},
    "cancel_at_timeout": {"inputs": "Y", "results": "Y"},
    "require_predecessor_success": {"inputs": "Y", "results": "Y"},
    "succeed_on_minor_errors": {"inputs": "Y", "results": "Y"},
    "wait": {"inputs": "Y", "results": ""},
}

# ---------------------------------------------------------------------------
# Task ID dimension
# ---------------------------------------------------------------------------

TASK_ID_ELEMENT_COUNT = 5000

# ---------------------------------------------------------------------------
# Run ID dimension — seed elements
# ---------------------------------------------------------------------------

RUN_ID_SEED_ELEMENTS = ["Input"]

# ---------------------------------------------------------------------------
# Workflow dimension — seed elements
# ---------------------------------------------------------------------------

WORKFLOW_SEED_ELEMENTS = ["Sample_Stage_Mode", "Sample_Optimal_Mode"]

# ---------------------------------------------------------------------------
# TI Process: }rushti.load.results
# ---------------------------------------------------------------------------

PROCESS_PROLOG = r"""#################################################################################################
##~~Join the bedrock TM1 community on GitHub https://github.com/cubewise-code/bedrock Ver 4.0~~##
#################################################################################################

#****Begin: Generated Statements***
#****End: Generated Statements****

#################################################################################################
### CHANGE HISTORY:
### MODIFICATION DATE 	CHANGED BY 	    COMMENT
### 2026-01-15 		    Nicolas Bisurgi 	Creation of Process
### YYYY-MM-DD 		    Developer Name 	Reason for modification here
#################################################################################################
#Region @DOC
# Description:
# This process will load RushTI result files into a TM1 cube

# Use case:
# Storing RushTI execution stats in a TM1 Cube

# Note:
# * This process assumes a there is a rushti compliant cube and dimensions. If you don't have it
# * please run: rushti.py build --tm1-instance tm1srv01
#EndRegion @DOC
#################################################################################################

#################################################################################################
#Region Process Declarations
### Process Parameters
# a short description of what the process does goes here in cAction variable, e.g. "copied data from cube A to cube B". This will be written to the message log if pLogOutput=1
cAction             = 'loading rushti stats into ' | pTargetCube;
cParamArray         = '';
# to use the parameter array remove the line above and uncomment the line below, adding the needed parameters in the provided format
#cParamArray         = 'pLogOutput:%pLogOutput%, pTemp:%pTemp%';

### Global Variables
StringGlobalVariable('sProcessReturnCode');
NumericGlobalVariable('nProcessReturnCode');

### Standard Constants
cThisProcName       = GetProcessName();
cUserName           = TM1User();
cTimeStamp          = TimSt( Now, '\Y\m\d\h\i\s' );
cRandomInt          = NumberToString( INT( RAND( ) * 1000 ));
cTempObjName        = Expand('%cThisProcName%_%cTimeStamp%_%cRandomInt%');
cViewClr            = '}bedrock_clear_' | cTempObjName;
cViewSrc            = '}bedrock_source_' | cTempObjName;
cMsgErrorLevel      = 'ERROR';
cMsgErrorContent    = 'Process:%cThisProcName% ErrorMsg:%sMessage%';
cLogInfo            = 'Process:%cThisProcName% run with parameters %cParamArray%';
sDelimEleStart      = '¦';
sDelimDim           = '&';
sDelimEle           = '+';
nProcessReturnCode  = 0;
nErrors             = 0;
nMetadataRecordCount= 0;
nDataRecordCount    = 0;

### Process Specific Constants
cFileSrc            = pSourceFile;
cCubeTgt            = pTargetCube;

#EndRegion Process Declarations
#################################################################################################

### LogOutput parameters
IF( pLogoutput = 1 );
    LogOutput('INFO', Expand( cLogInfo ) );
ENDIF;

#################################################################################################
#Region Validate Parameters

# pLogOutput
If( pLogOutput >= 1 );
    pLogOutput = 1;
Else;
    pLogOutput = 0;
EndIf;

# Validate source file
If( Trim( cFileSrc ) @= '' );
    nErrors = nErrors + 1;
    sMessage = 'No source cfileube specified.';
    LogOutput( cMsgErrorLevel, Expand( cMsgErrorContent ) );
ElseIf( FileExists( cFileSrc ) = 0 );
    nErrors = nErrors + 1;
    sMessage = Expand( 'Invalid source file specified: %cFileSrc%.');
    LogOutput( cMsgErrorLevel, Expand( cMsgErrorContent ) );
EndIf;

# Validate target cube
If( Trim( cCubeTgt ) @= '' );
    nErrors = nErrors + 1;
    sMessage = 'No target cube specified.';
    LogOutput( cMsgErrorLevel, Expand( cMsgErrorContent ) );
ElseIf( CubeExists( cCubeTgt ) = 0 );
    nErrors = nErrors + 1;
    sMessage = Expand( 'Invalid target cube specified: %cCubeTgt%.');
    LogOutput( cMsgErrorLevel, Expand( cMsgErrorContent ) );
EndIf;

# If any parameters fail validation then set data source of process to null and go directly to epilog
If( nErrors > 0 );
    DataSourceType = 'NULL';
    If( pStrictErrorHandling = 1 );
        ProcessQuit;
    Else;
        ProcessBreak;
    EndIf;
EndIf;

#################################################################################################
#EndRegion Validate Parameters

### If required switch transaction logging off (this should be done AFTER the escape/reject if parameters fail validation and BEFORE the zero out commences)
nCubeLogChanges = CubeGetLogChanges( cCubeTgt );
CubeSetLogChanges( cCubeTgt, 0 );

#################################################################################################
#Region - DataSource


### Assign data source
If( nErrors = 0 );
    DatasourceType          = 'CHARACTERDELIMITED';
    DatasourceNameForServer = cFileSrc;
    DatasourceASCIIHeaderRecords = 1;
    DatasourceASCIIDelimiter=',';
    DatasourceASCIIDecimalSeparator='.';
    DatasourceASCIIThousandSeparator='';
EndIf;

#EndRegion - DataSource
#################################################################################################
"""

PROCESS_METADATA = r"""#****Begin: Generated Statements***
#****End: Generated Statements****

If( pLogOutput >= 1 );
   nMetadataRecordCount = nMetadataRecordCount + 1;
EndIf;

# Add workflow if it doesn't exist
if (DimensionElementExists(pWorkflow_Dim, vworkflow) = 0);
  DimensionElementInsertDirect(pWorkflow_Dim, '', vworkflow, 'N');
endif;

# Add task id if it doesn't exist
if (DimensionElementExists(pTaskId_Dim, vtask_id) = 0);
  DimensionElementInsertDirect(pTaskId_Dim, '', vtask_id, 'N');
endif;

# Add run id if it doesn't exist
if (DimensionElementExists(pRunId_Dim, vrun_id) = 0);
  DimensionElementInsertDirect(pRunId_Dim, '', vrun_id, 'N');
endif;"""

PROCESS_DATA = r"""#****Begin: Generated Statements***
#****End: Generated Statements****

If( pLogOutput >= 1 );
   nDataRecordCount = nDataRecordCount + 1;
EndIf;

CellPutS(vinstance, pTargetCube, vworkflow, vtask_id, vrun_id, 'instance');
CellPutS(vprocess, pTargetCube, vworkflow, vtask_id, vrun_id, 'process');
CellPutS(vparameters, pTargetCube, vworkflow, vtask_id, vrun_id, 'parameters');
CellPutS(vstatus, pTargetCube, vworkflow, vtask_id, vrun_id, 'status');
CellPutS(vstart_time, pTargetCube, vworkflow, vtask_id, vrun_id, 'start_time');
CellPutS(vend_time, pTargetCube, vworkflow, vtask_id, vrun_id, 'end_time');
CellPutS(vduration_seconds, pTargetCube, vworkflow, vtask_id, vrun_id, 'duration_seconds');
CellPutS(vretries, pTargetCube, vworkflow, vtask_id, vrun_id, 'retries');
CellPutS(vretry_count, pTargetCube, vworkflow, vtask_id, vrun_id, 'retry_count');
CellPutS(verror_message, pTargetCube, vworkflow, vtask_id, vrun_id, 'error_message');
CellPutS(vpredecessors, pTargetCube, vworkflow, vtask_id, vrun_id, 'predecessors');
CellPutS(vstage, pTargetCube, vworkflow, vtask_id, vrun_id, 'stage');
CellPutS(vsafe_retry, pTargetCube, vworkflow, vtask_id, vrun_id, 'safe_retry');
CellPutS(vtimeout, pTargetCube, vworkflow, vtask_id, vrun_id, 'timeout');
CellPutS(vcancel_at_timeout, pTargetCube, vworkflow, vtask_id, vrun_id, 'cancel_at_timeout');
CellPutS(vrequire_predecessor_success, pTargetCube, vworkflow, vtask_id, vrun_id, 'require_predecessor_success');
CellPutS(vsucceed_on_minor_errors, pTargetCube, vworkflow, vtask_id, vrun_id, 'succeed_on_minor_errors');
"""

PROCESS_EPILOG = r"""#################################################################################################
##~~Join the bedrock TM1 community on GitHub https://github.com/cubewise-code/bedrock Ver 4.0~~##
#################################################################################################

#****Begin: Generated Statements***
#****End: Generated Statements****

### If required delete the source file
if(pDeleteSourceFile=1);
  ASCIIDelete(pSourceFile);
endif;

### If required switch transaction logging back on
CubeSetLogChanges( cCubeTgt, nCubeLogChanges );

### Return code & final error message handling
If( nErrors > 0 );
    sMessage = 'the process incurred at least 1 error. Please see above lines in this file for more details.';
    nProcessReturnCode = 0;
    LogOutput( cMsgErrorLevel, Expand( cMsgErrorContent ) );
    sProcessReturnCode = Expand( '%sProcessReturnCode% Process:%cThisProcName% completed with errors. Check tm1server.log for details.' );
    If( pStrictErrorHandling = 1 );
        ProcessQuit;
    EndIf;
Else;
    sProcessAction = Expand( 'Process:%cThisProcName% successfully %cAction%. %nDataRecordCount% records processed.' );
    sProcessReturnCode = Expand( '%sProcessReturnCode% %sProcessAction%' );
    nProcessReturnCode = 1;
    If( pLogoutput = 1 );
        LogOutput('INFO', Expand( sProcessAction ) );
    EndIf;
EndIf;

### End Epilog ###"""

PROCESS_PARAMETERS = [
    {
        "Name": "pSourceFile",
        "Prompt": "REQUIRED: Source file with RushTI stats (defaults to data/ directory)",
        "Value": "",
        "Type": "String",
    },
    {
        "Name": "pTargetCube",
        "Prompt": "OPTIONAL: Target cube (defaults to rushti",
        "Value": "rushti",
        "Type": "String",
    },
    {
        "Name": "pWorkflow_Dim",
        "Prompt": "OPTIONAL: Name of the dimension containing the workflows",
        "Value": "rushti_workflow",
        "Type": "String",
    },
    {
        "Name": "pTaskId_Dim",
        "Prompt": "OPTIONAL: Name if the dimension containing the task ids of a taskfile",
        "Value": "rushti_task_id",
        "Type": "String",
    },
    {
        "Name": "pRunId_Dim",
        "Prompt": "OPTIONAL: Name if the dimension containing the run ids of a task",
        "Value": "rushti_run_id",
        "Type": "String",
    },
    {
        "Name": "pStrictErrorHandling",
        "Prompt": "OPTIONAL: On encountering any error, exit with major error status by ProcessQuit after writing to the server message log (Boolean True = 1)",
        "Value": 0,
        "Type": "Numeric",
    },
    {
        "Name": "pLogOutput",
        "Prompt": "OPTIONAL:Write status messages to tm1server.log file?",
        "Value": 0,
        "Type": "Numeric",
    },
    {
        "Name": "pDeleteSourceFile",
        "Prompt": "OPTIONAL:Should the result file be deleted after it loads? (1=True, 0=False)",
        "Value": 0,
        "Type": "Numeric",
    },
]

PROCESS_VARIABLES = [
    "vworkflow",
    "vrun_id",
    "vtask_id",
    "vinstance",
    "vprocess",
    "vparameters",
    "vstatus",
    "vstart_time",
    "vend_time",
    "vduration_seconds",
    "vretries",
    "vretry_count",
    "verror_message",
    "vpredecessors",
    "vstage",
    "vsafe_retry",
    "vtimeout",
    "vcancel_at_timeout",
    "vrequire_predecessor_success",
    "vsucceed_on_minor_errors",
]

PROCESS_DATASOURCE = {
    "Type": "ASCII",
    "asciiDecimalSeparator": ".",
    "asciiDelimiterChar": ",",
    "asciiDelimiterType": "Character",
    "asciiHeaderRecords": 1,
    "asciiQuoteCharacter": '"',
    "asciiThousandSeparator": ",",
    "dataSourceNameForClient": "",
    "dataSourceNameForServer": "",
}

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_DATA_OPTIMAL_MODE = [
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "1",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "1",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "1",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=1",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "1",
        "run_id": "Input",
        "measure": "stage",
        "value": "load",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "2",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "2",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "2",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=2",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "2",
        "run_id": "Input",
        "measure": "stage",
        "value": "load",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "3",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "3",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "3",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=5",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "3",
        "run_id": "Input",
        "measure": "predecessors",
        "value": "2",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "3",
        "run_id": "Input",
        "measure": "stage",
        "value": "transfer",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "3",
        "run_id": "Input",
        "measure": "timeout",
        "value": "10",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "4",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "4",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "4",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=1",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "4",
        "run_id": "Input",
        "measure": "predecessors",
        "value": "2",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "4",
        "run_id": "Input",
        "measure": "stage",
        "value": "transfer",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "4",
        "run_id": "Input",
        "measure": "timeout",
        "value": "10",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "4",
        "run_id": "Input",
        "measure": "cancel_at_timeout",
        "value": "true",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "5",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "5",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "5",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=1",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "5",
        "run_id": "Input",
        "measure": "predecessors",
        "value": "3",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "5",
        "run_id": "Input",
        "measure": "stage",
        "value": "calc",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "6",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "6",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "6",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=1",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "6",
        "run_id": "Input",
        "measure": "predecessors",
        "value": "1,5",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "6",
        "run_id": "Input",
        "measure": "stage",
        "value": "export",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "7",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "7",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "7",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=1",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "7",
        "run_id": "Input",
        "measure": "predecessors",
        "value": "1,5",
    },
    {
        "workflow": "Sample_Optimal_Mode",
        "task_id": "7",
        "run_id": "Input",
        "measure": "stage",
        "value": "export",
    },
]

SAMPLE_DATA_STAGE_MODE = [
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "1",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "1",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "1",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=1",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "2",
        "run_id": "Input",
        "measure": "wait",
        "value": "true",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "3",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "3",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "3",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=2",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "4",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "4",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "4",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=3",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "5",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "5",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "5",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=4",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "6",
        "run_id": "Input",
        "measure": "wait",
        "value": "true",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "7",
        "run_id": "Input",
        "measure": "instance",
        "value": "tm1srv01",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "7",
        "run_id": "Input",
        "measure": "process",
        "value": "}bedrock.server.wait",
    },
    {
        "workflow": "Sample_Stage_Mode",
        "task_id": "7",
        "run_id": "Input",
        "measure": "parameters",
        "value": "pWaitSec=5",
    },
]

SAMPLE_DATA = {
    "Sample_Optimal_Mode": SAMPLE_DATA_OPTIMAL_MODE,
    "Sample_Stage_Mode": SAMPLE_DATA_STAGE_MODE,
}
