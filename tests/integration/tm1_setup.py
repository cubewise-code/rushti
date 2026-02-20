"""Auto-create required TM1 objects for integration tests.

This module ensures all TM1 objects needed by integration tests exist on
the target instance. All functions are idempotent — they create objects only
if they don't already exist.

Required objects:
- rushti.dimension.counter — dimension with elements "1" through "10"
  (used by expandable task tests with MDX expressions)
- rushti cube + dimensions — created via build_logging_objects()
  (used by cube-based task read/write tests)
- }rushti.load.results — TI process for loading CSV results into cube
  (used by auto-load results tests)
"""

import logging

from TM1py import TM1Service
from TM1py.Objects import Dimension, Hierarchy

logger = logging.getLogger(__name__)


def ensure_counter_dimension(
    tm1: TM1Service,
    dim_name: str = "rushti.dimension.counter",
    count: int = 10,
) -> bool:
    """Create the counter dimension with string elements 1..count if it doesn't exist.

    This dimension is used by expandable task tests. MDX expressions like
    {TM1SUBSETALL([rushti.dimension.counter].[rushti.dimension.counter])}
    expand to elements "1", "2", ..., "10".

    Returns True if created, False if already exists.
    """
    if tm1.dimensions.exists(dim_name):
        logger.debug(f"Counter dimension already exists: {dim_name}")
        return False

    hierarchy = Hierarchy(name=dim_name, dimension_name=dim_name)
    for i in range(1, count + 1):
        hierarchy.add_element(element_name=str(i), element_type="String")

    dimension = Dimension(name=dim_name, hierarchies=[hierarchy])
    tm1.dimensions.create(dimension)
    logger.info(f"Created counter dimension: {dim_name} with {count} elements")
    return True


def ensure_rushti_cube(tm1: TM1Service, **tm1_names) -> bool:
    """Create the rushti cube and dimensions if they don't exist.

    Uses the existing build_logging_objects() function from rushti.tm1_build.
    Accepts optional keyword arguments (cube_name, dim_workflow, dim_task,
    dim_run, dim_measure) to override default TM1 object names.

    Returns True if any objects were created, False if all existed.
    """
    from rushti.tm1_build import build_logging_objects, verify_logging_objects

    status = verify_logging_objects(tm1, **tm1_names)
    if all(status.values()):
        logger.debug("All rushti logging objects already exist")
        return False

    results = build_logging_objects(tm1, force=False, **tm1_names)
    created = any(results.values())
    if created:
        logger.info(f"Created rushti logging objects: {results}")
    return created


def ensure_load_results_process(
    tm1: TM1Service,
    process_name: str = "}rushti.load.results",
) -> bool:
    """Create the }rushti.load.results TI process if it doesn't exist.

    Uses the existing process definition from tm1_objects.py, which is the
    same process created by the 'rushti build' command.

    Returns True if created, False if already exists.
    """
    if tm1.processes.exists(process_name):
        logger.debug(f"Process already exists: {process_name}")
        return False

    from TM1py.Objects import Process
    from rushti.tm1_objects import (
        PROCESS_DATA,
        PROCESS_DATASOURCE,
        PROCESS_EPILOG,
        PROCESS_METADATA,
        PROCESS_PARAMETERS,
        PROCESS_PROLOG,
        PROCESS_VARIABLES,
    )

    process = Process(
        name=process_name,
        prolog_procedure=PROCESS_PROLOG,
        metadata_procedure=PROCESS_METADATA,
        data_procedure=PROCESS_DATA,
        epilog_procedure=PROCESS_EPILOG,
        datasource_type=PROCESS_DATASOURCE["Type"],
        datasource_ascii_decimal_separator=PROCESS_DATASOURCE["asciiDecimalSeparator"],
        datasource_ascii_delimiter_char=PROCESS_DATASOURCE["asciiDelimiterChar"],
        datasource_ascii_delimiter_type=PROCESS_DATASOURCE["asciiDelimiterType"],
        datasource_ascii_header_records=PROCESS_DATASOURCE["asciiHeaderRecords"],
        datasource_ascii_quote_character=PROCESS_DATASOURCE["asciiQuoteCharacter"],
        datasource_ascii_thousand_separator=PROCESS_DATASOURCE["asciiThousandSeparator"],
        datasource_data_source_name_for_client=PROCESS_DATASOURCE["dataSourceNameForClient"],
        datasource_data_source_name_for_server=PROCESS_DATASOURCE["dataSourceNameForServer"],
    )

    for param in PROCESS_PARAMETERS:
        process.add_parameter(param["Name"], param["Prompt"], param["Value"])

    for var_name in PROCESS_VARIABLES:
        process.add_variable(var_name, "String")

    tm1.processes.create(process)
    logger.info(f"Created TI process: {process_name}")
    return True


def ensure_sample_data(tm1: TM1Service, **tm1_names) -> bool:
    """Populate sample task data in the rushti cube if not already present.

    This ensures the Sample_Optimal_Mode and Sample_Stage_Mode workflows
    exist in the cube, which are required by cube source tests.

    Returns True if data was populated, False if already present.
    """
    from rushti.tm1_build import _populate_sample_data

    cube_name = tm1_names.get("cube_name", "rushti")

    # Check if sample data already exists by reading one cell
    try:
        value = tm1.cells.get_value(cube_name, "Sample_Optimal_Mode,Input,1,instance")
        if value:
            logger.debug("Sample data already exists in cube")
            return False
    except Exception:
        pass  # Element or cube doesn't exist, populate data

    try:
        results = _populate_sample_data(tm1, cube_name)
        if results:
            logger.info(f"Populated sample data: {results}")
            return True
    except Exception as e:
        logger.warning(f"Failed to populate sample data: {e}")

    return False


def setup_tm1_test_objects(tm1: TM1Service, **tm1_names) -> None:
    """Ensure all required test objects exist on a TM1 instance.

    This is the main entry point called by the conftest.py fixture.
    All operations are idempotent and safe for shared instances.

    Accepts optional keyword arguments (cube_name, dim_workflow, dim_task,
    dim_run, dim_measure) to override default TM1 object names.
    """
    ensure_counter_dimension(tm1)
    ensure_rushti_cube(tm1, **tm1_names)
    ensure_sample_data(tm1, **tm1_names)
    ensure_load_results_process(tm1)
