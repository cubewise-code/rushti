"""TM1 build module for RushTI.

This module provides functionality to create TM1 objects required for
logging and other RushTI features.

Usage:
    python rushti build --tm1-instance tm1srv01 [--force]

Creates:
    - Dimensions: rushti_workflow, rushti_task_id, rushti_run_id, rushti_measure
      (with attributes and MDX subsets)
    - Cube: rushti (with custom dimension names if configured in settings.ini)
    - MDX Views: Sample_Normal_Mode and Sample_Optimal_Mode
    - Process: }rushti.load.results (for loading execution results)
    - Sample data for demonstration taskfiles

All objects are defined as Python constants in tm1_objects.py. Custom dimension
and cube names can be configured in settings.ini.
"""

import logging
from typing import Dict

from TM1py import TM1Service
from TM1py.Objects import (
    Cube,
    Dimension,
    ElementAttribute,
    Hierarchy,
    MDXView,
    Process,
    Subset,
)

from rushti.tm1_objects import (
    MEASURE_ATTRIBUTES,
    MEASURE_ELEMENTS,
    PROCESS_DATA,
    PROCESS_DATASOURCE,
    PROCESS_EPILOG,
    PROCESS_METADATA,
    PROCESS_PARAMETERS,
    PROCESS_PROLOG,
    PROCESS_VARIABLES,
    RUN_ID_SEED_ELEMENTS,
    SAMPLE_DATA,
    TASK_ID_ELEMENT_COUNT,
    WORKFLOW_SEED_ELEMENTS,
)

logger = logging.getLogger(__name__)


def build_logging_objects(
    tm1: TM1Service,
    force: bool = False,
    cube_name: str = "rushti",
    dim_workflow: str = "rushti_workflow",
    dim_task: str = "rushti_task_id",
    dim_run: str = "rushti_run_id",
    dim_measure: str = "rushti_measure",
) -> Dict[str, bool]:
    """Build all TM1 objects required for RushTI logging.

    Objects are created programmatically from definitions in tm1_objects.py.
    No external asset files are needed.

    :param tm1: TM1Service instance
    :param force: If True, delete and recreate existing objects
    :param cube_name: Name of the cube to create
    :param dim_workflow: Name of the workflow dimension
    :param dim_task: Name of the task dimension
    :param dim_run: Name of the run dimension
    :param dim_measure: Name of the measure dimension
    :return: Dictionary of object names and whether they were created
    """
    results = {}

    # Create dimensions
    for dim_name, creator in [
        (dim_workflow, _create_workflow_dimension),
        (dim_task, _create_task_id_dimension),
        (dim_run, _create_run_id_dimension),
        (dim_measure, _create_measure_dimension),
    ]:
        try:
            created = _create_dimension(tm1, dim_name, creator, force=force)
            results[dim_name] = created
            if created:
                logger.info(f"Created dimension: {dim_name}")
            else:
                logger.info(f"Dimension exists: {dim_name}")
        except Exception as e:
            logger.error(f"Failed to create dimension {dim_name}: {e}")
            results[dim_name] = False

    # Create MDX-based subsets for measure dimension (before cube)
    try:
        subset_results = _create_mdx_subsets(tm1, dim_measure, force=force)
        for subset_name, created in subset_results.items():
            results[f"{dim_measure}/{subset_name}"] = created
        if any(subset_results.values()):
            logger.info("Created MDX-based subsets for measure dimension")
        else:
            logger.debug("MDX subsets already exist (skipped)")
    except Exception as e:
        logger.warning(f"Failed to create MDX subsets: {e}")
        results[f"{dim_measure}/rushti_inputs_opt"] = False
        results[f"{dim_measure}/rushti_inputs_norm"] = False
        results[f"{dim_measure}/rushti_results"] = False

    # Create cube (after dimensions and subsets)
    try:
        created = _create_cube(
            tm1, cube_name, dim_workflow, dim_task, dim_run, dim_measure, force=force
        )
        results[cube_name] = created
        if created:
            logger.info(f"Created cube: {cube_name}")
        else:
            logger.info(f"Cube exists: {cube_name}")
    except Exception as e:
        logger.error(f"Failed to create cube {cube_name}: {e}")
        results[cube_name] = False

    # Create MDX-based views for the cube
    try:
        view_results = _create_mdx_views(
            tm1, cube_name, dim_workflow, dim_task, dim_run, dim_measure, force=force
        )
        for view_name, created in view_results.items():
            results[f"{cube_name}/{view_name}"] = created
        if any(view_results.values()):
            logger.info("Created MDX-based views for cube")
        else:
            logger.debug("MDX views already exist (skipped)")
    except Exception as e:
        logger.warning(
            f"Failed to create MDX views programmatically: {e}. "
            f"Views can be created manually in TM1 if needed."
        )
        results[f"{cube_name}/Sample_Normal_Mode"] = False
        results[f"{cube_name}/Sample_Optimal_Mode"] = False

    # Create process
    try:
        process_name = "}rushti.load.results"
        created = _create_process(tm1, process_name, force=force)
        results[process_name] = created
        if created:
            logger.info(f"Created process: {process_name}")
    except Exception as e:
        logger.warning(f"Failed to create process: {e}")

    # Populate sample data
    try:
        sample_results = _populate_sample_data(tm1, cube_name)
        for workflow, count in sample_results.items():
            results[f"SampleData/{workflow}"] = count > 0
    except Exception as e:
        logger.warning(f"Failed to populate sample data: {e}")

    return results


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------


def _create_dimension(
    tm1: TM1Service,
    dim_name: str,
    builder_fn,
    force: bool = False,
) -> bool:
    """Create or update a dimension using the given builder function.

    Uses update_or_create to avoid issues with deleting dimensions
    that are referenced by cubes.

    :param tm1: TM1Service instance
    :param dim_name: Name of the dimension
    :param builder_fn: Callable that returns a Dimension object given a name
    :param force: If True, update the dimension even if it exists
    :return: True if created/updated, False if already exists and not forced
    """
    exists = tm1.dimensions.exists(dim_name)

    if exists and not force:
        return False

    dimension = builder_fn(dim_name)
    tm1.dimensions.update_or_create(dimension)
    return True


def _create_workflow_dimension(dim_name: str) -> Dimension:
    """Build the workflow dimension with seed elements."""
    hierarchy = Hierarchy(name=dim_name, dimension_name=dim_name)
    for elem_name in WORKFLOW_SEED_ELEMENTS:
        hierarchy.add_element(elem_name, "String")
    return Dimension(name=dim_name, hierarchies=[hierarchy])


def _create_task_id_dimension(dim_name: str) -> Dimension:
    """Build the task ID dimension with elements 1..5000."""
    hierarchy = Hierarchy(name=dim_name, dimension_name=dim_name)
    for i in range(1, TASK_ID_ELEMENT_COUNT + 1):
        hierarchy.add_element(str(i), "String")
    return Dimension(name=dim_name, hierarchies=[hierarchy])


def _create_run_id_dimension(dim_name: str) -> Dimension:
    """Build the run ID dimension with seed elements."""
    hierarchy = Hierarchy(name=dim_name, dimension_name=dim_name)
    for elem_name in RUN_ID_SEED_ELEMENTS:
        hierarchy.add_element(elem_name, "Numeric")
    return Dimension(name=dim_name, hierarchies=[hierarchy])


def _create_measure_dimension(dim_name: str) -> Dimension:
    """Build the measure dimension with elements and attributes."""
    hierarchy = Hierarchy(name=dim_name, dimension_name=dim_name)

    # Add elements
    for elem_name in MEASURE_ELEMENTS:
        hierarchy.add_element(elem_name, "String")

    # Add element attributes
    hierarchy.element_attributes.append(ElementAttribute("inputs", "String"))
    hierarchy.element_attributes.append(ElementAttribute("results", "String"))

    return Dimension(name=dim_name, hierarchies=[hierarchy])


def _apply_measure_attributes(tm1: TM1Service, dim_name: str):
    """Write attribute values to the measure dimension.

    :param tm1: TM1Service instance
    :param dim_name: Name of the measure dimension
    """
    cellset = {}
    for elem_name, attrs in MEASURE_ATTRIBUTES.items():
        for attr_name, attr_value in attrs.items():
            if attr_value:
                cellset[(elem_name, attr_name)] = attr_value

    if cellset:
        tm1.cells.write_values(f"}}ElementAttributes_{dim_name}", cellset)


# ---------------------------------------------------------------------------
# Cube
# ---------------------------------------------------------------------------


def _create_cube(
    tm1: TM1Service,
    cube_name: str,
    dim_workflow: str,
    dim_task: str,
    dim_run: str,
    dim_measure: str,
    force: bool = False,
) -> bool:
    """Create the RushTI logging cube.

    :return: True if created, False if already exists
    """
    if tm1.cubes.exists(cube_name):
        if force:
            logger.info(f"Deleting existing cube: {cube_name}")
            tm1.cubes.delete(cube_name)
        else:
            return False

    cube = Cube(
        name=cube_name,
        dimensions=[dim_workflow, dim_task, dim_run, dim_measure],
    )
    tm1.cubes.create(cube)
    return True


# ---------------------------------------------------------------------------
# Subsets
# ---------------------------------------------------------------------------


def _create_mdx_subsets(tm1: TM1Service, dim_measure: str, force: bool = False) -> Dict[str, bool]:
    """Create MDX-based subsets for measure dimension.

    Creates three subsets:
    - rushti_inputs_opt: Input measures excluding wait (for optimal mode)
    - rushti_inputs_norm: Input measures excluding require_predecessor_success and succeed_on_minor_errors (for normal mode)
    - rushti_results: All result measures

    :param tm1: TM1Service instance
    :param dim_measure: Name of the measure dimension
    :param force: If True, delete and recreate existing subsets
    :return: Dictionary of subset names and whether they were created
    """
    # First apply measure attributes so MDX subsets can reference them
    try:
        _apply_measure_attributes(tm1, dim_measure)
        logger.info(f"Applied measure attributes to {dim_measure}")
    except Exception as e:
        logger.warning(f"Failed to apply measure attributes: {e}")

    subsets_config = [
        {
            "name": "rushti_inputs_opt",
            "expression": (
                f"{{EXCEPT("
                f'{{TM1FILTERBYPATTERN({{[{dim_measure}].[{dim_measure}].Members}}, "Y","inputs")}},'
                f"{{[{dim_measure}].[{dim_measure}].[wait]}}"
                f")}}"
            ),
        },
        {
            "name": "rushti_inputs_norm",
            "expression": (
                f"{{EXCEPT("
                f'{{TM1FILTERBYPATTERN({{TM1SUBSETALL([{dim_measure}].[{dim_measure}])}}, "Y","inputs")}},'
                f"{{[{dim_measure}].[{dim_measure}].[require_predecessor_success],"
                f"[{dim_measure}].[{dim_measure}].[succeed_on_minor_errors]}}"
                f")}}"
            ),
        },
        {
            "name": "rushti_results",
            "expression": (
                f'{{TM1FILTERBYPATTERN({{[{dim_measure}].[{dim_measure}].Members}}, "Y","results")}}'
            ),
        },
    ]

    results = {}

    for config in subsets_config:
        subset_name = config["name"]

        # When force=True, try to delete the subset first
        if force:
            try:
                if tm1.subsets.exists(
                    subset_name=subset_name,
                    dimension_name=dim_measure,
                    hierarchy_name=dim_measure,
                    private=False,
                ):
                    tm1.subsets.delete(
                        subset_name=subset_name,
                        dimension_name=dim_measure,
                        hierarchy_name=dim_measure,
                        private=False,
                    )
                    logger.info(f"Deleted existing subset '{subset_name}' (force mode)")
            except Exception as e:
                logger.debug(f"Could not delete subset '{subset_name}': {e}")

        # Check if subset already exists
        if not force:
            try:
                if tm1.subsets.exists(
                    subset_name=subset_name,
                    dimension_name=dim_measure,
                    hierarchy_name=dim_measure,
                    private=False,
                ):
                    logger.debug(f"Subset '{subset_name}' already exists (skipped)")
                    results[subset_name] = False
                    continue
            except Exception:
                pass

        # Create MDX subset
        subset = Subset(
            subset_name=subset_name,
            dimension_name=dim_measure,
            hierarchy_name=dim_measure,
            expression=config["expression"],
        )

        try:
            tm1.subsets.create(subset, private=False)
            logger.info(f"Created MDX subset '{subset_name}' in dimension '{dim_measure}'")
            results[subset_name] = True
        except Exception as e:
            logger.error(f"Failed to create subset '{subset_name}': {e}")
            results[subset_name] = False
            raise

    return results


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


def _create_mdx_views(
    tm1: TM1Service,
    cube_name: str,
    dim_workflow: str,
    dim_task: str,
    dim_run: str,
    dim_measure: str,
    force: bool = False,
) -> Dict[str, bool]:
    """Create MDX-based views for the cube.

    Creates two views:
    - Sample_Normal_Mode: View for Sample_Stage_Mode taskfile with inputs_norm subset
    - Sample_Optimal_Mode: View for Sample_Optimal_Mode taskfile with inputs_opt subset

    :param tm1: TM1Service instance
    :param cube_name: Name of the cube
    :param dim_workflow: Name of the taskfile dimension
    :param dim_task: Name of the task dimension
    :param dim_run: Name of the run dimension
    :param dim_measure: Name of the measure dimension
    :param force: If True, delete and recreate existing views
    :return: Dictionary of view names and whether they were created
    """
    views_config = [
        {
            "name": "Sample_Normal_Mode",
            "taskfile": "Sample_Stage_Mode",
            "subset": "rushti_inputs_norm",
        },
        {
            "name": "Sample_Optimal_Mode",
            "taskfile": "Sample_Optimal_Mode",
            "subset": "rushti_inputs_opt",
        },
    ]

    results = {}

    for config in views_config:
        view_name = config["name"]

        # When force=True, try to delete the view first
        if force:
            try:
                if tm1.cubes.views.exists(cube_name=cube_name, view_name=view_name, private=False):
                    tm1.cubes.views.delete(cube_name=cube_name, view_name=view_name, private=False)
                    logger.info(f"Deleted existing view '{view_name}' (force mode)")
            except Exception as e:
                logger.debug(f"Could not delete view '{view_name}': {e}")

        # Check if view already exists
        if not force:
            try:
                if tm1.cubes.views.exists(cube_name=cube_name, view_name=view_name, private=False):
                    logger.debug(f"View '{view_name}' already exists (skipped)")
                    results[view_name] = False
                    continue
            except Exception:
                pass

        mdx_query = (
            f"SELECT "
            f'NON EMPTY {{TM1SubsetToSet([{dim_measure}].[{dim_measure}],"{config["subset"]}")}} ON COLUMNS, '
            f"NON EMPTY {{[{dim_task}].[{dim_task}].Members}} ON ROWS "
            f"FROM [{cube_name}] "
            f"WHERE ([{dim_workflow}].[{dim_workflow}].[{config['taskfile']}], "
            f"[{dim_run}].[{dim_run}].[Input])"
        )

        view = MDXView(cube_name=cube_name, view_name=view_name, MDX=mdx_query)

        try:
            tm1.cubes.views.create(view, private=False)
            logger.info(f"Created MDX view '{view_name}' for cube '{cube_name}'")
            results[view_name] = True
        except Exception as e:
            logger.error(f"Failed to create view '{view_name}': {e}")
            results[view_name] = False
            raise

    return results


# ---------------------------------------------------------------------------
# Process
# ---------------------------------------------------------------------------


def _create_process(tm1: TM1Service, process_name: str, force: bool = False) -> bool:
    """Create the }rushti.load.results TI process.

    :param tm1: TM1Service instance
    :param process_name: Name of the process
    :param force: If True, delete and recreate if exists
    :return: True if created, False if already exists
    """
    if tm1.processes.exists(process_name):
        if force:
            logger.info(f"Deleting existing process: {process_name}")
            tm1.processes.delete(process_name)
        else:
            logger.debug(f"Process already exists: {process_name}")
            return False

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

    # Add parameters
    for param in PROCESS_PARAMETERS:
        process.add_parameter(param["Name"], param["Prompt"], param["Value"])

    # Add variables (all string, from CSV data source)
    for var_name in PROCESS_VARIABLES:
        process.add_variable(var_name, "String")

    tm1.processes.create(process)
    return True


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------


def _populate_sample_data(tm1: TM1Service, cube_name: str) -> Dict[str, int]:
    """Populate sample data into the cube.

    :param tm1: TM1Service instance
    :param cube_name: Name of the cube
    :return: Dictionary with workflow and number of cells written
    """
    results = {}

    for workflow, records in SAMPLE_DATA.items():
        try:
            cellset = {}
            for record in records:
                key = (
                    record["workflow"],
                    record["task_id"],
                    record["run_id"],
                    record["measure"],
                )
                cellset[key] = record["value"]

            if cellset:
                tm1.cells.write_values(cube_name, cellset)
                results[workflow] = len(cellset)
                logger.info(f"Loaded {len(cellset)} cells for {workflow}")
            else:
                results[workflow] = 0
        except Exception as e:
            logger.error(f"Failed to load sample data for {workflow}: {e}")
            results[workflow] = 0

    return results


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify_logging_objects(
    tm1: TM1Service,
    cube_name: str = "rushti",
    dim_workflow: str = "rushti_workflow",
    dim_task: str = "rushti_task_id",
    dim_run: str = "rushti_run_id",
    dim_measure: str = "rushti_measure",
) -> Dict[str, bool]:
    """Verify all required TM1 objects exist.

    :param tm1: TM1Service instance
    :param cube_name: Name of the cube to check
    :param dim_workflow: Name of the workflow dimension
    :param dim_task: Name of the task dimension
    :param dim_run: Name of the run dimension
    :param dim_measure: Name of the measure dimension
    :return: Dictionary of object names and whether they exist
    """
    results = {}

    for dim_name in [dim_workflow, dim_task, dim_run, dim_measure]:
        results[dim_name] = tm1.dimensions.exists(dim_name)

    results[cube_name] = tm1.cubes.exists(cube_name)

    return results


def get_build_status(
    tm1: TM1Service,
    cube_name: str = "rushti",
    dim_workflow: str = "rushti_workflow",
    dim_task: str = "rushti_task_id",
    dim_run: str = "rushti_run_id",
    dim_measure: str = "rushti_measure",
) -> str:
    """Get a human-readable status of build objects.

    :param tm1: TM1Service instance
    :param cube_name: Name of the cube to check
    :param dim_workflow: Name of the workflow dimension
    :param dim_task: Name of the task dimension
    :param dim_run: Name of the run dimension
    :param dim_measure: Name of the measure dimension
    :return: Status message
    """
    verification = verify_logging_objects(
        tm1, cube_name, dim_workflow, dim_task, dim_run, dim_measure
    )

    if all(verification.values()):
        return "All RushTI logging objects are present."

    missing = [name for name, exists in verification.items() if not exists]
    present = [name for name, exists in verification.items() if exists]

    lines = []
    if present:
        lines.append(f"Present: {', '.join(present)}")
    if missing:
        lines.append(f"Missing: {', '.join(missing)}")
        lines.append("Run 'rushti build --tm1-instance <instance>' to create missing objects.")

    return "\n".join(lines)
