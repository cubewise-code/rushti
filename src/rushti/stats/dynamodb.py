"""DynamoDB-backed StatsDatabase adapter.

Extracted from ``rushti.stats`` (formerly ``stats.py``) in Phase 3 of
the architecture refactor. The class itself is unchanged byte-for-byte;
only its module home moved.
"""

import json
import logging
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

from rushti.stats.paths import (
    DEFAULT_DYNAMODB_RUNS_TABLE,
    DEFAULT_DYNAMODB_TASK_RESULTS_TABLE,
)
from rushti.stats.signature import calculate_task_signature

logger = logging.getLogger(__name__)


class DynamoDBStatsDatabase:
    """DynamoDB-backed database for execution statistics.

    Expected table design:
    - ``runs`` table:
      - Partition key: ``run_id`` (String)
      - Recommended GSI: ``workflow-start_time-index`` (PK: workflow, SK: start_time)
    - ``task_results`` table:
      - Partition key: ``run_id`` (String)
      - Sort key: ``task_result_id`` (String)
      - Recommended GSI: ``signature-start_time-index`` (PK: task_signature, SK: start_time)
      - Recommended GSI: ``workflow-start_time-index`` (PK: workflow, SK: start_time)
    """

    def __init__(
        self,
        enabled: bool = False,
        region_name: Optional[str] = None,
        runs_table_name: str = DEFAULT_DYNAMODB_RUNS_TABLE,
        task_results_table_name: str = DEFAULT_DYNAMODB_TASK_RESULTS_TABLE,
        endpoint_url: Optional[str] = None,
    ):
        self.enabled = enabled
        self.region_name = region_name
        self.runs_table_name = runs_table_name
        self.task_results_table_name = task_results_table_name
        self.endpoint_url = endpoint_url
        self._resource = None
        self._runs_table = None
        self._task_results_table = None

        if self.enabled:
            self._initialize_database()

    def _initialize_database(self) -> None:
        try:
            import boto3
        except ImportError as e:
            raise RuntimeError(
                "DynamoDB backend requires boto3. Install boto3 to use [stats] backend = dynamodb."
            ) from e

        resource_kwargs: Dict[str, Any] = {}
        if self.region_name:
            resource_kwargs["region_name"] = self.region_name
        if self.endpoint_url:
            resource_kwargs["endpoint_url"] = self.endpoint_url

        self._resource = boto3.resource("dynamodb", **resource_kwargs)
        self._runs_table = self._resource.Table(self.runs_table_name)
        self._task_results_table = self._resource.Table(self.task_results_table_name)

        # Validate table accessibility early (tables must already exist).
        self._runs_table.load()
        self._task_results_table.load()

        logger.info(
            "DynamoDB stats database initialized: runs_table=%s, task_results_table=%s",
            self.runs_table_name,
            self.task_results_table_name,
        )

    def _query_all(self, table, **kwargs) -> List[Dict[str, Any]]:
        """Paginate a DynamoDB query, honoring Limit as a global item cap."""
        limit = kwargs.pop("Limit", None)
        items: List[Dict[str, Any]] = []
        response = table.query(**kwargs)
        items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response and (limit is None or len(items) < limit):
            kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            response = table.query(**kwargs)
            items.extend(response.get("Items", []))
        return items[:limit] if limit is not None else items

    def _scan_all(self, table, **kwargs) -> List[Dict[str, Any]]:
        """Paginate a DynamoDB scan, honoring Limit as a global item cap."""
        limit = kwargs.pop("Limit", None)
        items: List[Dict[str, Any]] = []
        response = table.scan(**kwargs)
        items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response and (limit is None or len(items) < limit):
            kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            response = table.scan(**kwargs)
            items.extend(response.get("Items", []))
        return items[:limit] if limit is not None else items

    def _normalize_task_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "workflow": item.get("workflow"),
            "task_id": item.get("task_id"),
            "task_signature": item.get("task_signature"),
            "instance": item.get("instance"),
            "process": item.get("process"),
            "parameters": item.get("parameters", "{}"),
            "status": item.get("status"),
            "start_time": item.get("start_time"),
            "end_time": item.get("end_time"),
            "duration_seconds": self._to_float(item.get("duration_seconds")),
            "retry_count": int(item["retry_count"]) if item.get("retry_count") is not None else 0,
            "error_message": item.get("error_message"),
            "predecessors": item.get("predecessors"),
            "stage": item.get("stage"),
            "safe_retry": item.get("safe_retry"),
            "timeout": item.get("timeout"),
            "cancel_at_timeout": item.get("cancel_at_timeout"),
            "require_predecessor_success": item.get("require_predecessor_success"),
            "succeed_on_minor_errors": item.get("succeed_on_minor_errors"),
        }

    def _to_float(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _as_decimal(self, value: float) -> Decimal:
        """Convert float to Decimal for DynamoDB storage.

        boto3's DynamoDB resource interface does not accept Python floats;
        Decimal is required for all numeric values stored as DynamoDB N type.
        """
        return Decimal(str(round(value, 3)))

    def start_run(
        self,
        run_id: str,
        workflow: str,
        taskfile_path: Optional[str] = None,
        task_count: int = 0,
        taskfile_name: Optional[str] = None,
        taskfile_description: Optional[str] = None,
        taskfile_author: Optional[str] = None,
        max_workers: Optional[int] = None,
        retries: Optional[int] = None,
        result_file: Optional[str] = None,
        exclusive: Optional[bool] = None,
        optimize: Optional[bool] = None,
        optimization_algorithm: Optional[str] = None,
    ) -> None:
        if not self.enabled:
            return

        item = {
            "run_id": run_id,
            "workflow": workflow,
            "taskfile_path": taskfile_path,
            "start_time": datetime.now().isoformat(),
            "task_count": task_count,
            "taskfile_name": taskfile_name,
            "taskfile_description": taskfile_description,
            "taskfile_author": taskfile_author,
            "max_workers": max_workers,
            "retries": retries,
            "result_file": result_file,
            "exclusive": exclusive,
            "optimize": optimize,
            "optimization_algorithm": optimization_algorithm,
        }
        self._runs_table.put_item(Item=item)

    def record_task(
        self,
        run_id: str,
        task_id: str,
        instance: str,
        process: str,
        parameters: Optional[Dict[str, Any]],
        success: bool,
        start_time: datetime,
        end_time: datetime,
        retry_count: int = 0,
        error_message: Optional[str] = None,
        predecessors: Optional[List[str]] = None,
        stage: Optional[str] = None,
        safe_retry: Optional[bool] = None,
        timeout: Optional[int] = None,
        cancel_at_timeout: Optional[bool] = None,
        require_predecessor_success: Optional[bool] = None,
        succeed_on_minor_errors: Optional[bool] = None,
        workflow: Optional[str] = None,
    ) -> None:
        if not self.enabled:
            return

        duration = (end_time - start_time).total_seconds()
        task_signature = calculate_task_signature(instance, process, parameters)

        item = {
            "run_id": run_id,
            "task_result_id": f"{start_time.isoformat()}#{task_id}#{uuid.uuid4().hex[:8]}",
            "workflow": workflow,
            "task_id": task_id,
            "task_signature": task_signature,
            "instance": instance,
            "process": process,
            "parameters": json.dumps(parameters) if parameters else "{}",
            "status": "Success" if success else "Fail",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": self._as_decimal(duration),
            "retry_count": retry_count,
            "error_message": error_message if not success else None,
            "predecessors": json.dumps(predecessors) if predecessors else None,
            "stage": stage,
            "safe_retry": safe_retry,
            "timeout": timeout,
            "cancel_at_timeout": cancel_at_timeout,
            "require_predecessor_success": require_predecessor_success,
            "succeed_on_minor_errors": succeed_on_minor_errors,
        }
        self._task_results_table.put_item(Item=item)

    def batch_record_tasks(self, tasks: List[Dict[str, Any]]) -> None:
        if not self.enabled or not tasks:
            return

        with self._task_results_table.batch_writer() as writer:
            for task in tasks:
                start_time = task["start_time"]
                end_time = task["end_time"]
                duration = (end_time - start_time).total_seconds()
                instance = task["instance"]
                process = task["process"]
                parameters = task.get("parameters")
                task_signature = calculate_task_signature(instance, process, parameters)

                writer.put_item(
                    Item={
                        "run_id": task["run_id"],
                        "task_result_id": f"{start_time.isoformat()}#{task['task_id']}#{uuid.uuid4().hex[:8]}",
                        "workflow": task.get("workflow"),
                        "task_id": task["task_id"],
                        "task_signature": task_signature,
                        "instance": instance,
                        "process": process,
                        "parameters": json.dumps(parameters) if parameters else "{}",
                        "status": "Success" if task["success"] else "Fail",
                        "start_time": start_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "duration_seconds": self._as_decimal(duration),
                        "retry_count": task.get("retry_count", 0),
                        "error_message": task.get("error_message") if not task["success"] else None,
                        "predecessors": (
                            json.dumps(task.get("predecessors"))
                            if task.get("predecessors")
                            else None
                        ),
                        "stage": task.get("stage"),
                        "safe_retry": task.get("safe_retry"),
                        "timeout": task.get("timeout"),
                        "cancel_at_timeout": task.get("cancel_at_timeout"),
                        "require_predecessor_success": task.get("require_predecessor_success"),
                        "succeed_on_minor_errors": task.get("succeed_on_minor_errors"),
                    }
                )

    def complete_run(
        self,
        run_id: str,
        status: str = "Success",
        success_count: int = 0,
        failure_count: int = 0,
    ) -> None:
        if not self.enabled:
            return

        run_info = self.get_run_info(run_id)
        end_time = datetime.now()
        duration_seconds = None
        if run_info and run_info.get("start_time"):
            try:
                started = datetime.fromisoformat(run_info["start_time"])
                duration_seconds = (end_time - started).total_seconds()
            except (TypeError, ValueError):
                duration_seconds = None

        self._runs_table.update_item(
            Key={"run_id": run_id},
            UpdateExpression=(
                "SET end_time = :end_time, duration_seconds = :duration, #status = :status, "
                "success_count = :success_count, failure_count = :failure_count"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":end_time": end_time.isoformat(),
                ":duration": (
                    self._as_decimal(duration_seconds) if duration_seconds is not None else None
                ),
                ":status": status,
                ":success_count": success_count,
                ":failure_count": failure_count,
            },
        )

    def cleanup_old_data(self, retention_days: int) -> int:
        if not self.enabled or retention_days <= 0:
            return 0

        cutoff = (datetime.now() - timedelta(days=retention_days)).isoformat()
        runs = self._scan_all(self._runs_table)
        old_runs = [r for r in runs if (r.get("start_time") or "") < cutoff]

        for run in old_runs:
            run_id = run["run_id"]
            from boto3.dynamodb.conditions import Key

            task_items = self._query_all(
                self._task_results_table,
                KeyConditionExpression=Key("run_id").eq(run_id),
            )
            with self._task_results_table.batch_writer() as writer:
                for task in task_items:
                    writer.delete_item(
                        Key={"run_id": run_id, "task_result_id": task["task_result_id"]}
                    )
            self._runs_table.delete_item(Key={"run_id": run_id})

        if old_runs:
            logger.info("Cleaned up %s runs older than %s days", len(old_runs), retention_days)
        return len(old_runs)

    def get_task_history(self, task_signature: str, limit: int = 10) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []

        try:
            from boto3.dynamodb.conditions import Key

            items = self._query_all(
                self._task_results_table,
                IndexName="signature-start_time-index",
                KeyConditionExpression=Key("task_signature").eq(task_signature),
                ScanIndexForward=False,
                Limit=limit,
            )
        except Exception as e:
            from botocore.exceptions import ClientError

            if isinstance(e, ClientError) and e.response["Error"]["Code"] not in (
                "ValidationException",
                "ResourceNotFoundException",
            ):
                raise
            logger.warning(
                "GSI 'signature-start_time-index' not available, falling back to scan: %s", e
            )
            items = [
                i
                for i in self._scan_all(self._task_results_table)
                if i.get("task_signature") == task_signature
            ]
            items.sort(key=lambda i: i.get("start_time", ""), reverse=True)

        results = []
        for item in items:
            if item.get("status") != "Success":
                continue
            results.append(self._normalize_task_item(item))
            if len(results) >= limit:
                break
        return results

    def get_workflow_signatures(self, workflow: str) -> List[str]:
        if not self.enabled:
            return []

        items = [
            i
            for i in self._scan_all(self._task_results_table)
            if i.get("workflow") == workflow and i.get("task_signature")
        ]
        return sorted(set(i["task_signature"] for i in items))

    def get_task_sample_count(self, task_signature: str) -> int:
        history = self.get_task_history(task_signature, limit=10000)
        return len(history)

    def get_task_durations(self, task_signature: str, limit: int = 10) -> List[float]:
        history = self.get_task_history(task_signature, limit=limit)
        durations: List[float] = []
        for row in history:
            duration = self._to_float(row.get("duration_seconds"))
            if duration is not None:
                durations.append(duration)
        return durations

    def get_run_results(self, run_id: str) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []

        from boto3.dynamodb.conditions import Key

        items = self._query_all(
            self._task_results_table,
            KeyConditionExpression=Key("run_id").eq(run_id),
            ScanIndexForward=True,
        )
        return [self._normalize_task_item(item) for item in items]

    def get_run_info(self, run_id: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        response = self._runs_table.get_item(Key={"run_id": run_id})
        return response.get("Item")

    def get_runs_for_workflow(self, workflow: str) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []

        try:
            from boto3.dynamodb.conditions import Key

            items = self._query_all(
                self._runs_table,
                IndexName="workflow-start_time-index",
                KeyConditionExpression=Key("workflow").eq(workflow),
                ScanIndexForward=False,
            )
        except Exception as e:
            from botocore.exceptions import ClientError

            if isinstance(e, ClientError) and e.response["Error"]["Code"] not in (
                "ValidationException",
                "ResourceNotFoundException",
            ):
                raise
            logger.warning(
                "GSI 'workflow-start_time-index' not available, falling back to scan: %s", e
            )
            items = [i for i in self._scan_all(self._runs_table) if i.get("workflow") == workflow]
            items.sort(key=lambda i: i.get("start_time", ""), reverse=True)

        results: List[Dict[str, Any]] = []
        for item in items:
            results.append(
                {
                    "run_id": item.get("run_id"),
                    "start_time": item.get("start_time"),
                    "end_time": item.get("end_time"),
                    "duration_seconds": self._to_float(item.get("duration_seconds")),
                    "status": item.get("status"),
                    "task_count": item.get("task_count"),
                    "success_count": item.get("success_count"),
                    "failure_count": item.get("failure_count"),
                    "max_workers": item.get("max_workers"),
                }
            )
        return results

    def get_all_runs(self) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []

        items = list(self._scan_all(self._runs_table))
        items.sort(key=lambda i: i.get("start_time", ""), reverse=True)
        results: List[Dict[str, Any]] = []
        for item in items:
            results.append(
                {
                    "run_id": item.get("run_id"),
                    "start_time": item.get("start_time"),
                    "end_time": item.get("end_time"),
                    "duration_seconds": self._to_float(item.get("duration_seconds")),
                    "status": item.get("status"),
                    "task_count": item.get("task_count"),
                    "success_count": item.get("success_count"),
                    "failure_count": item.get("failure_count"),
                    "max_workers": item.get("max_workers"),
                }
            )
        return results

    def get_run_task_stats(self, run_id: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        results = [r for r in self.get_run_results(run_id) if r.get("status") == "Success"]
        if not results:
            return None

        durations = [d for d in (self._to_float(r.get("duration_seconds")) for r in results) if d]
        if not durations:
            return None
        return {
            "total_duration": sum(durations),
            "task_count": len(durations),
            "avg_duration": sum(durations) / len(durations),
        }

    def get_concurrent_task_counts(self, run_id: str) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []

        results = [r for r in self.get_run_results(run_id) if r.get("status") == "Success"]
        parsed = []
        for row in results:
            try:
                start = datetime.fromisoformat(row["start_time"])
                end = datetime.fromisoformat(row["end_time"])
            except (KeyError, TypeError, ValueError):
                continue
            parsed.append((row, start, end))

        output: List[Dict[str, Any]] = []
        for row, start, end in parsed:
            overlap = 0
            for other_row, other_start, other_end in parsed:
                if other_row is row:
                    continue
                if other_start < end and other_end > start:
                    overlap += 1
            output.append(
                {
                    "task_signature": row.get("task_signature"),
                    "duration_seconds": self._to_float(row.get("duration_seconds")),
                    "concurrent_count": overlap,
                }
            )
        return output

    def close(self) -> None:
        self._resource = None
        self._runs_table = None
        self._task_results_table = None

    def __enter__(self) -> "DynamoDBStatsDatabase":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
