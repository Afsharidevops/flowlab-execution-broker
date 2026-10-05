import asyncio
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from flowlab_execution_broker.models.domain import ExecutionJob, ExecutionStatus


class SecurityError(Exception):
    pass


class BrokerEngine:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def submit_job(
        self, tenant_id: str, workspace_id: str, command: str, arguments: list[str], policy: str = "auto"
    ) -> ExecutionJob:
        if not self._is_command_safe(command):
            raise SecurityError(f"Command '{command}' is not permitted")

        job = ExecutionJob(
            id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            command=command,
            arguments=arguments,
            approval_policy=policy,
            status=ExecutionStatus.QUEUED if policy == "auto" else ExecutionStatus.PENDING_APPROVAL,
        )
        self.db.add(job)
        await self.db.flush()
        return job

    async def claim_next(self, worker_id: str, lease_seconds: int = 60) -> ExecutionJob | None:
        now = datetime.now(UTC)
        expires = now + timedelta(seconds=lease_seconds)

        if self.db.bind and self.db.bind.dialect.name == "sqlite":
            query = """
                UPDATE execution_jobs
                SET status = 'running', lease_owner = :worker_id, lease_expires_at = :expires, updated_at = :now, attempts = attempts + 1
                WHERE id = (
                    SELECT id FROM execution_jobs
                    WHERE status IN ('queued', 'approved')
                    OR (status = 'running' AND lease_expires_at < :now)
                    ORDER BY created_at ASC
                    LIMIT 1
                )
                RETURNING id, tenant_id, workspace_id, command, arguments, status, approval_policy, lease_owner, lease_expires_at, attempts, created_at, updated_at
            """
        else:
            query = """
                UPDATE execution_jobs
                SET status = 'running', lease_owner = :worker_id, lease_expires_at = :expires, updated_at = :now, attempts = attempts + 1
                WHERE id = (
                    SELECT id FROM execution_jobs
                    WHERE status IN ('queued', 'approved')
                    OR (status = 'running' AND lease_expires_at < :now)
                    ORDER BY created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, tenant_id, workspace_id, command, arguments, status, approval_policy, lease_owner, lease_expires_at, attempts, created_at, updated_at
            """

        result = await self.db.execute(
            text(query),
            {"worker_id": worker_id, "expires": expires, "now": now},
        )
        row = result.fetchone()
        if not row:
            return None

        job = ExecutionJob(
            id=row[0],
            tenant_id=row[1],
            workspace_id=row[2],
            command=row[3],
            arguments=row[4],
            status=ExecutionStatus(row[5]),
            approval_policy=row[6],
            lease_owner=row[7],
            lease_expires_at=row[8],
            attempts=row[9],
            created_at=row[10],
            updated_at=row[11],
        )
        return job

    async def mark_done(self, job_id: str, success: bool) -> None:
        result = await self.db.execute(select(ExecutionJob).where(ExecutionJob.id == job_id))
        job = result.scalar_one_or_none()
        if job:
            job.status = ExecutionStatus.SUCCEEDED if success else ExecutionStatus.FAILED
            job.updated_at = datetime.now(UTC)
            job.lease_owner = None
            job.lease_expires_at = None

    async def execute(self, job: ExecutionJob) -> bool:
        if not self._is_command_safe(job.command):
            return False

        cmd = [job.command, *job.arguments]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            _, _ = await proc.communicate()
            if proc.returncode is None:
                return False
            return proc.returncode == 0
        except Exception:
            return False

    def _is_command_safe(self, command: str) -> bool:
        allowed = {"echo", "date", "whoami", "uname", "python"}
        return command in allowed
