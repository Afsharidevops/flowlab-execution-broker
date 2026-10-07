from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from flowlab_execution_broker.models.domain import Base, ExecutionStatus
from flowlab_execution_broker.services.engine import BrokerEngine, SecurityError


@pytest.fixture
async def session() -> AsyncIterator[AsyncSession]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        yield db
    await engine.dispose()


@pytest.mark.asyncio
async def test_job_submission_and_claim(session: AsyncSession) -> None:
    broker = BrokerEngine(session)
    job = await broker.submit_job("t1", "w1", "echo", ["hello"])
    assert job.status == ExecutionStatus.QUEUED

    claimed = await broker.claim_next("worker-1")
    assert claimed is not None
    assert claimed.id == job.id
    assert claimed.status == ExecutionStatus.RUNNING
    assert claimed.lease_owner == "worker-1"


@pytest.mark.asyncio
async def test_command_sanitization(session: AsyncSession) -> None:
    broker = BrokerEngine(session)
    with pytest.raises(SecurityError):
        await broker.submit_job("t1", "w1", "rm", ["-rf", "/"])


@pytest.mark.asyncio
async def test_approval_policy(session: AsyncSession) -> None:
    broker = BrokerEngine(session)
    job = await broker.submit_job("t1", "w1", "echo", ["hello"], policy="manual")
    assert job.status == ExecutionStatus.PENDING_APPROVAL

    claimed = await broker.claim_next("worker-1")
    assert claimed is None


@pytest.mark.asyncio
async def test_job_execution(session: AsyncSession) -> None:
    broker = BrokerEngine(session)
    job = await broker.submit_job("t1", "w1", "echo", ["hello"])
    success = await broker.execute(job)
    assert success is True
