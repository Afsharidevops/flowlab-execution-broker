import asyncio
import logging
import os
import signal
import uuid

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from flowlab_execution_broker.models.domain import Base
from flowlab_execution_broker.services.engine import BrokerEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("broker-worker")

shutdown = False

def handle_sigterm(sig: int, frame: object) -> None:
    global shutdown
    logger.info("Shutdown signal received")
    shutdown = True

async def run_worker() -> None:
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    db_url = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+asyncpg://", 1)
    
    engine = create_async_engine(db_url)
    
    if os.getenv("FLOWLAB_AUTO_CREATE_SCHEMA", "").lower() in ("1", "true", "yes"):
        logger.info("Auto-creating schema for flowlab-execution-broker")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
    worker_id = str(uuid.uuid4())
    logger.info("Starting execution broker worker %s", worker_id)

    while not shutdown:
        async with SessionLocal() as session:
            broker = BrokerEngine(session)
            try:
                job = await broker.claim_next(worker_id)
                if not job:
                    await session.rollback()
                    await asyncio.sleep(1)
                    continue
                
                await session.commit()
                logger.info("Executing job %s: %s", job.id, job.command)
                
                success = await broker.execute(job)
                
                await broker.mark_done(job.id, success)
                await session.commit()
                logger.info("Job %s completed: success=%s", job.id, success)
            except Exception as e:
                logger.error("Broker loop error: %s", e)
                await session.rollback()
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(run_worker())
