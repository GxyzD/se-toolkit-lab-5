"""
ETL pipeline for fetching data from Autochecker API and loading into database.
"""
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any

import httpx
from sqlalchemy.orm import Session

from app import settings
from app.database import get_db
from app.models.items import Item
from app.models.learners import Learner
from app.models.interactions import InteractionLog

logger = logging.getLogger(__name__)


async def fetch_items() -> List[Dict[str, Any]]:
    """
    Fetch the lab/task catalog from /api/items endpoint.
    
    Returns:
        List of item objects from the API
        
    Raises:
        httpx.HTTPError: If the API request fails
    """
    logger.info("Fetching items catalog from Autochecker API")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(
            f"{settings.AUTOCHECKER_API_URL}/api/items",
            auth=(settings.AUTOCHECKER_EMAIL, settings.AUTOCHECKER_PASSWORD)
        )
        response.raise_for_status()
        
        items = response.json()
        logger.info(f"Successfully fetched {len(items)} items")
        return items


async def fetch_logs(since: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """
    Fetch check logs from /api/logs endpoint with pagination.
    
    Args:
        since: Optional datetime to fetch only logs after this timestamp
        
    Returns:
        List of all log objects from the API
        
    Raises:
        httpx.HTTPError: If the API request fails
    """
    logger.info(f"Fetching logs from Autochecker API (since={since})")
    
    all_logs: List[Dict[str, Any]] = []
    offset = 0
    limit = 100  # Max items per page
    has_more = True
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        while has_more:
            # Prepare request parameters
            params: Dict[str, Any] = {
                "limit": limit,
                "offset": offset
            }
            if since:
                # Convert datetime to ISO format string
                params["since"] = since.isoformat().replace('+00:00', 'Z')
            
            logger.debug(f"Fetching logs with params: {params}")
            
            # Make request
            response = await client.get(
                f"{settings.AUTOCHECKER_API_URL}/api/logs",
                auth=(settings.AUTOCHECKER_EMAIL, settings.AUTOCHECKER_PASSWORD),
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            logs_batch = data.get("logs", [])
            all_logs.extend(logs_batch)
            
            # Check if there are more pages
            has_more = data.get("has_more", False)
            offset += limit
            
            logger.debug(f"Fetched {len(logs_batch)} logs, has_more={has_more}")
    
    logger.info(f"Successfully fetched total {len(all_logs)} logs")
    return all_logs


async def load_items(db: Session, items_data: List[Dict[str, Any]]) -> None:
    """
    Load items into database (labs and tasks).
    
    This function handles the parent-child relationship between labs and tasks.
    Labs are identified by their 'lab' field, tasks by their 'task' field.
    
    Args:
        db: Database session
        items_data: List of item objects from the API
    """
    logger.info(f"Loading {len(items_data)} items into database")
    
    # First pass: create/update all labs (type="lab")
    labs_map: Dict[str, Item] = {}
    for item_data in items_data:
        if item_data["type"] == "lab":
            external_id = item_data["lab"]  # e.g., "lab-01"
            
            # Check if lab already exists
            lab = db.query(Item).filter(
                Item.type == "lab",
                Item.external_id == external_id
            ).first()
            
            if lab:
                # Update existing lab
                lab.title = item_data["title"]
                logger.debug(f"Updated lab: {external_id}")
            else:
                # Create new lab
                lab = Item(
                    external_id=external_id,
                    title=item_data["title"],
                    type="lab",
                    parent_id=None
                )
                db.add(lab)
                logger.debug(f"Created lab: {external_id}")
            
            # Store in map for later use (need to flush to get ID)
            labs_map[external_id] = lab
    
    # Flush to get IDs for new labs
    db.flush()
    
    # Second pass: create/update all tasks (type="task")
    tasks_count = 0
    for item_data in items_data:
        if item_data["type"] == "task":
            external_id = item_data["task"]  # e.g., "setup"
            lab_id = item_data["lab"]  # Parent lab ID, e.g., "lab-01"
            
            # Find parent lab
            parent_lab = labs_map.get(lab_id)
            if not parent_lab:
                logger.error(f"Parent lab {lab_id} not found for task {external_id}")
                continue
            
            # Check if task already exists
            task = db.query(Item).filter(
                Item.type == "task",
                Item.external_id == external_id,
                Item.parent_id == parent_lab.id
            ).first()
            
            if task:
                # Update existing task
                task.title = item_data["title"]
                logger.debug(f"Updated task: {lab_id}/{external_id}")
            else:
                # Create new task
                task = Item(
                    external_id=external_id,
                    title=item_data["title"],
                    type="task",
                    parent_id=parent_lab.id
                )
                db.add(task)
                logger.debug(f"Created task: {lab_id}/{external_id}")
            
            tasks_count += 1
    
    # Commit all changes
    db.commit()
    logger.info(f"Successfully loaded {len(labs_map)} labs and {tasks_count} tasks")


async def load_logs(
    db: Session, 
    logs_data: List[Dict[str, Any]], 
    items_catalog: Dict[str, Item]
) -> None:
    """
    Load check logs into database, creating learners as needed.
    
    Args:
        db: Database session
        logs_data: List of log objects from the API
        items_catalog: Dictionary mapping "lab_task" or "lab" strings to Item objects
                      for quick lookup
    """
    logger.info(f"Loading {len(logs_data)} logs into database")
    
    new_logs_count = 0
    skipped_logs_count = 0
    
    for log_data in logs_data:
        # Check if log already exists (idempotency)
        external_id = str(log_data["id"])
        existing_log = db.query(InteractionLog).filter(
            InteractionLog.external_id == external_id
        ).first()
        
        if existing_log:
            skipped_logs_count += 1
            logger.debug(f"Log {external_id} already exists, skipping")
            continue
        
        # Find or create learner
        student_id = log_data["student_id"]
        learner = db.query(Learner).filter(
            Learner.external_id == student_id
        ).first()
        
        if not learner:
            learner = Learner(
                external_id=student_id,
                student_group=log_data["group"]
            )
            db.add(learner)
            db.flush()  # Get ID for new learner
            logger.debug(f"Created new learner: {student_id}")
        
        # Find the item (task or lab)
        lab = log_data["lab"]
        task = log_data.get("task")
        
        # Create lookup key: for tasks use "lab_task", for labs just "lab"
        if task:
            item_key = f"{lab}_{task}"
        else:
            item_key = lab
        
        item = items_catalog.get(item_key)
        if not item:
            logger.error(f"Item not found for key: {item_key}")
            continue
        
        # Parse submitted_at
        submitted_at_str = log_data["submitted_at"]
        # Handle ISO format with Z timezone
        if submitted_at_str.endswith('Z'):
            submitted_at_str = submitted_at_str.replace('Z', '+00:00')
        submitted_at = datetime.fromisoformat(submitted_at_str)
        
        # Create interaction log
        interaction_log = InteractionLog(
            external_id=external_id,
            learner_id=learner.id,
            item_id=item.id,
            score=log_data["score"],
            checks_passed=log_data["passed"],
            checks_total=log_data["total"],
            submitted_at=submitted_at
        )
        db.add(interaction_log)
        new_logs_count += 1
        logger.debug(f"Created log {external_id} for learner {student_id}")
    
    # Commit all new logs
    db.commit()
    logger.info(f"Successfully loaded {new_logs_count} new logs, skipped {skipped_logs_count} existing")


async def sync() -> Dict[str, int]:
    """
    Main ETL orchestration function.
    
    This function:
    1. Fetches and loads all items (labs and tasks)
    2. Builds a catalog for quick item lookup
    3. Finds the most recent log timestamp
    4. Fetches and loads new logs since that timestamp
    5. Returns counts of new and total records
    
    Returns:
        Dictionary with new_records and total_records counts
    """
    logger.info("Starting ETL sync")
    
    # Get database session
    db = next(get_db())
    
    try:
        # Step 1: Fetch and load items
        logger.info("Step 1: Syncing items")
        items_data = await fetch_items()
        await load_items(db, items_data)
        
        # Step 2: Build items catalog for quick lookup
        logger.info("Step 2: Building items catalog")
        all_items = db.query(Item).all()
        items_catalog: Dict[str, Item] = {}
        
        for item in all_items:
            if item.type == "lab":
                # Lab: key is just lab external_id
                items_catalog[item.external_id] = item
            elif item.type == "task" and item.parent:
                # Task: key is "lab_external_id_task_external_id"
                key = f"{item.parent.external_id}_{item.external_id}"
                items_catalog[key] = item
        
        logger.info(f"Built catalog with {len(items_catalog)} items")
        
        # Step 3: Find most recent log timestamp
        logger.info("Step 3: Finding most recent log timestamp")
        last_log = db.query(InteractionLog).order_by(
            InteractionLog.submitted_at.desc()
        ).first()
        
        since = last_log.submitted_at if last_log else None
        logger.info(f"Most recent log timestamp: {since}")
        
        # Step 4: Fetch and load new logs
        logger.info("Step 4: Fetching and loading new logs")
        logs_data = await fetch_logs(since)
        await load_logs(db, logs_data, items_catalog)
        
        # Step 5: Count results
        logger.info("Step 5: Counting results")
        total_records = db.query(InteractionLog).count()
        new_records = len(logs_data)
        
        result = {
            "new_records": new_records,
            "total_records": total_records
        }
        
        logger.info(f"Sync completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"ETL sync failed: {str(e)}")
        db.rollback()
        raise
    finally:
        db.close()