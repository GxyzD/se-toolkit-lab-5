from fastapi import APIRouter, Depends, Query, HTTPException
from sqlmodel import select, func
from sqlalchemy import case
from typing import List
from datetime import date

from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog
from sqlmodel.ext.asyncio.session import AsyncSession

router = APIRouter()


async def get_lab_item(session: AsyncSession, lab_param: str) -> ItemRecord:
    """
    Converts 'lab-04' to 'Lab 04' and finds the corresponding ItemRecord.
    """
    # Convert parameter: "lab-04" -> "Lab 04"
    lab_title = lab_param.replace('-', ' ').title()

    # Find the lab whose title contains this string
    lab_item = (await session.exec(
        select(ItemRecord).where(ItemRecord.title.contains(lab_title))
    )).first()

    if not lab_item:
        raise HTTPException(status_code=404, detail="Lab not found")

    return lab_item


@router.get("/scores")
async def get_scores_histogram(
    lab: str = Query(..., description="Lab identifier (e.g., lab-04)"),
    session: AsyncSession = Depends(get_session)
) -> List[dict]:
    """
    Returns score distribution across four buckets.
    GET /analytics/scores?lab=lab-04

    Returns:
    [
        {"bucket": "0-25", "count": 12},
        {"bucket": "26-50", "count": 8},
        {"bucket": "51-75", "count": 15},
        {"bucket": "76-100", "count": 25}
    ]
    """
    # Find the lab
    lab_item = await get_lab_item(session, lab)

    # Find all tasks of this lab
    tasks = (await session.exec(
        select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    )).all()
    task_ids = [task.id for task in tasks]

    if not task_ids:
        # If no tasks, return empty buckets
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0}
        ]

    # Create CASE expression for bucket determination
    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100"
    ).label("bucket")

    # Query with grouping by buckets
    query = (
        select(
            bucket_case,
            func.count(InteractionLog.id).label("count")
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by("bucket")
    )

    results = (await session.exec(query)).all()

    # Convert result to dictionary for convenience
    counts_dict = {row[0]: row[1] for row in results}

    # Return all 4 buckets, even if some are missing
    return [
        {"bucket": "0-25", "count": counts_dict.get("0-25", 0)},
        {"bucket": "26-50", "count": counts_dict.get("26-50", 0)},
        {"bucket": "51-75", "count": counts_dict.get("51-75", 0)},
        {"bucket": "76-100", "count": counts_dict.get("76-100", 0)}
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier (e.g., lab-04)"),
    session: AsyncSession = Depends(get_session)
) -> List[dict]:
    """
    Returns average score and number of attempts for each task.
    GET /analytics/pass-rates?lab=lab-04

    Returns:
    [
        {"task": "Repository Setup", "avg_score": 92.3, "attempts": 150},
        {"task": "Docker Setup", "avg_score": 78.5, "attempts": 120}
    ]
    """
    # Find the lab
    lab_item = await get_lab_item(session, lab)

    # Find all tasks of this lab
    tasks = (await session.exec(
        select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    )).all()
    task_ids = [task.id for task in tasks]

    if not task_ids:
        return []

    # For each task, calculate average score and number of interactions
    query = (
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts")
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    results = (await session.exec(query)).all()

    # Convert result
    return [
        {
            "task": row.task,
            "avg_score": float(row.avg_score) if row.avg_score else 0,
            "attempts": row.attempts
        }
        for row in results
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier (e.g., lab-04)"),
    session: AsyncSession = Depends(get_session)
) -> List[dict]:
    """
    Returns number of submissions by date.
    GET /analytics/timeline?lab=lab-04

    Returns:
    [
        {"date": "2026-02-28", "submissions": 45},
        {"date": "2026-03-01", "submissions": 32}
    ]
    """
    # Find the lab
    lab_item = await get_lab_item(session, lab)

    # Find all tasks of this lab
    tasks = (await session.exec(
        select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    )).all()
    task_ids = [task.id for task in tasks]

    if not task_ids:
        return []

    # Group by date (cast created_at to date)
    query = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count(InteractionLog.id).label("submissions")
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    results = (await session.exec(query)).all()

    # Convert result
    return [
        {
            "date": str(row.date),
            "submissions": row.submissions
        }
        for row in results
    ]


@router.get("/groups")
async def get_groups_performance(
    lab: str = Query(..., description="Lab identifier (e.g., lab-04)"),
    session: AsyncSession = Depends(get_session)
) -> List[dict]:
    """
    Returns performance by student groups.
    GET /analytics/groups?lab=lab-04

    Returns:
    [
        {"group": "B23-CS-01", "avg_score": 78.5, "students": 25},
        {"group": "B23-CS-02", "avg_score": 82.1, "students": 24}
    ]
    """
    # Find the lab
    lab_item = await get_lab_item(session, lab)

    # Find all tasks of this lab
    tasks = (await session.exec(
        select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    )).all()
    task_ids = [task.id for task in tasks]

    if not task_ids:
        return []

    # Join InteractionLog with Learner and group by student_group
    query = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students")
        )
        .join(InteractionLog, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    results = (await session.exec(query)).all()

    # Convert result
    return [
        {
            "group": row.group,
            "avg_score": float(row.avg_score) if row.avg_score else 0,
            "students": row.students
        }
        for row in results
    ]