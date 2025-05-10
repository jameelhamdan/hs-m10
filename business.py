import decimal
import time
from decimal import Decimal
from typing import List
import datetime
from sqlalchemy import insert, update
from sqlalchemy.orm import Session, load_only
import models


def create_client_with_subscription(
    db: Session,
    name: str,
    topics: List[models.Topic],
    initial_amount: decimal.Decimal = 0,
) -> models.Client:
    client = models.Client(name=name)
    db.add(client)
    db.flush()

    for topic, amount in topics:
        subscription = models.Subscription(
            client_id=client.id,
            topic_id=topic.id,
            total_amount=initial_amount,
        )
        db.add(subscription)

    db.commit()
    return client


def add_content_with_metric_values(
    db: Session,
    source_id: int,
    title: str,
    url: str,
    content_text: str,
    metric_values: dict,
) -> models.Content:
    content = models.Content(
        source_id=source_id,
        title=title,
        url=url,
        content=content_text
    )
    db.add(content)
    db.flush()

    for metric_id, value in metric_values.items():
        metric_value = models.MetricValue(
            metric_id=metric_id,
            value=value,
            calculated_on=datetime.datetime.now(datetime.timezone.utc)
        )
        db.add(metric_value)

    db.commit()
    return content


def process_metric_value(db: Session, metric_id: int, value: Decimal, calculated_on: datetime.datetime = None, _artificial_delay: float = 0):
    if calculated_on is None:
        calculated_on = models.now()

    metric = db.query(models.Metric).options(
        load_only(models.Metric.id, models.Metric.topic_ids)
    ).get(metric_id)

    if not metric:
        raise ValueError(f"Metric with ID {metric_id} not found")

    db.execute(
        insert(models.MetricValue).values(
            metric_id=metric_id,
            value=value,
            calculated_on=calculated_on
        )
    )

    subscriptions = db.query(
        models.Subscription.id,
        models.Subscription.single_metric_pricing,
        models.Subscription.total_amount
    ).join(models.Topic).filter(
        models.Topic.id.in_(metric.topic_ids)
    ).all()

    for subscription in subscriptions:
        db.execute(
            insert(models.Transaction).values(
                subscription_id=subscription.id,
                metric_id=metric_id,
                amount=subscription.single_metric_pricing,
                created_at=calculated_on
            )
        )
        # Isolation check // add amount from transaction to subscription total money spent
        db.execute(
            update(models.Subscription)
            .where(models.Subscription.id == subscription.id)
            .values(total_amount=models.Subscription.total_amount + subscription.single_metric_pricing)
        )
        if _artificial_delay:
            time.sleep(_artificial_delay)
    db.commit()
