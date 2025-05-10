import decimal
import random
import datetime
import threading
import time
from decimal import Decimal

from faker import Faker

from database import with_database
import models
import sqlalchemy.exc

SEED = 42
source_names = ['Reuters', 'AP', 'Bloomberg', 'CNN', 'BBC', 'New York Times', 'Washington Post', 'The Guardian']

topic_names = [
    'Technology',
    'Finance',
    'Health',
    'Politics',
    'Entertainment',
    'Sports',
    'Science',
    'Business',
    'Education',
    'Environment',
]

metric_names = [
    'Page Views', 'Engagement Score', 'Subscription Rate', 'Bounce Rate', 'Social Shares', 'Reading Time'
]


def _populate(db):
    fake = Faker()
    Faker.seed(SEED)
    random.seed(SEED)

    sources = []
    for name in source_names:
        source = models.Source(
            name=name,
            url=fake.url(),
            schedules=[random.choice(list(models.ScheduleFrequency)).value for _ in range(2)]
        )
        db.add(source)
        sources.append(source)
    db.commit()

    topics = []
    for name in topic_names:
        topic = models.Topic(name=name)
        db.add(topic)
        topics.append(topic)
    db.commit()

    metrics = []
    for name in metric_names:
        metric = models.Metric(
            name=name,
            topic_ids=[random.choice([t.id for t in topics]) for _ in range(random.randint(1, 3))]
        )
        db.add(metric)
        metrics.append(metric)
    db.commit()

    contents = []
    for _ in range(random.randint(200, 300)):
        content = models.Content(
            title=fake.sentence(),
            url=fake.url(),
            content=fake.text(max_nb_chars=2000),
            source_id=random.choice(sources).id,
            created_at=fake.date_time_between(start_date='-1y', tzinfo=datetime.timezone.utc)
        )
        db.add(content)
        contents.append(content)
    db.commit()

    # Create clients (10-20)
    clients = []
    for _ in range(random.randint(10, 20)):
        client = models.Client(
            name=fake.company(),
            created_at=fake.date_time_between(start_date='-2y', tzinfo=datetime.timezone.utc)
        )
        db.add(client)
        clients.append(client)
    db.commit()

    # Create subscriptions (3-5 per client)
    subscriptions = []
    for client in clients:
        for _ in range(random.randint(3, 5)):
            topic = random.choice(topics)
            total_amount = Decimal(random.randint(100, 10000) / 100)
            single_metric_pricing = total_amount / random.randint(5, 25)

            subscription = models.Subscription(
                topic_id=topic.id,
                client_id=client.id,
                total_amount=total_amount,
                single_metric_pricing=single_metric_pricing,
                created_at=fake.date_time_between(
                    start_date=client.created_at,
                    end_date='now',
                    tzinfo=datetime.timezone.utc
                )
            )
            db.add(subscription)
            subscriptions.append(subscription)
    db.commit()

    # Create transactions (5-20 per subscription)
    transactions = []
    for subscription in subscriptions:
        remaining_amount = subscription.total_amount

        while remaining_amount > 0:
            # Random amount but leave enough for remaining transactions
            amount = subscription.single_metric_pricing

            remaining_amount -= amount

            transaction = models.Transaction(
                subscription_id=subscription.id,
                metric_id=random.choice(metrics).id,
                amount=amount,
                created_at=fake.date_time_between(
                    start_date=subscription.created_at,
                    end_date='now',
                    tzinfo=datetime.timezone.utc
                )
            )
            db.add(transaction)
            transactions.append(transaction)
            db.commit()

            # Create metric values (daily for last year for each metric)
            metric_values = []
            for metric in metrics:
                current_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365)
            end_date = datetime.datetime.now(datetime.timezone.utc)

        if current_date and end_date:
            while current_date <= end_date:
                # Skip some days randomly to make it more realistic
                if random.random() > 0.2:  # 80% chance to create a value for this day
                    value = Decimal(random.randint(1, 10000) / 100)

                    metric_value = models.MetricValue(
                        metric_id=metric.id,
                        value=value,
                        calculated_on=current_date
                    )
                    db.add(metric_value)
                    metric_values.append(metric_value)

                current_date += datetime.timedelta(days=1)
    db.commit()

    print(f"Database populated with:")
    print(f"- {len(sources)} sources")
    print(f"- {len(topics)} topics")
    print(f"- {len(metrics)} metrics")
    print(f"- {len(contents)} content items")
    print(f"- {len(clients)} clients")
    print(f"- {len(subscriptions)} subscriptions")
    print(f"- {len(transactions)} transactions")
    print(f"- {len(metric_values)} metric values")


def populate():
    with with_database() as _db:
        _populate(_db)


def test_performance(count: int):
    random.seed(SEED)

    from business import process_metric_value

    with with_database() as db:
        metric_ids = [
            m.id for m in db.query(models.Metric).all()
        ]

    print(f"Starting performance test for metrics: {metric_ids}...")

    start_time = time.perf_counter()

    progress_step = count // 20  # 5% steps

    for i in range(count):
        if i == 0:
            continue

        metric_id = random.choice(metric_ids)
        value = Decimal(random.randint(1, 10000) / 100)
        with with_database() as db:

            process_metric_value(db, metric_id=metric_id, value=value)
            if i % progress_step == 0 or i == count and count > 0 :
                elapsed_now = time.perf_counter() - start_time
                print(f'Progress: {int((i / count) * 100)}% ({i}/{count}) in {elapsed_now:.4f} seconds')

    end_time = time.perf_counter()

    elapsed = end_time - start_time
    print(f"process_metric_value(count={count}) completed in {elapsed:.4f} seconds")
    print(f'with ({(count / elapsed):.4f}) per seconds')


def test_concurrent_subscription_updates(solved: bool = False):
    from business import process_metric_value

    random.seed(SEED)

    def process_metric_thread(_metric_id, _value):
        try:
            with with_database(
                isolation_level='AUTOCOMMIT' if solved else 'SERIALIZABLE',
            ) as _db, _db.begin():
                process_metric_value(
                    db=_db,
                    metric_id=_metric_id,
                    value=_value,
                    calculated_on=datetime.datetime.now(),
                    _artificial_delay=random.random() * 0.1,  # reduced delay
                )
        except Exception as e:
            errors.append(e)
            raise

    with with_database() as db:
        # Setup test data - ensure clean state
        db.query(models.Transaction).delete()
        db.query(models.MetricValue).delete()
        db.query(models.Subscription).delete()
        db.query(models.Metric).delete()
        db.commit()

        client = db.query(models.Client).first()
        topic = db.query(models.Topic).first()

        subscription = models.Subscription(
            topic_id=topic.id,
            client_id=client.id,
            total_amount=Decimal("0.00"),
            single_metric_pricing=Decimal("1.00"),
        )

        metric = models.Metric(
            name='Guaranteed Concurrency Failure Test',
            topic_ids=[topic.id],
        )

        db.add_all([subscription, metric])
        db.commit()

        errors = []
        threads = [
            threading.Thread(
                target=process_metric_thread,
                args=(metric.id, subscription.single_metric_pricing),
                name=f'Worker-1'
            ),
            threading.Thread(
                target=process_metric_thread,
                args=(metric.id, subscription.single_metric_pricing),
                name='Worker-2'
            )
        ]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        db.refresh(subscription)

        # This WILL fail due to the isolation problem
        assert subscription.total_amount == Decimal("2.00"), (
            f"Race condition triggered! total_amount is {subscription.total_amount} "
            f"when it should be 2.00. "
            f"This proves the isolation error exists."
        )


if __name__ == '__main__':
    # populate()
    # test_performance(100)
    try:
        test_concurrent_subscription_updates(solved=False)
        print('Error did not happen as expected!')
    except (AssertionError, sqlalchemy.exc.OperationalError) as e:
        print('Error Happens as expected: ', e)

    test_concurrent_subscription_updates(solved=True)
    print('\n\nIf you see this, the isolation error was not triggered by solved function and updating ISOLATION LEVEL from "SERIALIZABLE" to "AUTOCOMMIT"')
