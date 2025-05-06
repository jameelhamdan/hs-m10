import random
from faker import Faker
from database import with_database
from models import InformationBlock, Schedule, Topic, Client, Source, Metric

source_names = ['Reuters', 'AP', 'Bloomberg', 'CNN', 'BBC', 'New York Times', 'Washington Post', 'The Guardian']
schedule_frequencies = ['daily', 'weekly', 'monthly', 'hourly', 'quarterly']
topic_names = [
    'Technology', 'Finance', 'Health', 'Politics', 'Entertainment',
    'Sports', 'Science', 'Business', 'Education', 'Environment'
]

def populate(db, amount = 100):
    fake = Faker()

    schedules = []
    for _ in range(5):
        schedule = Schedule(
            frequency=random.choice(schedule_frequencies)
        )
        schedules.append(schedule)
    db.add_all(schedules)
    db.commit()

    topics = []

    for i, name in enumerate(topic_names):
        topic = Topic(
            name=name,
            schedule_id=random.choice([s.id for s in schedules])
        )
        topics.append(topic)
    db.add_all(topics)
    db.commit()

    clients = []
    for _ in range(amount):
        client = Client(
            name=fake.company()
        )

        client.topics = random.sample(topics, random.randint(1, 3))
        clients.append(client)
    db.add_all(clients)
    db.commit()

    sources = []
    for name in source_names:
        source = Source(
            name=name,
            schedule_id=random.choice([s.id for s in schedules])
        )
        sources.append(source)

    db.add_all(sources)
    db.commit()

    information_blocks = []
    for _ in range(amount * 3):
        info_block = InformationBlock(
            title=fake.sentence(),
            content=fake.text(max_nb_chars=500),
            happened_on=fake.date_time_between(start_date='-1y', end_date='now'),
            source_id=random.choice([s.id for s in sources])
        )

        info_block.topics = random.sample(topics, random.randint(1, 2))
        information_blocks.append(info_block)

    db.add_all(information_blocks)
    db.commit()

    # Create metrics
    metrics = []
    for topic in topics:
        for _ in range(random.randint(1, 3)):
            metric = Metric(
                value=round(random.uniform(0.0, 100.0), 2),
                calculated_on=fake.date_time_between(start_date='-1y', end_date='now'),
                topic_id=topic.id
            )
            metrics.append(metric)

    db.add_all(metrics)
    db.commit()

    print(f"Successfully created dummy data:")
    print(f"- {len(clients)} clients")
    print(f"- {len(schedules)} schedules")
    print(f"- {len(sources)} sources")
    print(f"- {len(topics)} topics")
    print(f"- {len(information_blocks)} information blocks")
    print(f"- {len(metrics)} metrics")


def print_all(db):
    information_blocks = db.query(InformationBlock).all()
    print('Information Blocks', len(information_blocks))

    for information_block in information_blocks:
        print(information_block.id, information_block.title)


if __name__ == '__main__':
    with with_database() as _db:
        # populate(_db)
        print_all(_db)
