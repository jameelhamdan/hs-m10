import datetime
import random
import time
from faker import Faker
from database import with_database
import models


SEED = 42
PROGRESS_STEP = 4


def test_create_performance(count: int):
    random.seed(SEED)
    fake = Faker()
    Faker.seed(SEED)

    with with_database() as db:
        source_ids = [
            m.id for m in db.query(models.Source).all()
        ]

    print(f"Starting performance test for MONGO INSERT")

    start_time = time.perf_counter()

    progress_step = count // PROGRESS_STEP  # 5% steps
    for i in range(1, count + 1):
        content = models.Content(
            title=fake.sentence(),
            url=fake.url(),
            content=fake.text(max_nb_chars=2000),
            source_id=random.choice(source_ids),
            created_at=fake.date_time_between(start_date='-1y', tzinfo=datetime.timezone.utc),
            updated_at=fake.date_time_between(start_date='-1y', tzinfo=datetime.timezone.utc),
        )
        content.save(validate=False, write_concern={'w': 0})

        if i % progress_step == 0 or i == count and count > 0:
            elapsed_now = time.perf_counter() - start_time
            print(f'Progress: {int((i / count) * 100)}% ({i}/{count}) in {elapsed_now:.4f} seconds')

    end_time = time.perf_counter()

    elapsed = end_time - start_time
    print(f"create_documents(count={count}) completed in {elapsed:.4f} seconds")
    print(f'with ({(count / elapsed):.4f}) per seconds')


def test_update_performance():
    random.seed(SEED)
    fake = Faker()
    Faker.seed(SEED)

    print(f"Starting performance test for MONGO UPDATE...")

    start_time = time.perf_counter()
    count = models.Content.objects.count()
    progress_step = count // PROGRESS_STEP  # 5% steps
    i = 0
    for content in models.Content.objects:
        i += 1
        content.title = 'UPDATED ' + fake.sentence()
        content.content = fake.text(max_nb_chars=2000)
        content.updated_at = models.now()
        content.save(validate=False, write_concern={'w': 0})

        if i % progress_step == 0 or i == count and count > 0:
            elapsed_now = time.perf_counter() - start_time
            print(f'Progress: {int((i / count) * 100)}% ({i}/{count}) in {elapsed_now:.4f} seconds')

    end_time = time.perf_counter()

    elapsed = end_time - start_time
    print(f"update_document(count={count}) completed in {elapsed:.4f} seconds")
    print(f'with ({(count / elapsed):.4f}) per seconds')


if __name__ == '__main__':
    test_create_performance(10000)
    test_update_performance()
