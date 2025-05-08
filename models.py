import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.dialects.postgresql import ARRAY
from config import Config
from enum import Enum

Model = declarative_base()
Model.metadata.schema = Config.DB_SCHEMA


def now():
    return datetime.datetime.now(datetime.timezone.utc)


class ModelMixin:
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=now, nullable=False)

    @declared_attr
    def __tablename__(self):
        # Auto-generate table name from class name (lowercase)
        return self.__name__.lower()


class Client(ModelMixin, Model):
    name = Column(String)
    subscriptions = relationship("Subscription", back_populates="client")


class Subscription(ModelMixin, Model):
    topic_id = Column(Integer, ForeignKey('topic.id'))
    topic = relationship('Topic', back_populates='subscriptions')

    client_id = Column(Integer, ForeignKey('client.id'))
    client = relationship('Client', back_populates='subscriptions')

    # Total amount spent
    total_amount = Column(Numeric(precision=10, scale=4), nullable=False, default=0.0)
    single_metric_pricing = Column(Numeric(precision=10, scale=4), nullable=False, default=1.0)
    transactions = relationship("Transaction", back_populates="subscription")


class Transaction(ModelMixin, Model):
    subscription_id = Column(Integer, ForeignKey('subscription.id'))
    subscription = relationship('Subscription', back_populates='transactions')

    metric_id = Column(Integer, ForeignKey('metric.id'))
    metric = relationship('Metric', back_populates='transactions')

    # Increase by single_metric_pricing of related subscription
    amount = Column(Numeric(precision=10, scale=4))


class ScheduleFrequency(Enum):
    HOURLY = 'hourly'
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'
    QUARTERLY = 'quarterly'
    YEARLY = 'yearly'


class Source(ModelMixin, Model):
    name = Column(String)
    url = Column(String)
    schedules = Column(ARRAY(String))
    contents = relationship("Content", back_populates="source")


class Content(ModelMixin, Model):
    title = Column(String)
    url = Column(String)
    content = Column(String)

    source_id = Column(Integer, ForeignKey('source.id'))
    source = relationship('Source', back_populates='contents')


class Topic(ModelMixin, Model):
    name = Column(String)
    subscriptions = relationship("Subscription", back_populates="topic")


class Metric(ModelMixin, Model):
    name = Column(String)

    topic_ids = Column(ARRAY(Integer))
    metric_values = relationship("MetricValue", back_populates="metric")
    transactions = relationship("Transaction", back_populates="metric")


class MetricValue(ModelMixin, Model):
    value = Column(Numeric(precision=10, scale=4))
    calculated_on = Column(DateTime)

    metric_id = Column(Integer, ForeignKey('metric.id'))
    metric = relationship('Metric', back_populates='metric_values')
