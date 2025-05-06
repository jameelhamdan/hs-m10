from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Table, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from config import Config

Model = declarative_base()
Model.metadata.schema = Config.DB_SCHEMA

# Many-to-Many relationship between InformationBlock and Topic and Client and Topic
information_block_topic = Table(
    'information_block_topic', Model.metadata,
    Column('information_block_id', Integer, ForeignKey('information_block.id'), primary_key=True),
    Column('topic_id', Integer, ForeignKey('topic.id'), primary_key=True)
)

client_topic = Table(
    'client_topic', Model.metadata,
    Column('client_id', Integer, ForeignKey('client.id'), primary_key=True),
    Column('topic_id', Integer, ForeignKey('topic.id'), primary_key=True)
)


class Client(Model):
    __tablename__ = 'client'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    topics = relationship('Topic', secondary=client_topic, back_populates='clients')


class Schedule(Model):
    __tablename__ = 'schedule'

    id = Column(Integer, primary_key=True)
    frequency = Column(String)

    sources = relationship('Source', back_populates='schedule')
    topics = relationship('Topic', back_populates='schedule')


class Source(Model):
    __tablename__ = 'source'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    schedule_id = Column(Integer, ForeignKey('schedule.id'))
    schedule = relationship('Schedule', back_populates='sources')

    information_blocks = relationship('InformationBlock', back_populates='source')


class InformationBlock(Model):
    __tablename__ = 'information_block'

    id = Column(Integer, primary_key=True)
    title = Column(String)
    content = Column(String)
    happened_on = Column(DateTime)

    source_id = Column(Integer, ForeignKey('source.id'))
    source = relationship('Source', back_populates='information_blocks')

    topics = relationship('Topic', secondary=information_block_topic, back_populates='information_blocks')


class Topic(Model):
    __tablename__ = 'topic'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    schedule_id = Column(Integer, ForeignKey('schedule.id'))
    schedule = relationship('Schedule', back_populates='topics')

    information_blocks = relationship('InformationBlock', secondary=information_block_topic, back_populates='topics')
    clients = relationship('Client', secondary=client_topic, back_populates='topics')
    metrics = relationship('Metric', back_populates='topic')


class Metric(Model):
    __tablename__ = 'metric'

    id = Column(Integer, primary_key=True)
    value = Column(Numeric(precision=10, scale=4))
    calculated_on = Column(DateTime)

    topic_id = Column(Integer, ForeignKey('topic.id'))
    topic = relationship('Topic', back_populates='metrics')
