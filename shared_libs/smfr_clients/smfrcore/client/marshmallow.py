# -*- coding:utf-8 -*-
# this is auto-generated by swagger-marshmallow-codegen
from smfrcore.client._marshmallow_custom import BaseSchema
from marshmallow import fields
from swagger_marshmallow_codegen.fields import (
    DateTime
)
from marshmallow.validate import (
    Length,
    OneOf,
    Regexp
)
from swagger_marshmallow_codegen.validate import ItemsRange
import re


class Aggregation(BaseSchema):
    id = fields.Integer()
    collection_id = fields.Integer()
    data = fields.Field()
    last_tweetid_annotated = fields.Integer()
    last_tweetid_collected = fields.Integer()
    last_tweetid_geotagged = fields.Integer()
    timestamp_start = DateTime()
    timestamp_end = DateTime()


class Collection(BaseSchema):
    id = fields.Integer()
    trigger = fields.String(validate=[OneOf(choices=['background', 'on-demand', 'manual'], labels=[])])
    ctype = fields.String(validate=[OneOf(choices=['keywords', 'geo'], labels=[])])
    forecast_id = fields.String()
    tracking_keywords = fields.List(fields.String())
    locations = fields.List(fields.String())
    languages = fields.List(fields.String())
    runtime = DateTime()
    nuts2 = fields.String()
    nuts2source = fields.String()
    status = fields.String(validate=[OneOf(choices=['active', 'inactive'], labels=[])])
    started_at = DateTime()
    stopped_at = DateTime()
    bboxfinder = fields.String()


class CollectorPayload(BaseSchema):
    trigger = fields.String(required=True, validate=[OneOf(choices=['background', 'manual'], labels=[])])
    forecast_id = fields.String()
    runtime = DateTime()
    nuts2 = fields.String()
    keywords = fields.String()
    bounding_boxes = fields.String()
    configuration = fields.Field()


class CollectorResponse(BaseSchema):
    collection = fields.Nested('Collection')
    aggregation = fields.Nested('Aggregation')
    id = fields.Integer()


class OnDemandPayload(BaseSchema):
    efas_id = fields.Integer()
    forecast = fields.String()
    keywords = fields.String()
    trigger = fields.String(validate=[OneOf(choices=['background', 'on-demand', 'manual'], labels=[])])
    bbox = fields.Field()
    lead_time = fields.Integer()
    nuts = fields.String()


class Collector(BaseSchema):
    id = fields.Integer()
    collection_id = fields.Integer()
    parameters = fields.String()


class CollectionStats(BaseSchema):
    tweets_count = fields.Integer()
    tweets_annotated = fields.Integer()
    tweets_geotagged = fields.Integer()
    tweets_day_avg = fields.Float()


class CollectionTweetSample(BaseSchema):
    tweetid = fields.String(required=True)
    collectionid = fields.Integer()
    tweet = fields.Field(required=True)
    annotations = fields.Field()
    nuts3 = fields.String()
    latlong = fields.List(fields.Float(), validate=[ItemsRange(min=2, max=2)])
    ttype = fields.String(validate=[OneOf(choices=['annotated', 'collected', 'geotagged'], labels=[])])
    created_at = DateTime(required=True)


class CollectionResponse(BaseSchema):
    collection = fields.Nested('Collection')
    collector = fields.Nested('Collector')
    stats = fields.Nested('CollectionStats')
    running_annotators = fields.List(fields.Field('CollectionResponseRunning_annotatorsItem'))
    samples = fields.List(fields.Nested('CollectionTweetSample'))


class CollectionResponseRunning_annotatorsItem(BaseSchema):
    pass


class Login(BaseSchema):
    email = fields.String(required=True, validate=[Length(min=1, max=None, equal=None)])
    password = fields.String(required=True, validate=[Length(min=1, max=None, equal=None)])


class User(BaseSchema):
    """a registered user"""
    id = fields.Integer()
    name = fields.String()
    email = fields.String(required=True, description='username must be unique', validate=[Length(min=8, max=200, equal=None), Regexp(regex=re.compile('^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+.[a-zA-Z0-9-.]+$'))])
    password_hash = fields.String(description='Hashed Password')
