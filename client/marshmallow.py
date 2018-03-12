# -*- coding:utf-8 -*-
# this is auto-generated by swagger-marshmallow-codegen
from client._marshmallow_custom import BaseSchema
from marshmallow import fields
from marshmallow.validate import (
    OneOf
)
from swagger_marshmallow_codegen.fields import (
    DateTime
)
from swagger_marshmallow_codegen.validate import ItemsRange


class Collection(BaseSchema):
    id = fields.Integer()
    trigger = fields.String(validate=[OneOf(choices=['background', 'on-demand', 'manual'], labels=[])])
    ctype = fields.String(validate=[OneOf(choices=['keywords', 'geo'], labels=[])])
    forecast_id = fields.String()
    tracking_keywords = fields.List(fields.String())
    locations = fields.String()
    languages = fields.List(fields.String())
    runtime = DateTime()
    nuts3 = fields.String()
    nuts3source = fields.String()
    status = fields.String(validate=[OneOf(choices=['active', 'inactive'], labels=[])])
    started_at = DateTime()
    stopped_at = DateTime()


class CollectorPayload(BaseSchema):
    trigger = fields.String(required=True, validate=[OneOf(choices=['background', 'on-demand', 'manual'], labels=[])])
    forecast_id = fields.String()
    runtime = DateTime()
    nuts3 = fields.String()
    nuts3source = fields.String()


class CollectorResponse(BaseSchema):
    collection = fields.Nested('Collection')
    id = fields.Integer()


class CollectionStats(BaseSchema):
    tweets_count = fields.Integer()
    tweets_annotated = fields.Integer()
    tweets_geotagged = fields.Integer()
    tweets_day_avg = fields.Float()


class CollectionTweetSample(BaseSchema):
    tweetid = fields.String()
    collectionid = fields.Integer()
    text = fields.String()
    annotations = fields.List(fields.String())
    nuts3 = fields.String()
    latlong = fields.List(fields.Float(), validate=[ItemsRange(min=2, max=2)])
    ttype = fields.String(validate=[OneOf(choices=['annotated', 'collected', 'geotagged'], labels=[])])
    created_at = DateTime()


class CollectionResponse(BaseSchema):
    collection = fields.Nested('Collection')
    stats = fields.Nested('CollectionStats')
    samples = fields.List(fields.Nested('CollectionTweetSample'))
