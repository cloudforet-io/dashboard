from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class DateRange(EmbeddedDocument):
    period_type = StringField(max_length=255, choices=('AUTO', 'FIXED'))
    start = StringField(required=True)
    end = StringField(required=True)


class Options(EmbeddedDocument):
    date_range = EmbeddedDocumentField(DateRange, default=DateRange)


class Widget(MongoModel):
    widget_id = StringField(max_length=40, generate_id='widget', unique=True)
    name = StringField(max_length=255)
    view_mode = StringField(max_length=255, choices=('AUTO', 'FULL'))
    options = EmbeddedDocumentField(Options, default=Options)
    variables = DictField(default={})
    schema = DictField(default={})
    labels = ListField(StringField, default=[])
    tags = DictField(default={})
    user_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
        ],
        'minimal_fields': [
            'widget_id',
            'name',
            'view_mode',
            'options',
            'variables',
            'labels',
            'user_id',
            'domain_id'
        ],
        'ordering': ['-created_at'],
        'indexes': [
        ]
    }
