from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Period(EmbeddedDocument):
    start = StringField(required=True)
    end = StringField(required=True)

    def to_dict(self):
        return dict(self.to_mongo())


class DateRange(EmbeddedDocument):
    enabled = BooleanField()
    period_type = StringField(max_length=255, choices=('AUTO', 'FIXED'))
    period = EmbeddedDocumentField(Period)


class Currency(EmbeddedDocument):
    enabled = BooleanField()


class Options(EmbeddedDocument):
    date_range = EmbeddedDocumentField(DateRange, default=DateRange)
    currency = EmbeddedDocumentField(Currency, default=Currency)


class Widget(MongoModel):
    widget_id = StringField(max_length=40, generate_id='widget', unique=True)
    name = StringField(max_length=255)
    options = EmbeddedDocumentField(Options, default=Options)
    variables = DictField(default={})
    schema = DictField(default={})
    labels = ListField(StringField())
    tags = ListField(StringField())
    user_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'options',
            'variables',
            'schema',
            'labels',
            'tags'
        ],
        'minimal_fields': [
            'widget_id',
            'name',
            'options',
            'variables',
            'labels',
            'user_id',
            'domain_id'
        ],
        'ordering': ['name'],
        'indexes': [
            'name',
            'widget_id',
        ]
    }
