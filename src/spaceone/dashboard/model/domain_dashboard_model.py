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


class DomainDashboard(MongoModel):
    domain_dashboard_id = StringField(max_length=40, generate_id='domain-dash', unique=True)
    name = StringField(max_length=255)
    scope = StringField(max_length=255, choices=('DOMAIN', 'USER'))
    layouts = ListField(default=[])
    options = EmbeddedDocumentField(Options, default=Options)
    default_variables = DictField(default={})
    labels = ListField(default=[])
    tags = DictField(default={})
    user_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'layouts',
            'options',
            'default_variables',
            'labels',
            'tags'
        ],
        'minimal_fields': [
            'domain_dashboard_id',
            'name',
            'scope',
            'options',
            'default_variables',
            'labels',
            'user_id',
            'domain_id'
        ],
        'ordering': ['-created_at'],
        'indexes': [
            'name',
            'scope',
            'labels',
            'user_id',
            'domain_id'
        ]
    }
