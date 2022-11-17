from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class DateRange(EmbeddedDocument):
    enabled = BooleanField(default=False)


class Currency(EmbeddedDocument):
    enabled = BooleanField(default=False)


class Settings(EmbeddedDocument):
    date_range = EmbeddedDocumentField(DateRange, default=DateRange)
    currency = EmbeddedDocumentField(Currency, default=Currency)

    def to_dict(self):
        return self.to_mongo()


class ProjectDashboard(MongoModel):
    project_dashboard_id = StringField(max_length=40, generate_id='project-dash', unique=True)
    name = StringField(max_length=255)
    viewers = StringField(max_length=255, choices=('PUBLIC', 'PRIVATE'))
    version = IntField(default=1)
    layouts = ListField(DictField(default={}))
    dashboard_options = DictField(default={})
    settings = EmbeddedDocumentField(Settings, default=Settings)
    dashboard_options_schema = DictField(default={})
    labels = ListField(StringField())
    tags = DictField(default={})
    user_id = StringField(max_length=40)
    project_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'layouts',
            'dashboard_options',
            'settings',
            'dashboard_options_schema',
            'labels',
            'tags'
        ],
        'minimal_fields': [
            'project_dashboard_id',
            'name',
            'viewers',
            'version',
            'project_id'
            'user_id',
            'domain_id'
        ],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'ordering': ['name'],
        'indexes': [
            'name',
            'viewers',
            'labels',
            'project_id',
            'user_id',
            'domain_id'
        ]
    }
