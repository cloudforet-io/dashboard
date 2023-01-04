from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class DomainDashboard(MongoModel):
    domain_dashboard_id = StringField(max_length=40, generate_id='domain-dash', unique=True)
    name = StringField(max_length=255, unique_with='domain_id')
    viewers = StringField(max_length=255, choices=('PUBLIC', 'PRIVATE'))
    version = IntField(default=1)
    layouts = ListField(default=[])
    variables = DictField(default={})
    settings = DictField(default={})
    variables_schema = DictField(default={})
    labels = ListField(StringField())
    tags = DictField(default={})
    user_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'layouts',
            'variables',
            'settings',
            'variables_schema',
            'labels',
            'tags'
        ],
        'minimal_fields': [
            'domain_dashboard_id',
            'name',
            'viewers',
            'version',
            'user_id',
            'domain_id'
        ],
        'ordering': ['name'],
        'indexes': [
            'name',
            'viewers',
            'labels',
            'user_id',
            'domain_id'
        ]
    }
