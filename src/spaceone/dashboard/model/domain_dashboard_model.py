from mongoengine import *
from spaceone.core.error import ERROR_NOT_UNIQUE

from spaceone.core.model.mongo_model import MongoModel


class DomainDashboard(MongoModel):
    domain_dashboard_id = StringField(max_length=40, generate_id='domain-dash', unique=True)
    name = StringField(max_length=100)
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

    @classmethod
    def create(cls, data):
        domain_dashboard_vos = cls.filter(name=data['name'], user_id=data['user_id'], domain_id=data['domain_id'])
        print(domain_dashboard_vos)
        if domain_dashboard_vos.count() > 0:
            raise ERROR_NOT_UNIQUE(key='name', value=data['name'])
        return super().create(data)

    def update(self, data):
        if 'name' in data:
            domain_dashboard_vos = self.filter(name=data['name'], user_id=self.user_id, domain_id__ne=self.domain_id)
            if domain_dashboard_vos.count() > 0:
                raise ERROR_NOT_UNIQUE(key='name', value=data['name'])

        return super().update(data)
