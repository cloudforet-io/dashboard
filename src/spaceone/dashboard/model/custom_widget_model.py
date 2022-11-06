from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CustomWidget(MongoModel):
    custom_widget_id = StringField(max_length=40, generate_id='custom-widget', unique=True)
    widget_id = StringField(max_length=40)
    name = StringField(max_length=255)
    version = StringField(max_length=40)
    widget_options = DictField(default={})
    inherit_options = DictField(default={})
    tags = DictField(default={})
    user_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'name',
            'widget_id',
            'widget_options',
            'inherit_options',
            'tags'
        ],
        'minimal_fields': [
            'custom_widget_id'
            'widget_id',
            'name',
            'version',
            'user_id',
            'domain_id'
        ],
        'ordering': ['name'],
        'indexes': [
            'name',
            'widget_id',
            'version',
            'user_id',
            'domain_id'
        ]
    }
