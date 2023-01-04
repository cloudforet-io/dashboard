from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class CustomWidget(MongoModel):
    custom_widget_id = StringField(max_length=40, generate_id='custom-widget', unique=True)
    widget_name = StringField(max_length=40)
    title = StringField(max_length=255)
    version = StringField(max_length=40)
    options = DictField(default={})
    settings = DictField(default={})
    inherit_options = DictField(default={})
    labels = ListField(StringField())
    tags = DictField(default={})
    user_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'widget_name',
            'title',
            'options',
            'settings',
            'inherit_options',
            'labels',
            'tags'
        ],
        'minimal_fields': [
            'custom_widget_id',
            'widget_name',
            'title',
            'version',
            'user_id',
            'domain_id'
        ],
        'ordering': ['title'],
        'indexes': [
            'widget_name',
            'title',
            'version',
            'user_id',
            'domain_id'
        ]
    }
