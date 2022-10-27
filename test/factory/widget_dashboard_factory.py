import factory
from spaceone.core import utils

from spaceone.dashboard.model import Widget


class WidgetFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = Widget