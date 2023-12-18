import factory
from spaceone.core import utils

from spaceone.dashboard.model import CustomWidget


class CustomWidgetFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = CustomWidget

    custom_widget_id = factory.LazyAttribute(
        lambda o: utils.generate_id("custom-widget")
    )
    widget_name = factory.LazyAttribute(lambda o: utils.random_string())
    title = factory.LazyAttribute(lambda o: utils.random_string())
    version = "v1"
    options = {
        "group_by": "product",
    }
    inherit_options = {"group_by": {"enabled": True}}
    tags = {"type": "test", "env": "dev"}
    user_id = "cloudforet@gmail.com"
    domain_id = factory.LazyAttribute(lambda o: utils.generate_id("domain"))
    created_at = factory.Faker("date_time")
    updated_at = factory.Faker("date_time")
