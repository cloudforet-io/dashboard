# Database Settings
DATABASES = {
    "default": {
        "db": "dashboard",
        "host": "localhost",
        "port": 27017,
        "username": "",
        "password": "",
    }
}

# Cache Settings
CACHES = {
    "default": {
        # Redis Example
        # 'backend': 'spaceone.core.cache.redis_cache.RedisCache',
        # 'host': '<host>',
        # 'port': 6379,
    }
}

# Handler Configuration
HANDLERS = {
    "authentication": [
        # Default Authentication Handler
        # {
        #     'backend': 'spaceone.core.handler.authentication_handler.AuthenticationGRPCHandler',
        #     'uri': 'grpc://identity:50051/v1/Domain/get_public_key'
        # }
    ],
    "authorization": [
        # Default Authorization Handler
        # {
        #     'backend': 'spaceone.core.handler.authorization_handler.AuthorizationGRPCHandler',
        #     'uri': 'grpc://identity:50051/v1/Authorization/verify'
        # }
    ],
    "mutation": [],
    "event": [],
}

# Connector Settings
CONNECTORS = {
    "SpaceConnector": {
        "backend": "spaceone.core.connector.space_connector:SpaceConnector",
        "endpoints": {
            "identity": "grpc://identity:50051",
            "inventory": "grpc://inventory:50051",
            "cost_analysis": "grpc://cost-analysis:50051",
        },
    }
}

# Log Settings
LOG = {}

# System Token Settings
TOKEN = ""
