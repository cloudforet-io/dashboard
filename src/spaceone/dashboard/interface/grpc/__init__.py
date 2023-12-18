from spaceone.core.pygrpc.server import GRPCServer
from spaceone.dashboard.interface.grpc.dashboard import Dashboard

__all__ = ["app"]

app = GRPCServer()
app.add_service(Dashboard)
