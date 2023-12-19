from spaceone.core.pygrpc.server import GRPCServer
from spaceone.dashboard.interface.grpc.public_dashboard import PublicDashboard

__all__ = ["app"]

app = GRPCServer()
app.add_service(PublicDashboard)
