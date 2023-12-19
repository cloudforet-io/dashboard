from spaceone.core.pygrpc.server import GRPCServer
from spaceone.dashboard.interface.grpc.public_dashboard import PublicDashboard
from spaceone.dashboard.interface.grpc.private_dashboard import PrivateDashboard

__all__ = ["app"]

app = GRPCServer()
app.add_service(PublicDashboard)
app.add_service(PrivateDashboard)
