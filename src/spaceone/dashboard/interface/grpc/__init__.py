from spaceone.core.pygrpc.server import GRPCServer
from spaceone.dashboard.interface.grpc.public_folder import PublicFolder
from spaceone.dashboard.interface.grpc.public_dashboard import PublicDashboard
from spaceone.dashboard.interface.grpc.public_widget import PublicWidget
from spaceone.dashboard.interface.grpc.public_data_table import PublicDataTable
from spaceone.dashboard.interface.grpc.private_folder import PrivateFolder
from spaceone.dashboard.interface.grpc.private_dashboard import PrivateDashboard
from spaceone.dashboard.interface.grpc.private_widget import PrivateWidget
from spaceone.dashboard.interface.grpc.private_data_table import PrivateDataTable

__all__ = ["app"]

app = GRPCServer()
app.add_service(PublicFolder)
app.add_service(PublicDashboard)
app.add_service(PublicWidget)
app.add_service(PublicDataTable)
app.add_service(PrivateFolder)
app.add_service(PrivateDashboard)
app.add_service(PrivateWidget)
app.add_service(PrivateDataTable)
