from spaceone.api.dashboard.v1 import private_dashboard_pb2, private_dashboard_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class PrivateDashboard(BaseAPI, private_dashboard_pb2_grpc.PrivateDashboardServicer):
    pb2 = private_dashboard_pb2
    pb2_grpc = private_dashboard_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "PrivateDashboardService", metadata
        ) as dashboard_service:
            return self.locator.get_info(
                "PrivateDashboardInfo", dashboard_service.create(params)
            )

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "PrivateDashboardService", metadata
        ) as dashboard_service:
            return self.locator.get_info(
                "PrivateDashboardInfo", dashboard_service.update(params)
            )

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "PrivateDashboardService", metadata
        ) as dashboard_service:
            dashboard_service.delete(params)
            return self.locator.get_info("EmptyInfo")

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "PrivateDashboardService", metadata
        ) as dashboard_service:
            return self.locator.get_info(
                "PrivateDashboardInfo", dashboard_service.get(params)
            )

    def delete_version(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "PrivateDashboardService", metadata
        ) as dashboard_service:
            dashboard_service.delete_version(params)
            return self.locator.get_info("EmptyInfo")

    def revert_version(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "PrivateDashboardService", metadata
        ) as dashboard_service:
            return self.locator.get_info(
                "PrivateDashboardInfo", dashboard_service.revert_version(params)
            )

    def get_version(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "PrivateDashboardService", metadata
        ) as dashboard_service:
            return self.locator.get_info(
                "PrivateDashboardVersionInfo",
                dashboard_service.get_version(params),
            )

    def list_versions(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "PrivateDashboardService", metadata
        ) as dashboard_service:
            (
                private_dashboard_version_vos,
                total_count,
                private_dashboard_version,
            ) = dashboard_service.list_versions(params)
            return self.locator.get_info(
                "PrivateDashboardVersionsInfo",
                private_dashboard_version_vos,
                total_count,
                minimal=self.get_minimal(params),
                latest_version=private_dashboard_version,
            )

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "PrivateDashboardService", metadata
        ) as dashboard_service:
            private_dashboard_vos, total_count = dashboard_service.list(params)
            return self.locator.get_info(
                "PrivateDashboardsInfo",
                private_dashboard_vos,
                total_count,
                minimal=self.get_minimal(params),
            )

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "PrivateDashboardService", metadata
        ) as dashboard_service:
            return self.locator.get_info(
                "StatisticsInfo", dashboard_service.stat(params)
            )
