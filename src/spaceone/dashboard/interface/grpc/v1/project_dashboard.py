from spaceone.api.dashboard.v1 import project_dashboard_pb2, project_dashboard_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class ProjectDashboard(BaseAPI, project_dashboard_pb2_grpc.ProjectDashboardServicer):
    pb2 = project_dashboard_pb2
    pb2_grpc = project_dashboard_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ProjectDashboardService', metadata) as project_dashboard_service:
            return self.locator.get_info('ProjectDashboardInfo', project_dashboard_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ProjectDashboardService', metadata) as project_dashboard_service:
            return self.locator.get_info('ProjectDashboardInfo', project_dashboard_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ProjectDashboardService', metadata) as project_dashboard_service:
            project_dashboard_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ProjectDashboardService', metadata) as project_dashboard_service:
            return self.locator.get_info('ProjectDashboardInfo', project_dashboard_service.get(params))

    def delete_version(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ProjectDashboardService', metadata) as project_dashboard_service:
            project_dashboard_service.delete_version(params)
            return self.locator.get_info('EmptyInfo')

    def revert_version(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ProjectDashboardService', metadata) as project_dashboard_service:
            return self.locator.get_info('ProjectDashboardInfo', project_dashboard_service.revert_version(params))

    def get_version(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ProjectDashboardService', metadata) as project_dashboard_service:
            return self.locator.get_info('ProjectDashboardVersionInfo', project_dashboard_service.get_version(params))

    def list_versions(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ProjectDashboardService', metadata) as project_dashboard_service:
            project_dashboard_version_vos, total_count, dashboard_version = project_dashboard_service.list_versions(
                params)
            return self.locator.get_info('ProjectDashboardVersionsInfo',
                                         project_dashboard_version_vos,
                                         total_count,
                                         minimal=self.get_minimal(params), latest_version=dashboard_version)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ProjectDashboardService', metadata) as project_dashboard_service:
            project_dashboard_vos, total_count = project_dashboard_service.list(params)
            return self.locator.get_info('ProjectDashboardsInfo',
                                         project_dashboard_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ProjectDashboardService', metadata) as project_dashboard_service:
            return self.locator.get_info('StatisticsInfo', project_dashboard_service.stat(params))
