from spaceone.api.dashboard.v1 import (
    private_data_table_pb2,
    private_data_table_pb2_grpc,
)
from spaceone.core.pygrpc import BaseAPI
from spaceone.dashboard.service.private_data_table_service import (
    PrivateDataTableService,
)


class PrivateDataTable(BaseAPI, private_data_table_pb2_grpc.PrivateDataTableServicer):
    pb2 = private_data_table_pb2
    pb2_grpc = private_data_table_pb2_grpc

    def add(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_data_table_svc = PrivateDataTableService(metadata)
        response: dict = pri_data_table_svc.add(params)
        return self.dict_to_message(response)

    def transform(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_data_table_svc = PrivateDataTableService(metadata)
        response: dict = pri_data_table_svc.transform(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_data_table_svc = PrivateDataTableService(metadata)
        response: dict = pri_data_table_svc.update(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_data_table_svc = PrivateDataTableService(metadata)
        pri_data_table_svc.delete(params)
        return self.empty()

    def load(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_data_table_svc = PrivateDataTableService(metadata)
        response: dict = pri_data_table_svc.load(params)
        return self.dict_to_message(response)

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_data_table_svc = PrivateDataTableService(metadata)
        response: dict = pri_data_table_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        pri_data_table_svc = PrivateDataTableService(metadata)
        response: dict = pri_data_table_svc.list(params)
        return self.dict_to_message(response)
