from spaceone.api.dashboard.v1 import public_folder_pb2, public_folder_pb2_grpc
from spaceone.core.pygrpc import BaseAPI
from spaceone.dashboard.service.public_folder_service import PublicFolderService


class PublicFolder(BaseAPI, public_folder_pb2_grpc.PublicFolderServicer):
    pb2 = public_folder_pb2
    pb2_grpc = public_folder_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_folder_svc = PublicFolderService(metadata)
        response: dict = pub_folder_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_folder_svc = PublicFolderService(metadata)
        response: dict = pub_folder_svc.update(params)
        return self.dict_to_message(response)

    def share(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_folder_svc = PublicFolderService(metadata)
        response: dict = pub_folder_svc.share(params)
        return self.dict_to_message(response)

    def unshare(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_folder_svc = PublicFolderService(metadata)
        response: dict = pub_folder_svc.unshare(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_folder_svc = PublicFolderService(metadata)
        pub_folder_svc.delete(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_folder_svc = PublicFolderService(metadata)
        response: dict = pub_folder_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_folder_svc = PublicFolderService(metadata)
        response: dict = pub_folder_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        pub_folder_svc = PublicFolderService(metadata)
        response: dict = pub_folder_svc.stat(params)
        return self.dict_to_message(response)
