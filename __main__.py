import asyncio
import sys

import grpc
import grpc.aio
from pulumi.runtime.proto import converter_pb2, converter_pb2_grpc

from account_scraper import scrape


class Converter(converter_pb2_grpc.ConverterServicer):
    def ConvertState(self, request, context):
        response = converter_pb2.ConvertStateResponse()
        import_file = scrape()

        for resource in import_file["resources"]:
            response.resources.append(converter_pb2.Resource(
                type=resource["type"],
                name=resource["name"],
                id=resource["id"],
            ))

        response.result = request.input.upper()
        return response

def main():
    async def serve() -> None:
        # _MAX_RPC_MESSAGE_SIZE raises the gRPC Max Message size from `4194304` (4mb) to `419430400` (400mb)
        _MAX_RPC_MESSAGE_SIZE = 1024 * 1024 * 400
        _GRPC_CHANNEL_OPTIONS = [("grpc.max_receive_message_length", _MAX_RPC_MESSAGE_SIZE)]

        server = grpc.aio.server(options=_GRPC_CHANNEL_OPTIONS)
        servicer = Converter()
        converter_pb2_grpc.add_ConverterServicer_to_server(servicer, server)
        port = server.add_insecure_port(address="127.0.0.1:0")
        await server.start()
        sys.stdout.buffer.write(f"{port}\n".encode())
        sys.stdout.buffer.flush()
        await server.wait_for_termination()

    try:
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(serve())
        finally:
            loop.close()
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()