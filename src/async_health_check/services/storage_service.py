import uuid

from asgiref.sync import sync_to_async
from django.core.files.base import ContentFile
from django.core.files.storage import InvalidStorageError, storages
from django.core.files.storage import default_storage

from async_health_check.exceptions import ServiceUnavailable
from async_health_check.services.base_service import BaseService


class AsyncStorageHealthCheck(BaseService):
    """
    Asynchronous health check for a `StorageBackend`.

    Can be extended to test any storage backend by subclassing:

        class MyAsyncStorageHealthCheck(AsyncStorageHealthCheck):
            storage = 'some.other.StorageBackend'
    """

    SERVICE_CODE = "STORAGE"
    SERVICE_NAME = "Async Storage Health Check"

    storage_alias = None
    storage = None

    def __init__(self, critical_service: bool = True):
        super().__init__(critical_service)

    def get_storage(self):
        try:
            return storages[self.storage_alias]
        except InvalidStorageError:
            return None

    def get_file_name(self):
        return f"health_check_storage_test/test-{uuid.uuid4()}.txt"

    def get_file_content(self):
        return b"this is the healthtest file content"

    async def _run_check(self):
        storage = self.get_storage()
        if storage is None:
            raise ServiceUnavailable("Storage backend is not configured properly")

        file_name = self.get_file_name()
        file_content = self.get_file_content()

        try:
            # Save the test file
            file_name = await self.check_save(storage, file_name, file_content)

            # Delete the test file
            await self.check_delete(storage, file_name)
        except ServiceUnavailable as e:
            self.add_error(str(e))
            raise
        except Exception as e:
            self.add_error("An unexpected error occurred during storage health check", e)
            raise

    async def check_save(self, storage, file_name, file_content):
        # Save the file
        file_name = await sync_to_async(storage.save, thread_sensitive=False) \
            (file_name, ContentFile(content=file_content))

        # Verify the file exists and content matches
        file_exists = await sync_to_async(storage.exists, thread_sensitive=False)(file_name)
        if not file_exists:
            raise ServiceUnavailable("File does not exist after saving")

        # Open and read the file asynchronously
        file = await sync_to_async(storage.open, thread_sensitive=False)(file_name)
        try:
            file_content_read = await sync_to_async(file.read, thread_sensitive=False)()
            if file_content_read != file_content:
                await sync_to_async(file.close, thread_sensitive=False)()
                raise ServiceUnavailable("File content does not match after saving")
        finally:
            # Ensure the file is properly closed
            await sync_to_async(file.close, thread_sensitive=False)()

        return file_name

    async def check_delete(self, storage, file_name):
        # Delete the file and verify it was removed
        await sync_to_async(storage.delete, thread_sensitive=False)(file_name)
        file_exists = await sync_to_async(storage.exists, thread_sensitive=False)(file_name)
        if file_exists:
            raise ServiceUnavailable("File was not deleted")


class DefaultAsyncFileStorageHealthCheck(AsyncStorageHealthCheck):
    storage_alias = "default"
    storage = default_storage
