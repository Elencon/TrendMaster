import pytest
import anyio
import msgspec
from datetime import datetime
from src.api.data_processor import APIDataProcessor, ProcessingStats
from src.api.api_models import APIResponse

# Sample struct for testing
class User(msgspec.Struct):
    id: int
    name: str

@pytest.mark.anyio
async def test_data_processor_success():
    processor = APIDataProcessor(max_concurrent=10)
    
    responses = [
        APIResponse(
            status=200,
            data={"id": i, "name": f"User {i}"},
            headers={},
            url=f"https://api.test.com/users/{i}",
            request_time=0.1,
            response_time=datetime.now()
        ) for i in range(5)
    ]
    
    results = await processor.process_responses(responses, User)
    
    assert len(results) == 5
    assert all(isinstance(r, User) for r in results)
    assert processor.stats.successful_responses == 5
    assert processor.stats.processed_items == 5

@pytest.mark.anyio
async def test_data_processor_partial_failure():
    processor = APIDataProcessor(max_concurrent=10)
    
    responses = [
        APIResponse(
            status=200,
            data={"id": 1, "name": "User 1"},
            headers={},
            url="https://api.test.com/users/1",
            request_time=0.1,
            response_time=datetime.now()
        ),
        APIResponse(
            status=500, # Should be skipped
            data=None,
            headers={},
            url="https://api.test.com/users/2",
            request_time=0.1,
            response_time=datetime.now()
        ),
        APIResponse(
            status=200, # Invalid data structure
            data={"invalid": "data"},
            headers={},
            url="https://api.test.com/users/3",
            request_time=0.1,
            response_time=datetime.now()
        )
    ]
    
    results = await processor.process_responses(responses, User)
    
    assert len(results) == 1
    assert processor.stats.successful_responses == 1
    assert processor.stats.failed_responses == 2

@pytest.mark.anyio
async def test_data_processor_context_manager():
    async with APIDataProcessor().processing_context("Test Job") as processor:
        responses = [
            APIResponse(
                status=200,
                data={"id": 1, "name": "User 1"},
                headers={},
                url="url",
                request_time=0.1,
                response_time=datetime.now()
            )
        ]
        await processor.process_responses(responses, User)
    
    assert processor.stats.processing_time > 0
