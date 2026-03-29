import asyncio
import sys
from pathlib import Path

# Add src to sys.path
src_path = str(Path(r'c:\Economy\Invest\TrendMaster\src').resolve())
if src_path not in sys.path:
    sys.path.insert(0, src_path)

print(f"DEBUG: sys.path = {sys.path}")

async def run_test():
    try:
        from api.api_client import AsyncAPIClient
        from api.api_models import APIRequest, RequestMethod
        from common.exceptions import APIResponseError
        import httpx

        print("Starting test_api_client_error simulation...")

        def handler(request):
            return httpx.Response(404, text="Not Found")

        transport = httpx.MockTransport(handler)

        async with AsyncAPIClient(base_url="https://api.test.com") as client:
            client._client = httpx.AsyncClient(transport=transport, base_url="https://api.test.com")

            request = APIRequest(url="/missing", method=RequestMethod.GET)

            try:
                await client.request(request)
            except APIResponseError as e:
                print(f"Caught expected APIResponseError: {e}")
                stats = await client.get_stats()
                print(f"Stats after failure: {stats}")

                # Verify assertions from the test
                # assert stats["failed_requests"] == 1
                # assert stats["total_requests"] == 3

                if stats["failed_requests"] != 1:
                    print(f"ASSERTION FAILED: stats['failed_requests'] is {stats['failed_requests']}, expected 1")
                else:
                    print("ASSERTION PASSED: stats['failed_requests'] is 1")

                if stats["total_requests"] != 3:
                     print(f"ASSERTION FAILED: stats['total_requests'] is {stats['total_requests']}, expected 3")
                else:
                    print("ASSERTION PASSED: stats['total_requests'] is 3")

            except Exception as e:
                print(f"Caught UNEXPECTED exception: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()

    except Exception as e:
        print(f"Setup failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_test())
