
import asyncio
import unittest
from unittest.mock import MagicMock
from framework.runtime.execution_stream import ExecutionStream, EntryPointSpec

class TestExecutionStreamConcurrency(unittest.IsolatedAsyncioTestCase):
    async def test_execution_stream_stop_concurrency(self):
        """
        Test that stopping an ExecutionStream does not raise RuntimeError
        when executions clean themselves up concurrently.
        """
        # Setup mocks
        mock_entry = EntryPointSpec(
            id="test-entry", 
            name="Test", 
            entry_node="start", 
            trigger_type="manual"
        )
        mock_storage = MagicMock()
        mock_aggregator = MagicMock()
        mock_state_manager = MagicMock()
        
        stream = ExecutionStream(
            stream_id="test-stream",
            entry_spec=mock_entry,
            graph=MagicMock(),
            goal=MagicMock(),
            state_manager=mock_state_manager,
            storage=mock_storage,
            outcome_aggregator=mock_aggregator,
        )
        
        # Start stream
        await stream.start()
        
        # Manually populate execution tasks with tasks that modify the dict on completion
        async def fast_completing_task(exec_id):
            try:
                await asyncio.sleep(0.01) # Small sleep to ensure we are in the loop
            except asyncio.CancelledError:
                pass
            finally:
                # Simulate cleanup that happens in _run_execution
                if exec_id in stream._execution_tasks:
                    del stream._execution_tasks[exec_id]

        # Create several tasks
        for i in range(10):
            exec_id = f"exec_{i}"
            task = asyncio.create_task(fast_completing_task(exec_id))
            stream._execution_tasks[exec_id] = task
            
        # Now stop the stream
        # This should NOT raise RuntimeError: dictionary changed size during iteration
        try:
            await stream.stop()
        except RuntimeError as e:
            self.fail(f"ExecutionStream.stop() raised RuntimeError: {e}")
        except Exception as e:
            self.fail(f"ExecutionStream.stop() raised unexpected exception: {e}")

        # Verify all tasks are gone
        self.assertEqual(len(stream._execution_tasks), 0)

if __name__ == "__main__":
    unittest.main()
