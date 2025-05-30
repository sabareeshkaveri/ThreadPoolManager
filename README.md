# ThreadPoolManager

A Python class for managing parallel task execution using a thread pool, with support for task queuing, status updates, result tracking, and a completion callback.

## Author
- **Name**: Sabareesh Kaveri
- **Email**: sabareeshkaveri@gmail.com
- **Date**: May 29, 2025

## Features
- **Parallel Task Execution**: Uses `ThreadPoolExecutor` to run tasks concurrently with a configurable number of worker threads.
- **Task Queue**: Manages tasks in a thread-safe queue for controlled execution.
- **Status Updates**: Integrated `status_callback` method to log task progress (start, completion, or failure) to console or a file in `~/ThreadPoolLog/YYYY-MM-DD_HH-MM-SS.log`.
- **Completion Callback**: Optional `on_complete` callback runs automatically when all tasks are finished, receiving results and task counts.
- **Result Tracking**: Stores task results as `(task_id, result)` tuples, accessible via `get_results()`.
- **Task Counts**: Tracks total and completed tasks, accessible via `get_task_counts()`.
- **Thread Safety**: Uses locks to ensure safe access to shared state.
- **Non-blocking**: Processes tasks using `threading.Timer` every 0.1s.

## Installation
Requires Python standard library modules: `threading`, `queue`, `concurrent.futures`, `time`, `os`, `datetime`, `pathlib`.

## Usage
1. **Initialize**:
   ```python
   from threadpool_manager import ThreadPoolManager

   # Console output, with completion callback
   def on_complete(results, total, completed):
       print(f"Done: {results}, Total: {total}, Completed: {completed}")

   pool = ThreadPoolManager(max_workers=3, on_complete=on_complete)
   ```
   or
   ```python
   # Log to ~/ThreadPoolLog/YYYY-MM-DD_HH-MM-SS.log
   pool = ThreadPoolManager(max_workers=3, log_to_file=True, on_complete=on_complete)
   ```

2. **Submit Tasks**:
   ```python
   def my_task(task_id, sleep_time):
       time.sleep(sleep_time)
       return f"Task {task_id} output"

   pool.submit_task(my_task, 1, 2)  # Task ID 1, sleep 2s
   pool.submit_task(my_task, 2, 1)  # Task ID 2, sleep 1s
   ```

3. **Start Processing**:
   ```python
   pool.process_queue()  # Starts processing, calls status_callback and on_complete
   ```

4. **Retrieve Results and Counts**:
   ```python
   results = pool.get_results()  # List of (task_id, result) tuples
   total, completed = pool.get_task_counts()  # Total and completed task counts
   print(f"Results: {results}")
   print(f"Task Counts: {total}, {completed}")
   ```

5. **Shutdown**:
   ```python
   pool.shutdown()  # Waits for tasks to complete and shuts down
   ```

6. **Customize (Optional)**:
   Subclass to override `status_callback` for custom logging:
   ```python
   class MyThreadPoolManager(ThreadPoolManager):
       def status_callback(self, message):
           print(f"CUSTOM: {message}")
   ```

## Example
```python
import time
from threadpool_manager import ThreadPoolManager

def example_task(task_id, sleep_time):
    print(f"Task {task_id} started, sleeping for {sleep_time}s")
    time.sleep(sleep_time)
    result = f"Task {task_id} output"
    print(f"Task {task_id} completed with result: {result}")
    return result

def on_complete_callback(results, total_tasks, completed_tasks):
    print(f"Completion callback: {results}, {total_tasks} submitted, {completed_tasks} completed")

# Initialize with file logging and completion callback
pool = ThreadPoolManager(max_workers=3, log_to_file=True, on_complete=on_complete_callback)

# Submit tasks
pool.submit_task(example_task, 1, 2)
pool.submit_task(example_task, 2, 1)
pool.submit_task(example_task, 3, 3)

# Start processing
pool.process_queue()

# Wait for completion
time.sleep(5)

# Get results and counts
print(f"Results: {pool.get_results()}")
print(f"Task Counts: {pool.get_task_counts()}")

# Shutdown
pool.shutdown()
```

## Output
Console output (if `log_to_file=False`):
```
[21:16:01] Task 1 started (Total: 3, Completed: 0)
Task 1 started, sleeping for 2s
[21:16:01] Task 2 started (Total: 3, Completed: 0)
Task 2 started, sleeping for 1s
Task 2 completed with result: Task 2 output
[21:16:02] Task 2 completed with result: Task 2 output (Total: 3, Completed: 1)
[21:16:02] Task 3 started (Total: 3, Completed: 1)
Task 3 started, sleeping for 3s
Task 1 completed with result: Task 1 output
[21:16:03] Task 1 completed with result: Task 1 output (Total: 3, Completed: 2)
Task 3 completed with result: Task 3 output
[21:16:04] Task 3 completed with result: Task 3 output (Total: 3, Completed: 3)
[21:16:04] All tasks completed, calling on_complete method
Completion callback: [(2, 'Task 2 output'), (1, 'Task 1 output'), (3, 'Task 3 output')], 3 submitted, 3 completed
Results: [(2, 'Task 2 output'), (1, 'Task 1 output'), (3, 'Task 3 output')]
Task Counts: (3, 3)
```

File output (`~/ThreadPoolLog/2025-05-29_21-16-00.log`, if `log_to_file=True`):
```
[21:16:01] Task 1 started (Total: 3, Completed: 0)
[21:16:01] Task 2 started (Total: 3, Completed: 0)
[21:16:02] Task 2 completed with result: Task 2 output (Total: 3, Completed: 1)
[21:16:02] Task 3 started (Total: 3, Completed: 1)
[21:16:03] Task 1 completed with result: Task 1 output (Total: 3, Completed: 2)
[21:16:04] Task 3 completed with result: Task 3 output (Total: 3, Completed: 3)
[21:16:04] All tasks completed, calling on_complete method
```

## Notes
- **Thread Safety**: Uses locks for shared state and thread-safe file writes.
- **Non-blocking**: Processes tasks using `threading.Timer` every 0.1s.
- **Completion Callback**: `on_complete` runs automatically when all tasks finish, receiving results and task counts.
- **Logging**: If `log_to_file=True`, logs to `~/ThreadPoolLog/YYYY-MM-DD_HH-MM-SS.log`, creating the directory if needed.
- **Customization**: Override `status_callback` via subclassing for custom logging or UI updates.
- **Error Handling**: Captures task errors in results and status messages.