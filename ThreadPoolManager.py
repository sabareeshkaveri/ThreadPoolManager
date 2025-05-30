# ThreadPoolManager
# Author: Kaveri Sabareesh (sabareeshkaveri@gmail.com)
# Created: May 29, 2025
# Description: A thread pool manager for parallel task execution with status updates and completion callback.

import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import time
import os
from datetime import datetime

class ThreadPoolManager:
    def __init__(self, max_workers=4, log_to_file=False, on_complete=None):
        """
        Initialize the ThreadPoolManager with a specified number of worker threads.
        
        Args:
            max_workers (int): Maximum number of threads in the pool (default: 4)
            log_to_file (bool, optional): If True, status messages are logged to ~/ThreadPoolLog/YYYY-MM-DD_HH-MM-SS.log
                                         If False, print to console (default: False)
            on_complete (callable, optional): Method to call when all tasks are completed
        """
        self.task_queue = Queue()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.lock = threading.Lock()
        self._shutdown = False
        self.total_tasks = 0
        self.completed_tasks = 0
        self.results = []
        self.on_complete = on_complete
        # Set up logging path
        self.log_to_file = None
        if log_to_file:
            log_dir = os.path.expanduser("~/ThreadPoolLog")
            os.makedirs(log_dir, exist_ok=True)  # Create directory if it doesn't exist
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            self.log_to_file = os.path.join(log_dir, f"log_{timestamp}.log")

    def submit_task(self, task, *args, **kwargs):
        """
        Submit a task to the queue for execution.
        
        Args:
            task: The function to execute
            *args: Positional arguments for the task
            **kwargs: Keyword arguments for the task
        """
        with self.lock:
            if not self._shutdown:
                self.task_queue.put((task, args, kwargs))
                self.total_tasks += 1
            else:
                raise RuntimeError("Cannot submit tasks after shutdown")

    def process_queue(self):
        """
        Process tasks in the queue without blocking.
        Calls the internal status_callback for status updates and on_complete when done.
        """
        try:
            while not self.task_queue.empty():
                task_info = self.task_queue.get_nowait()
                task, args, kwargs = task_info
                try:
                    future = self.executor.submit(task, *args, **kwargs)
                    future.add_done_callback(
                        lambda f: self._task_done(f, args[0] if args else None)
                    )
                    self.status_callback(f"Task {args[0] if args else 'unknown'} started "
                                        f"(Total: {self.total_tasks}, Completed: {self.completed_tasks})")
                except Exception as e:
                    self.status_callback(f"Error executing task: {e} "
                                        f"(Total: {self.total_tasks}, Completed: {self.completed_tasks})")
                    self.task_queue.task_done()
        except Queue.Empty:
            pass

        with self.lock:
            if self.total_tasks > 0 and self.completed_tasks == self.total_tasks and self.task_queue.empty():
                if self.on_complete:
                    self.status_callback("All tasks completed, calling on_complete method")
                    try:
                        self.on_complete(self.results, self.total_tasks, self.completed_tasks)
                    except Exception as e:
                        self.status_callback(f"Error in on_complete: {e}")
                self._shutdown = True
            elif not self._shutdown:
                threading.Timer(0.1, self.process_queue).start()

    def _task_done(self, future, task_id):
        """
        Handle task completion, store result, and update counters.
        
        Args:
            future: The completed future object
            task_id: Identifier for the task
        """
        with self.lock:
            self.completed_tasks += 1
            try:
                result = future.result()
                self.results.append((task_id, result))
                self.status_callback(f"Task {task_id} completed with result: {result} "
                                    f"(Total: {self.total_tasks}, Completed: {self.completed_tasks})")
            except Exception as e:
                self.results.append((task_id, f"Error: {e}"))
                self.status_callback(f"Task {task_id} failed: {e} "
                                    f"(Total: {self.total_tasks}, Completed: {self.completed_tasks})")
            finally:
                self.task_queue.task_done()

    def status_callback(self, message):
        """
        Handle status updates for tasks (print to console or log to file).
        
        Args:
            message (str): Status message (e.g., task started, completed, failed)
        """
        formatted_message = f"[{time.strftime('%H:%M:%S')}] {message}"
        if self.log_to_file:
            with open(self.log_to_file, "a") as f:
                f.write(f"{formatted_message}\n")
        else:
            print(formatted_message)

    def shutdown(self, wait=True):
        """
        Shutdown the thread pool and wait for all tasks to complete.
        
        Args:
            wait (bool): Whether to wait for all tasks to complete (default: True)
        """
        with self.lock:
            self._shutdown = True
        if wait:
            self.executor.shutdown(wait=True)

    def get_results(self):
        """
        Return the list of task results.
        
        Returns:
            list: List of tuples (task_id, result)
        """
        with self.lock:
            return self.results

    def get_task_counts(self):
        """
        Return total and completed task counts.
        
        Returns:
            tuple: (total_tasks, completed_tasks)
        """
        with self.lock:
            return self.total_tasks, self.completed_tasks
        
# Example usage with console and file-based status_callback, plus on_complete
def example_task(task_id, sleep_time):
    print(f"Task {task_id} started, sleeping for {sleep_time}s")
    time.sleep(sleep_time)
    result = f"Task {task_id} output"
    print(f"Task {task_id} completed with result: {result}")
    return result

def on_complete_callback(results, total_tasks, completed_tasks):
    """Example callback to run after all tasks complete."""
    print(f"Completion callback triggered!")
    print(f"Final Results: {results}")
    print(f"Task Summary: {total_tasks} submitted, {completed_tasks} completed")

if __name__ == "__main__":
    # Using ThreadPoolManager with console output and on_complete
    print("Using ThreadPoolManager (console output):")
    pool = ThreadPoolManager(max_workers=3, on_complete=on_complete_callback)
    pool.submit_task(example_task, 1, 2)
    pool.submit_task(example_task, 2, 1)
    pool.submit_task(example_task, 3, 3)
    pool.submit_task(example_task, 4, 3)
    pool.submit_task(example_task, 5, 3)
    pool.submit_task(example_task, 6, 3)
    pool.process_queue()
    time.sleep(5)  # Allow tasks to complete
    total, completed = pool.get_task_counts()
    print(f"Task Counts - Total: {total}, Completed: {completed}")
    print(f"Results: {pool.get_results()}")
    pool.shutdown()
    print("Thread pool shutdown complete")

    # Using ThreadPoolManager with file logging and on_complete
    print("\nUsing ThreadPoolManager (file logging):")
    file_pool = ThreadPoolManager(max_workers=2, log_to_file="tasks.log", on_complete=on_complete_callback)
    file_pool.submit_task(example_task, 4, 1)
    file_pool.submit_task(example_task, 5, 2)
    file_pool.process_queue()
    time.sleep(3)  # Allow tasks to complete
    total, completed = file_pool.get_task_counts()
    print(f"Task Counts - Total: {total}, Completed: {completed}")
    print(f"Results: {file_pool.get_results()}")
    file_pool.shutdown()
    print("File-based thread pool shutdown complete")