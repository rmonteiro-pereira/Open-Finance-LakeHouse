#!/usr/bin/env python3
"""
Wrapper script to run Kedro pipeline with suppressed Spark command output.
This script temporarily redirects stdout to suppress the "Spark Command" output
that cannot be suppressed through logging configuration.
"""

import subprocess
import sys
import os
import tempfile
import threading

def run_kedro_quiet():
    """Run kedro with suppressed Spark command output."""
    
    # Create a temporary file to capture output
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_filename = temp_file.name
    
    try:
        # Start the kedro process
        process = subprocess.Popen(
            [sys.executable, "-m", "kedro", "run"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Function to read and filter output
        def process_output(pipe, output_func):
            for line in iter(pipe.readline, ''):
                # Skip the "Spark Command" line
                if not line.startswith("Spark Command:"):
                    output_func(line, end='')
        
        # Create threads to handle stdout and stderr
        stdout_thread = threading.Thread(
            target=process_output,
            args=(process.stdout, print)
        )
        stderr_thread = threading.Thread(
            target=process_output,
            args=(process.stderr, lambda x, **kwargs: print(x, file=sys.stderr, **kwargs))
        )
        
        # Start threads
        stdout_thread.start()
        stderr_thread.start()
        
        # Wait for process to complete
        return_code = process.wait()
        
        # Wait for threads to finish
        stdout_thread.join()
        stderr_thread.join()
        
        return return_code
        
    except KeyboardInterrupt:
        print("\nKedro pipeline interrupted by user")
        process.terminate()
        return 1
    except Exception as e:
        print(f"Error running Kedro pipeline: {e}")
        return 1
    finally:
        # Clean up temp file
        try:
            os.unlink(temp_filename)
        except Exception:
            pass

if __name__ == "__main__":
    sys.exit(run_kedro_quiet())
