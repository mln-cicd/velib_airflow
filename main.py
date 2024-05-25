import logging
import subprocess
import os
import time
import importlib.util
from prefect import flow
from prefect.client import get_client
from app.flows.app_startup import startup_application_flow

logger = logging.getLogger(__name__)

FLOWS_DIR = './app/flows'

def import_flow_from_file(filepath):
    module_name = os.path.splitext(os.path.basename(filepath))[0]
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Assuming the flow function name matches the filename (without .py)
    flow_name = f"{module_name}_flow"
    return getattr(module, flow_name)

def register_flows():
    client = get_client()
    
    # Dynamically import and register flows from the flows directory
    for filename in os.listdir(FLOWS_DIR):
        if filename.endswith(".py") and filename != "__init__.py":
            filepath = os.path.join(FLOWS_DIR, filename)
            try:
                flow_func = import_flow_from_file(filepath)
                flow_func()
                logger.info(f"Registered flow: {filename}")
            except Exception as e:
                logger.error(f"Failed to register flow {filename}: {e}")

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    logger.info("Starting the Prefect server...")
    # Start the Prefect server in a subprocess
    subprocess.Popen(["prefect", "server", "start", "--host", "0.0.0.0", "--port", "4200"])
    time.sleep(10)  # Give the server some time to start

    logger.info("Running the startup flow...")
    # Run the startup flow
    startup_application_flow()
    
    logger.info("Startup flow completed. Registering additional flows...")
    # Register additional flows
    register_flows()

    logger.info("Startup sequence completed. Prefect server is running.")
    
    # Keep the script running
    while True:
        time.sleep(3600)  # Sleep for 1 hour (adjust as needed)
