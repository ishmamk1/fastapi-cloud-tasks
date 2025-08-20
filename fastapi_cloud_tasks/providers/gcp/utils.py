from google.cloud import tasks_v2
from google.api_core.exceptions import NotFound

def validate_queue(client: tasks_v2.CloudTasksClient, queue_path: str):
    if client == None:
        client = tasks_v2.CloudTasksClient()
    
    try:
        queue = client.get_queue(name=queue_path)
        print(f"Queue exists: ${queue.name}")
    except NotFound:
        print("Queue not found. Creating queue...")

        queue_path_parts = queue_path.split("/")
        project = queue_path_parts[1]
        location = queue_path_parts[3]
        queue_id = queue_path_parts[5]

        parent = f"projects/{project}/locations/{location}"
        queue = {"name" : queue_path}
        
        created_queue = client.create_queue(parent=parent, queue=queue)
        print(f"Queue Created: {created_queue.name}")


