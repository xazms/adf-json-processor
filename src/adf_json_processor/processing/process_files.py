import json
import adf_json_processor
from adf_json_processor.file_handling.file_handler import FileHandler

def build_hierarchical_structure(adf_json, include_types=None, include_empty=False):
    """
    Builds a hierarchical structure of an ADF pipeline, including nodes and links.

    Args:
        adf_json (dict): The JSON representation of the ADF pipeline.
        include_types (list, optional): A list of types to include for both activities and dependencies.
        include_empty (bool, optional): Include pipelines with no activities.

    Returns:
        dict: A dictionary containing the pipeline structure, nodes, and links.
    """
    nodes = []
    links = []
    node_id = 1

    pipeline_name = adf_json.get("name")
    if not pipeline_name:
        raise ValueError("Pipeline JSON is missing the 'name' field.")

    root_node = {
        "name": pipeline_name,
        "type": "Pipeline",
        "level": 1,
        "activities": []
    }
    nodes.append({
        "id": str(node_id),
        "name": pipeline_name,
        "type": "Pipeline",
        "level": 1
    })
    node_id += 1

    activities = adf_json.get("properties", {}).get("activities", [])

    if not activities and not include_empty:
        return None

    for activity in activities:
        activity_name = activity.get("name")
        activity_type = activity.get("type")

        if not activity_name or not activity_type:
            continue

        if include_types and activity_type not in include_types:
            continue

        level = 2

        activity_node = {
            "name": activity_name,
            "type": activity_type,
            "level": level,
            "dependencies": []
        }

        # Handle dependencies
        for dependency in activity.get("dependsOn", []):
            dependent_activity = dependency.get("activity")
            if not dependent_activity:
                continue

            dependent_activity_node = next(
                (node for node in nodes if node["name"] == dependent_activity),
                None
            )

            if dependent_activity_node:
                dependency_type = dependent_activity_node["type"]
                dependency_level = dependent_activity_node["level"]

                if include_types is None or dependency_type in include_types:
                    links.append({
                        "source": dependent_activity,
                        "target": activity_name
                    })
                    activity_node["dependencies"].append({
                        "name": dependent_activity,
                        "type": dependency_type,
                        "level": max(dependency_level, level)
                    })
            else:
                future_activity = next(
                    (act for act in activities if act["name"] == dependent_activity),
                    None
                )
                dependency_type = future_activity["type"] if future_activity else "Unknown"

                if include_types is None or dependency_type in include_types:
                    activity_node["dependencies"].append({
                        "name": dependent_activity,
                        "type": dependency_type,
                        "level": level
                    })

        # Handle ExecutePipeline reference
        if activity_type == "ExecutePipeline":
            referenced_pipeline = activity["typeProperties"]["pipeline"]["referenceName"]
            activity_node["pipelineReference"] = referenced_pipeline

            # Check if the referenced pipeline is already in nodes
            referenced_node = next(
                (node for node in nodes if node["name"] == referenced_pipeline and node["type"] == "Pipeline"),
                None
            )
            if not referenced_node:
                referenced_node = {
                    "id": str(node_id),
                    "name": referenced_pipeline,
                    "type": "Pipeline",
                    "level": level + 1
                }
                nodes.append(referenced_node)
                node_id += 1
            
            links.append({
                "source": activity_name,
                "target": referenced_pipeline
            })

        root_node["activities"].append(activity_node)
        nodes.append({
            "id": str(node_id),
            "name": activity_name,
            "type": activity_type,
            "level": level
        })
        node_id += 1

    if not root_node["activities"] and not include_empty:
        return None

    return {"pipeline": root_node, "nodes": nodes, "links": links}

def process_multiple_json_files(file_handler, include_types=None, include_empty=False, include_parts=None):
    """
    Process multiple JSON files and build a combined hierarchical structure.

    Args:
        file_handler (FileHandler): The file handler object for managing file retrieval.
        include_types (list, optional): A list of types to include for both activities and dependencies.
        include_empty (bool, optional): Include pipelines with no activities.
        include_parts (list, optional): Parts of the structure to include ('pipelines', 'nodes', 'links').

    Returns:
        dict: Combined structure containing all pipelines, nodes, and links.
    """
    combined_structure = {"pipelines": [], "nodes": [], "links": []}
    error_log = []

    filtered_files = file_handler.get_filtered_file_list()

    for file in filtered_files:
        try:
            file_content = file_handler.get_adf_file_content(file['path'])
            adf_data = json.loads(file_content)
            hierarchical_structure = build_hierarchical_structure(adf_data, include_types, include_empty)
            if hierarchical_structure:
                if include_parts is None or "pipelines" in include_parts:
                    combined_structure["pipelines"].append(hierarchical_structure["pipeline"])
                if include_parts is None or "nodes" in include_parts:
                    combined_structure["nodes"].extend(hierarchical_structure["nodes"])
                if include_parts is None or "links" in include_parts:
                    combined_structure["links"].extend(hierarchical_structure["links"])
        except json.JSONDecodeError as e:
            error_message = f"JSONDecodeError in file {file['path']}: {e}"
            error_log.append({"file": file['path'], "error": "JSONDecodeError", "details": str(e)})
        except KeyError as e:
            error_message = f"KeyError in file {file['path']}: Missing key {e}"
            error_log.append({"file": file['path'], "error": "KeyError", "details": str(e)})
        except Exception as e:
            error_message = f"Unexpected error in file {file['path']}: {e}"
            error_log.append({"file": file['path'], "error": "UnexpectedError", "details": str(e)})

    file_handler.log_errors(error_log)

    return combined_structure

def process_json_files(file_handler, include_parts=None, include_types=None, include_empty=False, source_filename=None):
    """
    Process JSON files and save the combined structure.

    Args:
        file_handler (FileHandler): The file handler object.
        include_parts (list, optional): Parts of the structure to include ('pipelines', 'nodes', 'links').
        include_types (list, optional): A list of types to include for both activities and dependencies.
        include_empty (bool, optional): Include empty pipelines.
        source_filename (str, optional): If provided, overrides the default source filename.

    Returns:
        None
    """
    # Update the source filename if provided
    if source_filename:
        file_handler.update_source_filename(source_filename)

    # Process JSON files and build the combined structure
    combined_structure = process_multiple_json_files(
        file_handler=file_handler,
        include_types=include_types, 
        include_empty=include_empty, 
        include_parts=include_parts
    )

    # Print the combined structure for debugging purposes
    print("\n=== Combined Hierarchical Pipeline Structure ===")
    print(json.dumps(combined_structure, indent=4))

    # Save the combined structure to a JSON file in DBFS
    with open(file_handler.output_file_path, "w") as outfile:
        json.dump(combined_structure, outfile, indent=4)
    
    # Print confirmation of the save location
    print(f"Combined hierarchical pipeline structure saved to {file_handler.output_file_path}.")