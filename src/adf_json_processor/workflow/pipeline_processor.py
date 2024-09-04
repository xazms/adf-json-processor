import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import functions as F
from adf_json_processor.transformations.dataframe_utils import add_relationship_id  # Import the function instead of defining it here

def build_hierarchical_structure(adf_json, include_types=None, include_empty=False):
    """
    Build a hierarchical structure from ADF pipeline JSON.
    Returns nodes and links as dictionaries for further processing.
    """
    nodes = []
    links = []
    node_id = 1

    pipeline_name = adf_json.get("name")
    if not pipeline_name:
        raise ValueError("Pipeline JSON is missing the 'name' field.")

    # Root node for the pipeline
    root_node = {"id": str(node_id), "name": pipeline_name, "type": "Pipeline", "level": 1}
    nodes.append(root_node)
    node_id += 1

    # Process activities and build nodes and links
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

        # Create activity node
        activity_node = {
            "id": str(node_id),
            "name": activity_name,
            "type": activity_type,
            "level": 2,
            "pipelineReference": None
        }
        node_id += 1

        # Add dependencies (links)
        for dependency in activity.get("dependsOn", []):
            dependent_activity = dependency.get("activity")
            if dependent_activity:
                links.append({"source": dependent_activity, "target": activity_name, "type": "dependency"})

        # Handle ExecutePipeline reference
        if activity_type == "ExecutePipeline":
            referenced_pipeline = activity["typeProperties"]["pipeline"]["referenceName"]
            activity_node["pipelineReference"] = referenced_pipeline
            links.append({"source": activity_name, "target": referenced_pipeline, "type": "pipeline_reference"})

            # Add referenced pipeline if not already present
            if referenced_pipeline not in [node["name"] for node in nodes]:
                referenced_node = {"id": str(node_id), "name": referenced_pipeline, "type": "Pipeline", "level": 3}
                nodes.append(referenced_node)
                node_id += 1

        nodes.append(activity_node)

    return {"nodes": nodes, "links": links}

def process_multiple_json_files(file_handler, include_types=None, include_empty=False):
    """
    Process multiple JSON files retrieved from the Azure DevOps repository.
    Combines the structures of all JSON files into a single nodes and links structure.

    Args:
        file_handler (FileHandler): FileHandler instance to interact with the Azure DevOps repository.
        include_types (list): Optional list of types to include from the JSON (e.g., 'ExecutePipeline', 'Copy').
        include_empty (bool): Whether to include pipelines with no activities (default is False).

    Returns:
        dict: A combined structure with 'nodes' and 'links' from all processed files.
    """
    combined_nodes = []
    combined_links = []

    # Get the list of filtered files
    filtered_files = file_handler.get_filtered_file_list()

    if not filtered_files:
        print("No JSON files found.")
        return None

    # Loop over each file and process it
    for file_info in filtered_files:
        file_path = file_info['path']
        print(f"Processing file: {file_path}")
        
        # Retrieve the content of the JSON file
        adf_json_content = file_handler.get_adf_file_content(file_path)
        
        # Parse the content as JSON
        adf_json = json.loads(adf_json_content)
        
        # Build the hierarchical structure for each file
        structure = build_hierarchical_structure(adf_json, include_types=include_types, include_empty=include_empty)
        
        if structure:
            combined_nodes.extend(structure['nodes'])
            combined_links.extend(structure['links'])
    
    # Return the combined structure with nodes and links
    return {"nodes": combined_nodes, "links": combined_links}

def convert_to_dataframe(spark: SparkSession, combined_structure: dict):
    """
    Convert hierarchical JSON structure to Spark DataFrames.
    Ensures correct data types for 'id', 'level', and adds 'DWCreatedDate' timestamp.
    """
    # Convert nodes and links to DataFrames
    nodes_df = spark.createDataFrame(combined_structure['nodes']).withColumn("DWCreatedDate", current_timestamp())
    links_df = spark.createDataFrame(combined_structure['links']).withColumn("DWCreatedDate", current_timestamp())

    # Cast 'id' and 'level' in nodes_df to int
    nodes_df = nodes_df.withColumn("id", col("id").cast("int")).withColumn("level", col("level").cast("int"))

    # Display the DataFrames
    print("\n=== Nodes DataFrame ===")
    nodes_df.show(truncate=False)

    print("\n=== Links DataFrame ===")
    links_df.show(truncate=False)

    return nodes_df, links_df