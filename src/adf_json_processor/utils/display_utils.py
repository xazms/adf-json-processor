import json

def display_full_json(combined_structure):
    """
    Display the full hierarchical JSON structure in a human-readable format.
    
    Args:
        combined_structure (dict): The hierarchical JSON structure to display.
    
    Example:
        display_full_json(combined_structure)
    """
    print("\n=== Combined Hierarchical Pipeline Structure ===")
    print(json.dumps(combined_structure, indent=4))