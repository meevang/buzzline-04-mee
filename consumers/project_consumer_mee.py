"""
project_consumer_mee.py

Read a JSON-formatted file as it is being written. 

Example JSON message:
{"message": "I just saw a movie! It was amazing.", "author": "Eve"}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import json
import os
import sys
import time
import pathlib
from collections import defaultdict
import matplotlib.pyplot as plt
import networkx as nx
from utils.utils_logger import logger

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

# Initialize a graph to represent author interactions
author_graph = nx.Graph()

# Set up live visuals
fig, ax = plt.subplots(figsize=(9, 6))
plt.ion()

def update_chart():
    """Update the live network graph with the latest author interactions."""
    ax.clear()
    
    # Draw the network graph
    pos = nx.spring_layout(author_graph)
    nx.draw(author_graph, pos, ax=ax, with_labels=True, node_color='lightblue', 
            node_size=1500, font_size=10, font_weight='bold')
    
    # Add edge labels (interaction counts)
    edge_labels = nx.get_edge_attributes(author_graph, 'weight')
    nx.draw_networkx_edge_labels(author_graph, pos, edge_labels=edge_labels)

    ax.set_title("Author Interaction Network")
    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

def process_message(message: str) -> None:
    try:
        message_dict: dict = json.loads(message)
        
        if isinstance(message_dict, dict):
            author = message_dict.get("author", "unknown")
            category = message_dict.get("category", "unknown")
            
            # Add or update the edge in the graph
            if author_graph.has_edge(author, category):
                author_graph[author][category]['weight'] += 1
            else:
                author_graph.add_edge(author, category, weight=1)
            
            logger.info(f"{author} posted in category: {category}")
            update_chart()
            
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


def main() -> None:
    logger.info("START consumer.")

    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        with open(DATA_FILE, "r") as file:
            file.seek(0, os.SEEK_END)
            print("Consumer is ready and waiting for new JSON messages...")

            while True:
                where = file.tell()
                line = file.readline()
                if not line:
                    file.seek(where)
                    time.sleep(0.1)  # Sleep for a shorter time
                else:
                    process_message(line.strip())

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()
