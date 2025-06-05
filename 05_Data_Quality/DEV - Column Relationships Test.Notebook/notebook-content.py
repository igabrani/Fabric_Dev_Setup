# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe",
# META       "default_lakehouse_name": "Bronze_Lakehouse_DEV",
# META       "default_lakehouse_workspace_id": "4748fbe5-9b18-4aac-9d74-f79c39ff81db",
# META       "known_lakehouses": [
# META         {
# META           "id": "b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd
import networkx as nx
import dash
from dash import dcc, html
import plotly.graph_objects as go
from dash.dependencies import Input, Output

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_lineage_graph(df, selected_column):
    G = nx.DiGraph()
    node_tooltips = {}
    
    # Add edges to the graph
    for _, row in df.iterrows():
        source_node = f"{row['destination']}"
        dest_node = f"{row['source']}"
        G.add_edge(source_node, dest_node)
    
        # Store tooltips for source and destination
        if source_node not in node_tooltips:
            node_tooltips[source_node] = []
        if dest_node not in node_tooltips:
            node_tooltips[dest_node] = []
        
        node_tooltips[dest_node].append(f"Left: {row['destination']}")
        node_tooltips[source_node].append(f"Right: {row['source']}")
        

    # Determine node depth (distance from source tables)
    node_depth = {}
    for node in nx.topological_sort(G):
        predecessors = list(G.predecessors(node))
        node_depth[node] = 0 if not predecessors else max(node_depth[p] for p in predecessors) + 1
    
    # Assign unique y-coordinates for nodes at the same depth to avoid overlap
    depth_counts = {}
    pos = {}
    max_depth = max(node_depth.values()) if node_depth else 0
    for node, depth in node_depth.items():
        if depth not in depth_counts:
            depth_counts[depth] = 0
        pos[node] = (depth * (2000 / (max_depth + 1)), depth_counts[depth] * -1.5)
        depth_counts[depth] += 1
    
    # Create Plotly edges
    edge_x, edge_y, edge_trace_data = [], [], []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
        edge_trace_data.append((edge[0], edge[1]))
    
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=1.5, color='lightblue'),
        hoverinfo='none',
        mode='lines',
        name='Edges')
    
    # Create Plotly nodes
    node_x, node_y, node_text, node_hovertext, node_trace_data, node_colors = [], [], [], [], {}, []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_text.append(node.replace(".", "\n"))  # Improve readability
        # Join all relationships for tooltips
        relationships = "<br>".join(node_tooltips.get(node, []))
        node_hovertext.append(relationships)
        
    
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        marker=dict(size=12, color='black', line=dict(width=1, color='black')),
        text=node_text,
        textposition='middle left' if max_depth > 3 else 'middle right',
        hoverinfo='text',
        hovertext=node_hovertext,
        name='Nodes',
        textfont=dict(size=14, family='Arial', color='black'))
    
    # JavaScript for node highlighting
    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title='Full Column Lineage (Left to Right)',
                        hovermode='closest',
                        margin=dict(b=20, l=20, r=100, t=50),  # Increase right margin
                        width=1850,  # Increase canvas width further
                        height=1000,  # Increase canvas height
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
    
    fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example DataFrame
data = {
    'Source Table': ['A', 'A', 'B', 'C'],
    'Source Column': ['col1', 'col2', 'col3', 'col4'],
    'Destination Table': ['B', 'C', 'D', 'D'],
    'Destination Column': ['col3', 'col4', 'col5', 'col5']
}
df_test = pd.DataFrame(data)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = pd.read_csv("/lakehouse/default/Files/ohds/mapping/Temp/column_relationship_test.csv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select a column to visualize lineage
selected_col = 'silver.sunlife_fi02.Total_Amount'
create_lineage_graph(df, selected_col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
