"""Political Money Flow Graph - Creates the named graph definition.

This asset creates the named graph that ties all vertex and edge collections
together, enabling powerful graph traversal queries.

Named Graph: political_money_flow
Vertex Collections: donors, employers, committees, candidates
Edge Collections: contributed_to, transferred_to, affiliated_with, employed_by, spent_on
"""

from typing import Dict, Any

from dagster import asset, AssetExecutionContext, MetadataValue, Output

from src.resources.arango import ArangoDBResource


@asset(
    name="political_money_graph",
    description="Creates the named graph for political money flow traversal.",
    group_name="graph",
    compute_kind="graph_definition",
    deps=["donors", "employers", "contributed_to", "transferred_to", "affiliated_with", "employed_by", "spent_on"],
)
def political_money_graph_asset(
    context: AssetExecutionContext,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Create the political_money_flow named graph.
    
    Graph structure:
    - donors --[contributed_to]--> committees
    - donors --[employed_by]--> employers  
    - committees --[transferred_to]--> committees
    - committees --[affiliated_with]--> candidates
    - committees --[spent_on]--> candidates (independent expenditures FOR/AGAINST)
    
    This enables queries like:
    - "Trace all money from employer X to any candidate"
    - "Find all committees that received money from donors at company Y"
    - "Follow the money from donor A through any number of committee hops"
    """
    
    with arango.get_client() as client:
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        graph_name = "political_money_flow"
        
        # Delete existing graph if it exists
        if agg_db.has_graph(graph_name):
            agg_db.delete_graph(graph_name, drop_collections=False)
            context.log.info(f"Deleted existing graph: {graph_name}")
        
        # Define edge definitions
        # Each edge definition specifies: edge collection, from collections, to collections
        edge_definitions = [
            {
                "edge_collection": "contributed_to",
                "from_vertex_collections": ["donors"],
                "to_vertex_collections": ["committees"]
            },
            {
                "edge_collection": "transferred_to", 
                "from_vertex_collections": ["committees"],
                "to_vertex_collections": ["committees"]
            },
            {
                "edge_collection": "affiliated_with",
                "from_vertex_collections": ["committees"],
                "to_vertex_collections": ["candidates"]
            },
            {
                "edge_collection": "employed_by",
                "from_vertex_collections": ["donors"],
                "to_vertex_collections": ["employers"]
            },
            {
                "edge_collection": "spent_on",
                "from_vertex_collections": ["committees"],
                "to_vertex_collections": ["candidates"]
            }
        ]
        
        context.log.info(f"🕸️ Creating named graph: {graph_name}")
        
        # Create the named graph
        graph = agg_db.create_graph(
            graph_name,
            edge_definitions=edge_definitions
        )
        
        context.log.info(f"✅ Graph created with {len(edge_definitions)} edge definitions")
        
        # Log collection stats
        stats = {
            "graph_name": graph_name,
            "edge_definitions": len(edge_definitions),
            "vertex_collections": [],
            "edge_collections": []
        }
        
        # Gather vertex collection counts
        for coll_name in ["donors", "employers", "committees", "candidates"]:
            if agg_db.has_collection(coll_name):
                count = agg_db.collection(coll_name).count()
                stats["vertex_collections"].append({"name": coll_name, "count": count})
                context.log.info(f"  📊 {coll_name}: {count:,} vertices")
        
        # Gather edge collection counts
        for coll_name in ["contributed_to", "transferred_to", "affiliated_with", "employed_by", "spent_on"]:
            if agg_db.has_collection(coll_name):
                count = agg_db.collection(coll_name).count()
                stats["edge_collections"].append({"name": coll_name, "count": count})
                context.log.info(f"  🔗 {coll_name}: {count:,} edges")
        
        context.log.info(f"""
🎉 Named graph '{graph_name}' ready for traversal!

Example queries you can now run:

1. Find all money paths from a specific employer's employees to candidates:
   FOR v, e, p IN 1..10 OUTBOUND 'employers/{{employer_key}}'
     GRAPH 'political_money_flow'
     FILTER IS_SAME_COLLECTION('candidates', v)
     RETURN p

2. Find total contributions from donors at a company:
   FOR donor IN 1..1 INBOUND 'employers/{{employer_key}}'
     GRAPH 'political_money_flow'
     OPTIONS {{edgeCollections: ['employed_by']}}
     FOR c IN 1..1 OUTBOUND donor
       GRAPH 'political_money_flow'
       OPTIONS {{edgeCollections: ['contributed_to']}}
       RETURN {{donor: donor.name, committee: c.name}}
""")
        
        return Output(
            value=stats,
            metadata={
                "graph_name": graph_name,
                "edge_definitions": len(edge_definitions),
                "vertex_collections": MetadataValue.json(stats["vertex_collections"]),
                "edge_collections": MetadataValue.json(stats["edge_collections"]),
            }
        )
