### bytewax:

Sources:

- For **REST APIs**: Use **FixedPartitionedSource** for partitioned data processing with recovery, or **SimplePollingSource** if you're polling data periodically from a single API endpoint.
- For **WebSocket**: If you need high-throughput parallel processing, use **DynamicSource**. If the WebSocket stream requires managing state (e.g., reconnecting after disconnection), consider **StatefulSourcePartition**.
- For **Polling**: Use **SimplePollingSource** when polling data at regular intervals from a REST API, especially if exact-once processing is needed.


run pipeline:

`python -m bytewax.run "./bytewax_pipeline/backend/flow:run_hn_flow({post_id}})"
`