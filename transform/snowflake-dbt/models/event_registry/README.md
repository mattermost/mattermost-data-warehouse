# Event registry

The event registry schema holds a high-level view of the events that are stored to the data warehouse via Rudderstack.
There are two main tables available:

- `daily_event_stats` - stores daily count of each event by source.
- `event_registry` - stores facts about each event for its whole lifecycle.

These two tables can power a variety of different reports/analytics:
- Time series display of each event.
- Anomaly detection for event volumes.
- Identify duplicate events or events reused over different parts of the product.

## Future improvements

The event registry can be extended to capture:

- Drill down on events that have some hierarchical structure (i.e. `events` from webapp).
- Changes in the event schemas.
- Ownership of each event.