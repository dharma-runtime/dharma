# GraphQL Generation (Planned)

DHARMA does not yet ship a GraphQL API. The intended design is:

- Generate GraphQL schemas from DHL aggregates.
- Bind resolvers to DHARMA-Q queries.
- Preserve provenance (assertion IDs) in results.

This will enable typed API access without abandoning DHARMA's audit model.

