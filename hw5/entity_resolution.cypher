// @StandingQuery(name="FindEntitiesWithPoBoxAndPostcode", mode="MultipleValues")
MATCH (pb)<-[:poBox]-(e)-[:postcode]->(pc)
RETURN id(e) AS entity, pb.poBox AS poBox, pc.postcode AS postcode;

// @StandingQuery(name="CreateCanonicalAndLink", mode="WriteQuery")
MATCH (e), (canonical)
WHERE id(e) = $that.data.entity
  AND id(canonical) = idFrom($that.data.poBox, $that.data.postcode)
SET canonical.canonical = {poBox: $that.data.poBox, postcode: $that.data.postcode},
    canonical: Canonical
CREATE (e)-[:resolved]->(canonical);

// @StandingQuery(name="MatchResolvedRecords", mode="MultipleValues")
MATCH (record)-[:record_for_entity]->(entity)-[:resolved]->(resolved)
WHERE resolved.canonical IS NOT NULL
RETURN id(record) AS record, id(resolved) AS resolved;

// @StandingQuery(name="EmitResolvedEntity", mode="MultipleValues")
MATCH (record)
WHERE id(record) = $that.data.record
WITH properties(record) AS props
RETURN props {.*, resolved: $that.data.resolved} AS resolved_entity;
