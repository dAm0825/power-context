---
name: nearby-place-recommender
description: Recommend nearby places from a natural-language request, verify walking time with AMap MCP tools, and return at most three evidence-based choices.
---

# Nearby Place Recommender

Use this Skill when the user asks for nearby cafés, restaurants, parks, attractions, shops, or similar places and wants location-aware recommendations or walking directions.

## Required evidence

Use the configured AMap MCP tools for all place and route facts. Do not invent a place, address, coordinate, walking distance, walking time, business status, rating, queue, noise level, or opening status.

Prefer these MCP capabilities when available:

- `maps_around_search` or the available nearby/around-search tool for POIs around a coordinate.
- `maps_search_detail` or the available POI-detail tool for an address or location.
- `maps_direction_walking`, `maps_walk_search`, or the available walking-route tool for route distance and duration.
- A geocoding tool such as `maps_geo` only when an exact origin coordinate is not supplied.

Treat tool names as descriptive. If the platform exposes different names, select the available tool with the same function. Never replace an unavailable route tool with a straight-line estimate.

## Procedure

1. Extract the city, origin, requested place type, maximum walking time, maximum result count, and ordering preference.
2. Default to at most three recommendations. Never return more than the user's requested count.
3. Determine the exact origin:
   - Use the supplied coordinate when present.
   - Otherwise geocode the named origin with AMap and use the returned coordinate.
   - If the origin remains ambiguous, ask one concise clarification question instead of guessing.
4. Derive a search radius from the explicit walking-time limit:
   - Use approximately 80 metres per walking minute as a conservative planning value.
   - For 15 minutes, begin around 1,200 metres.
   - If no time limit is given, choose a modest radius and state it.
   - Radius is only a candidate filter; walking duration must come from the route tool.
5. Query nearby POIs with the user's place type and any useful preference terms. Prefer several relevant candidates rather than only one.
6. Inspect candidate names and categories. Remove results of the wrong type, closed permanently, duplicated, or outside the requested area.
7. For each remaining candidate, request a walking route from the exact origin to the POI coordinate. Use the tool-returned distance and duration.
8. Enforce every explicit constraint:
   - Discard candidates whose route duration exceeds the maximum walking time.
   - Respect exclusions such as "no bars", "park only", "indoor", or a named district only when the returned POI data supports them.
   - Do not claim a business is open, quiet, highly rated, or queue-free unless the tool result explicitly provides that fact.
9. Sort valid candidates according to the user's preference:
   - If the user says "closest" or "prefer nearby", sort by walking distance ascending.
   - Otherwise preserve the AMap relevance/order and use walking time as a tie-breaker.
10. Return at most the requested number of places. Every listed place must have been returned by AMap and have an AMap walking route.

## Response format

Present the result as a compact table when more than one place is returned:

| Place | Address | Walking distance | Walking time | Why it fits |
| --- | --- | ---: | ---: | --- |

After the table, add at most two short notes when needed:

- State that opening status, queue length, noise, or similar subjective conditions are `未核实` when no supporting tool field exists.
- Mention any filter or radius used when it helps explain the result.

For a single place, a short bullet list is acceptable.

## Empty and failure handling

- If there are fewer valid places than requested, return the valid subset and explain the limiting constraint.
- If no place satisfies the walking-time constraint, say so explicitly and suggest one adjustment, such as increasing the time limit or changing the place type.
- If AMap returns no nearby POIs, report that no matching place was found in the searched radius.
- If route lookup fails for a candidate, do not recommend that candidate with an estimated time. You may report the candidate as unverified only if it is useful to the user, clearly labelled `路线未核实`.
- If the location or tool service fails, explain the concrete failure and invite the user to retry. Never fabricate a fallback recommendation.

## Safety and privacy

- Never request or expose an AMap API key.
- Use the environment's configured MCP connection only.
- Do not reveal internal tool payloads unless the user asks for them.
- Do not infer a user's real-time precise location without an explicit coordinate or an authorized location tool result.
