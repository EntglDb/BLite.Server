// BLite.Client.Java — ClientKeyMap
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Thread-safe cache of field-name ↔ ushort-id mappings shared across all
// collection handles created from a single BLiteClient.

package io.blite.client;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ClientKeyMap {

    private final Map<String, Integer> forward = new ConcurrentHashMap<>();
    private final Map<Integer, String> reverse = new ConcurrentHashMap<>();

    /** Forward map: lower-case field name → ushort id (unmodifiable view). */
    public Map<String, Integer> forward() { return Collections.unmodifiableMap(forward); }

    /** Reverse map: ushort id → lower-case field name (unmodifiable view). */
    public Map<Integer, String> reverse() { return Collections.unmodifiableMap(reverse); }

    /**
     * Merges server-returned entries (name → uint32 id) into the local maps.
     * Values from the server are always treated as the authoritative mapping.
     */
    public void merge(Map<String, Integer> entries) {
        entries.forEach((name, id) -> {
            String lower   = name.toLowerCase();
            forward.put(lower, id);
            reverse.put(id, lower);
        });
    }

    /** Returns true if every name (lowercased) is present in the forward map. */
    public boolean hasAll(Collection<String> names) {
        return names.stream().map(String::toLowerCase).allMatch(forward::containsKey);
    }

    /** Returns the names (lowercased) not yet in the forward map. */
    public Set<String> missing(Collection<String> names) {
        var result = new HashSet<String>();
        for (var name : names) {
            if (!forward.containsKey(name.toLowerCase())) result.add(name.toLowerCase());
        }
        return result;
    }
}
