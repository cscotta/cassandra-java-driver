// Shim provenance: net-new bridge — package com.datastax.shim.* is internal shim support,
// not part of the 3.x ABI (japicmp compares only com.datastax.driver.*). See PROVENANCE.md.
package com.datastax.shim.bridge;

/**
 * Placeholder to bootstrap the shim module build. The real bridge classes (FutureBridge,
 * ExceptionBridge, DataTypeBridge, CodecBridge, ResultSetBridge, StatementBridge, MetadataBridge,
 * EnumBridge, ConfigBridge, TrackerBridge, ...) land here during Tier 0/implementation. This
 * package is package-private to the 3.x ABI (japicmp compares only com.datastax.driver.*).
 */
final class Bridges {
  private Bridges() {}
}
