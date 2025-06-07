# Bun's DAG-based Engine (`bun-meta-dag`) – Technical Implementation Specification

## Architecture Overview

Bun’s new DAG-based engine (`bun-meta-dag`) introduces a directed acyclic graph (DAG) to manage build and runtime tasks. This engine models each build step (like parsing a file, executing a macro, or running a test) as a node in a DAG with explicit dependencies, enabling **incremental builds**, concurrency, and introspection. All modules, transformations, and tasks become nodes with edges representing dependencies, ensuring that tasks execute only after their prerequisites and never more than necessary. The engine integrates with Bun’s existing bundler, plugin, and macro systems so that plugins still intercept imports and macros still execute at bundle-time, but now with DAG-driven scheduling. Key architectural features include:

- **DAG Representation:** All build tasks form a graph of nodes (e.g. file reads, transpilation, macro execution). There are no cycles in the *dependency* graph, preventing deadlocks; cyclical module imports are detected and handled gracefully (see edge case handling below).
- **Concurrent Execution:** Independent nodes execute in parallel when possible, using a mix of asynchronous I/O and thread pools for CPU-bound work. This parallelism accelerates builds and test runs, while honoring dependency ordering.
- **Incremental Build & Caching:** Each node tracks a content **version** (hash or timestamp) to detect changes. Unchanged nodes are reused across builds, and only **dirty** (modified or affected) nodes are recomputed. This enables fast re-builds by skipping up-to-date work.
- **Integration with Plugins/Macros:** The DAG engine invokes plugin hooks (onResolve, onLoad, etc.) and macro functions as part of node execution, preserving all existing functionality. The plugin API and macro semantics remain the same, but orchestrated by the DAG for correctness and speed.
- **Reflective Introspection:** The engine provides interfaces to inspect the DAG at runtime – listing nodes, dependencies, states, and versions. This allows tools and plugins to query build graphs and enables rich logging/tracing of build progress. Internally, node definitions are designed with self-descriptive fields (a “schema”) to support reflection.
- **Dynamic Task Generation:** Nodes can spawn new nodes during execution (dynamic recursion). For example, a macro that writes out a new file will add a new file node to the graph on the fly. The engine handles this gracefully, extending the DAG and executing the new nodes as needed.
- **Immutability & Copy-on-Write:** Core DAG data structures are treated as mostly immutable – additions produce new node entries without altering completed ones. This enables safe concurrency and potential use of copy-on-write techniques for efficient multi-process forking. For instance, a worker process can be forked from the main process after initial graph construction, leveraging OS-level copy-on-write memory sharing to avoid reloading identical data in each worker.
- **Cross-Process Coordination:** The engine supports distributing work across multiple OS processes when isolation is needed (e.g. running tests or executing user-provided plugin code). A **process pool** is maintained to spawn and reuse worker processes. The DAG engine orchestrates tasks on these workers and collects results via IPC, so that Bun’s runtime becomes “DAG-aware” even across process boundaries. For example, multiple test files can run in parallel in pooled processes, coordinated by the main DAG scheduler.
- **Integration with Bun Runtime:** The DAG engine hooks into Bun’s runtime and build pipeline at key points (startup, module resolution, file loading, etc.) without regressing any existing features. It augments Bun’s module loader and watch mode to leverage the DAG: watch mode can trigger partial rebuilds of the graph instead of full restarts, and the runtime’s module system can optionally consult pre-built DAG artifacts for faster startup.

Below we specify each component/file in detail, including its purpose, types, functions, and how it collaborates with others. New files are introduced under `src/meta_dag/`, and changes to existing Bun source files are described to ensure seamless integration. All structures and functions are designed for Zig, following Bun’s code conventions and performance focus.

## src/meta_dag/SchemaNode.zig

**Purpose & Scope:** Defines the core data structures for DAG nodes and their “schema”. A *SchemaNode* represents a unit of work (e.g. parsing a source file, executing a macro, etc.) along with its dependencies and current state. This file establishes the **types, enums, and invariants** that all nodes must follow, providing a foundation for DAG execution. The term "Schema" implies that each node’s structure (inputs, outputs, metadata) is well-defined and introspectable, aiding reflection and versioning.

**Types and Structs:**  
- **`enum NodeType`** – Enumerates all node categories in the DAG: e.g. `FileModule`, `TranspiledModule`, `MacroInvoke`, `PluginTask`, `TestTask`, etc. Each type indicates the kind of work and output:
  - `FileModule` – Raw source file input (JS/TS/CSS etc.).
  - `TranspiledModule` – A compiled/transformed module (output of parsing/transpiling a FileModule).
  - `MacroInvoke` – Execution of a macro function at bundle-time.
  - `PluginTask` – Execution of a plugin hook (like onResolve/onLoad) that yields data.
  - `TestTask` – Execution of a test file or test case.
  - `VirtualNode` – A synthetic node for grouping or triggering (e.g. an entry-point aggregator or a final bundle output node).
- **`struct SchemaNode`** – The primary struct representing a node in the DAG. Key fields include:
  - `id: NodeId` – A unique identifier (could be an integer or pointer) for this node within the graph.
  - `type: NodeType` – The category of the node (from the enum above) determining its role.
  - `key: []const u8` – A unique key for the node’s content or task. For file-based nodes, this might be the normalized file path or module specifier. For others, it could be a derived string (e.g. `"macro:<file>#<funcName>"` for a macro call).
  - `deps: []SchemaNode` – An array (or slice) of references to this node’s dependency nodes (the nodes that must complete before this one can run). This encodes the adjacency list of the DAG [oai_citation:5‡bugfree.ai](https://www.bugfree.ai/knowledge-hub/designing-a-dag-based-workflow-engine-from-scratch#:~:text=behavior). The graph structure ensures no duplicate or circular dependencies here.
  - `dependentsCount: usize` – (Optional) count of how many other nodes depend on this node. Maintained for faster reverse lookups (used in invalidation or scheduling ready nodes). This is essentially the out-degree info or tracking of unresolved dependencies count.
  - `state: NodeState` – An enum or struct capturing this node’s state in the build process:
    * `New` (not yet processed),
    * `Pending` (in queue waiting for deps),
    * `Active` (currently executing),
    * `Completed` (done successfully),
    * `Failed` (execution resulted in error).
  - `version: VersionId` – A content/version identifier for the node’s inputs. This could be a hash of the node’s input data or a composite of dependency versions (for instance, a file’s content hash). It’s used to detect changes between builds.
  - `output: ?OutputData` – The result produced by this node, if any. This is type-dependent:
    * For a `TranspiledModule`, this could be a reference to the generated code (e.g., an AST pointer or a Blob/BuildArtifact handle).
    * For a `MacroInvoke`, it may hold the value returned by the macro (as a string or JSON).
    * For a `TestTask`, it could be the test results or status.
    * Many nodes (like `FileModule`) might not store a direct output besides feeding into others.
    * OutputData can be a union or generic container; actual storage might be elsewhere if large (e.g., written to disk or in a cache, with only a handle here).
  - **Invariants:** Each SchemaNode’s dependencies must have a **lower or equal topological order** than itself (no forward refs), ensuring no cycles in the dependency graph. The `id` is unique and stable within a graph instance. If `state` is `Completed` or `Failed`, execution should not be re-run unless `version` changes or the node is explicitly invalidated.

- **`struct VersionId`** – Represents a version or hash of node inputs. Likely a fixed-size hash (e.g. 128-bit or 256-bit from a fast hashing algorithm) or an incrementing generation number. This is used to quickly compare if a node’s inputs have changed. VersionId might be computed from content or timestamps of source files, plus the VersionIds of dependencies (to incorporate transitive changes).
- **`enum NodeState`** – Represents execution state as described above (`New`, `Pending`, etc.). It may also include states like `Skipped` (if up-to-date and not executed in this build) to denote incremental reuse.

**Public Functions & Methods:** (These functions act as methods on SchemaNode or operate on Node types; actual execution orchestration is in other components, but SchemaNode provides foundational behaviors.)
- `fn init(type: NodeType, key: []const u8, deps: []SchemaNode) SchemaNode` – Constructs a new node. Performs validation such as ensuring no duplicate deps and that adding this node won’t introduce a cycle (the Graph will also double-check globally). Sets initial state to `New` and computes initial `version` if possible (e.g. precompute file hash for a FileModule node).
- `fn computeVersion(self: *SchemaNode) VersionId` – Calculates the current content signature of this node. Implementation depends on node type:
  - For `FileModule`: read file content (via Graph or FS interface) and hash it.
  - For `TranspiledModule`: combine the source file’s version with relevant config (transpile target, etc.) to produce version.
  - For `MacroInvoke`: use the macro function source file’s version plus the arguments (if any) as input.
  - This function does **not** execute the node’s task; it only examines inputs. It may delegate to other components (e.g., `Versioning` module) for hashing logic.
- `fn markDirty(self: *SchemaNode)` – Flags the node and (optionally) its dependents as needing re-execution. This sets state to `New` or a special `Dirty` state and invalidates any cached output. It propagates to dependents: any node depending on a dirty node becomes dirty as well (via Graph helper) so they know to recompute. This ensures **incremental build correctness** – if one file changes, all downstream compiled outputs reflect it.
- `fn isUpToDate(self: *SchemaNode) bool` – Checks if the node’s current `version` matches a stored previous version (from a cache or last run). If true and none of its dependencies were re-run, then this node can be **skipped** (its output remains valid). This leverages the `version` field and perhaps a persisted build record. (Integration with `Versioning.zig` provides cached version data for prior builds.)
- `fn getOutput(self: *const SchemaNode) OutputData` – Returns or fetches the output of this node. For completed nodes, it returns the stored `output`. For nodes not yet executed, this either triggers an execution (in context of Executor) or returns nothing. *Note:* In practice, execution is handled by the Executor and this function might be limited to retrieving already available results. It could also perform lazy loading: e.g., if output was serialized to disk cache, this function can load it on demand.
- `fn describe(self: *const SchemaNode) SchemaDescription` – Produces a reflection-friendly description of the node, including type, key, current state, version, and a list of dependency keys/IDs. This is used by introspection tooling (for logging or debugging). The description may be a struct or string that collates key metadata and is safe to call at runtime (read-only, no side effects).

**State Management & Invariants:**  
- The `SchemaNode` struct is primarily a data container; **mutations** to a node’s fields occur in controlled ways:
  - On initialization or when adding dependencies (done through Graph’s building methods).
  - When marking dirty or updating version (which happens in Graph when changes are detected).
  - When execution completes: setting state to `Completed` and storing `output`.
- **No concurrent writes** to a single node occur without synchronization. Typically, a node is “owned” by the Executor thread while running. The design may use atomic flags or a mutex around state changes if needed (especially if multiple threads attempt to markDirty or check state simultaneously). But often, Graph/Executor will ensure a single thread operates on a node at a time. 
- All dependencies in `deps` are assumed to be *finalized* in the graph (i.e., already added to the Graph’s node list). A node should not depend on another that is not yet in the Graph structure.
- **Cycle handling:** If a cycle in module imports is encountered (e.g. module A imports B and B imports A), the Graph’s add logic will detect it (via already-existing node in the dependency chain). In such cases, no new node is created for the repeated import; instead, the existing node is reused in `deps`. This means a node might appear in its own transitive dependency chain. We maintain an invariant that *during execution*, if a node’s dependency is in-progress (to complete a cycle), the engine will not deadlock: one of the cyclic nodes will proceed with partial information. For instance, if A depends on B and B on A, the engine might execute A, which will trigger B; upon detecting A is already running, B’s execution will use A’s partial result (consistent with ES module cyclic loading semantics). The `SchemaNode` may have a flag like `allowPartial` or a special state to denote this scenario. The invariant is that such cycles are rare and must not break the scheduler – they are resolved by runtime semantics rather than full DAG ordering.
- Each `NodeType` may have additional invariants. E.g., `MacroInvoke` nodes always have exactly one dependency which is the macro’s module (the file defining the macro function), ensuring the macro’s code is loaded before invocation.

**Integration & Usage:**  
- `SchemaNode` is used by **Graph** (see `Graph.zig`) as the fundamental element stored in the DAG. Graph operations create and link SchemaNodes.
- Other components (Executor, Coordinator) primarily treat nodes as opaque handles or indices, using Graph’s interface to query or update them, rather than manipulating SchemaNode fields directly in most cases.
- The design is meant to be extensible: adding new NodeTypes (e.g., for future features like CSS preprocessing or new workflow tasks) is straightforward – define the type in `NodeType` and handle its specifics in execution and versioning logic.
- **Reflection:** The node schema is designed to be self-contained so that introspection tools (or even Bun’s JS APIs if exposed) can iterate all nodes and read their fields. This could be used for debugging (printing the build graph) or for features like “rebuild on demand” where a plugin could examine the graph and trigger certain nodes to rerun.

**Error Handling:**  
- SchemaNode itself doesn’t perform complex operations that throw; however, it may carry an `error` field or use the `Failed` state to indicate something went wrong in execution.
- If a node fails (e.g., a syntax error in a source file or an exception in a macro), the `state` is set to `Failed` and the `output` may contain an error object or message. Downstream nodes should notice that one of their deps failed and typically should not run; the Coordinator/Executor will abort or skip further processing accordingly.
- Invariants ensure that once a node is marked `Failed`, dependents will not accidentally run with incomplete inputs. The Graph or Executor likely checks `isUpToDate`/state of deps before scheduling a node, and a failure state will propagate a build failure.
- Any attempt to use an output from a Failed node (via `getOutput`) should raise an error or propagate the failure upstream.

**Copy-on-Write Considerations:**  
- The SchemaNode data can be considered quasi-immutable once execution is done. If the DAG needs to be duplicated (for example, if we implement parallel builds of different targets using the same graph structure, or snapshotting the graph for a new watch cycle), we can copy nodes by reference and only modify differences. The node’s content (like `output`) can be large, so copying is avoided. Instead, if a new version of a node is needed (e.g., file changed), the existing node can either be updated in place with a new version and state reset (if no concurrent usage of old data), or a new node instance can be made representing the new version while the old is retained for reference (the latter approach might be used if multiple build graph versions are kept).
- By treating nodes as immutable once completed, multiple worker processes could share a memory snapshot of the graph (via OS copy-on-write) to avoid duplicating memory. The process pool might fork after initial graph creation; all SchemaNode structures would then be shared as long as they aren’t modified. If a worker needs to modify (e.g., markDirty for an incremental rebuild in that process), it would get a private copy due to COW – this way, each process can diverge if needed without interfering, yet initial memory load is minimized.

In summary, `SchemaNode.zig` provides the definitions for DAG nodes – the building blocks of the meta-dag system – capturing everything about each task’s identity, inputs, and results in a structured, introspectable way.

## src/meta_dag/Graph.zig

**Purpose & Scope:** Manages the collection of `SchemaNode` instances and the relationships (edges) between them. This is the **central DAG management component**: it offers APIs to create or query nodes, enforce DAG invariants (no unintended cycles), and perform dependency lookups. The Graph is responsible for building the dependency graph from entry points, maintaining maps for quick access, and supporting operations like topological traversal, marking nodes dirty, and retrieving affected subgraphs for incremental rebuilds. Essentially, `Graph.zig` is the substrate that connects nodes into a coherent DAG structure.

**Core Data Structures:**  
- **`struct MetaDag`** (or simply `Graph`): The container for all nodes and their indices. Key fields:
  - `nodes: ArrayList<SchemaNode>` – A dynamic array or list of all nodes in the graph. Each node has an integer index (its position in this array) serving as NodeId. Using an ArrayList (or Zig’s `std.ArrayList`) allows contiguous storage for cache efficiency. This can be replaced or complemented by a hash map if Node lookup by key is frequent.
  - `nodeIndexMap: HashMap<[]const u8, NodeId>` – Maps a node’s unique key to its NodeId/index. This allows quick check for duplicates and retrieval (e.g., if a file path has already been added as a node). The key comparison uses normalized keys (like absolute file paths or canonical identifiers for macros) to avoid adding the same logical node twice.
  - `adjacency: ArrayList<[]NodeId>` – (Optional) An explicit adjacency list, where `adjacency[i]` is the list of NodeIds that node `i` depends on (i.e., its `deps`). This might duplicate info in each node’s `deps` field, but can be maintained for faster global operations or memory compaction (since Node stores full objects). If memory is a concern, the graph could store only NodeIds in adjacency lists and have a separate array of node data. We can choose to rely on `SchemaNode.deps` instead to avoid duplication.
  - `readySet: NodeIdSet` – A set or queue of nodes that are ready to execute (dependencies resolved). This is primarily used by the Executor but maintained by Graph: when a node’s dependency count drops to zero (i.e., all deps completed or node has no deps), it is added to the ready set. This could be a simple list or a priority queue if we want to prioritize certain nodes (e.g., maybe entry points first, or smaller tasks first, though priority is optional).
  - `entryNodes: []NodeId` – List of NodeIds that correspond to the original entry points of the build (e.g., the root modules passed to `bun build` or all test files in a test run). This is mainly for reference (to know which outputs to collect or which nodes to mark dirty when restarting).
  - `lock: ThreadMutex` (if needed) – A mutex or atomic lock to synchronize modifications to the graph structure in multi-threaded contexts. If node creation and marking dirty can happen from different threads (which might in watch mode or interactive scenarios), this is needed. However, typical usage will build the graph mostly on a single thread (Coordinator) before parallel execution, so we might minimize locking.
  - `versionCache: ?VersionCache` – (If we use an external cache structure) Reference to a VersionCache/BuildCache component (from `Versioning.zig`), which can persist node versions and outputs between runs. The Graph interacts with this to load prior build info or save updated versions.

**Key Methods (Public API):**  
- **Graph Creation & Initialization:**
  - `fn init() MetaDag` – Initializes an empty graph. Sets up the internal lists and maps. Pre-allocates some capacity if needed (e.g., if we have an estimate of total nodes). Also possibly loads any persisted cache of previous build (through Versioning) to pre-populate known nodes and versions for incremental build (this could be done separately in Coordinator).
  - `fn addNode(node: SchemaNode) NodeId` – Adds a prepared SchemaNode to the graph, returning its assigned NodeId. This ensures the node’s key is unique (checks `nodeIndexMap`):
    * If a node with the same key exists, it can either return the existing NodeId (avoiding duplicate) or handle conflicts (if a duplicate indicates a logical error).
    * If new, it appends to `nodes` list and updates `nodeIndexMap`.
    * Also initializes adjacency info, e.g., allocate an entry in `adjacency` list if used.
    * Not typically called directly by outside code; usually higher-level `createNode*` functions (see below) will construct a node and then call `addNode`.
  - `fn createNode(type: NodeType, key: []const u8, deps: []NodeId, extra: ?*SomeExtraInfo) NodeId` – High-level utility to construct and add a node in one call. It handles creation of SchemaNode (populating its `deps` by converting NodeIds to references), and then uses `addNode`. The `extra` parameter could carry type-specific data (for example, a pointer to AST or a reference to plugin callback for a `PluginTask` node) – such data might instead be stored in a separate context or later in execution, but if needed, it can be attached here or via a separate map from NodeId to extra info.
    - This function will detect if adding the dependencies would introduce a cycle: it might perform a DFS from the new node through deps (or rely on each dependency’s known ancestors) to see if the new node appears in its own transitive deps. If a cycle is found, it logs a warning or raises an error unless it's a known allowable cycle (like a module import cycle). True logical cycles in build tasks (like task A depends on B and B depends on A in general) are not allowed – this would be a configuration error. Module import cycles are allowable in JS semantics, so the Graph might allow the edge but mark the cycle for special handling (as described in SchemaNode, possibly by not requiring one to wait on the other fully).
    - If cycle is detected between distinct nodes (not the same key), Graph could either:
       * Prevent adding the edge and store a flag indicating a cycle; the Executor would then handle executing one side without full dependency resolution.
       * Or accept the edge but ensure the scheduler doesn’t deadlock (this likely involves marking one node as having a “soft” dependency).
    - The simpler approach: do not enforce acyclic check for module import nodes, assume the runtime can resolve it. We document that Graph will log detected cycles for debugging but not treat it as fatal in the case of module imports.
  - `fn getNode(id: NodeId) *SchemaNode` – Returns a pointer or reference to the SchemaNode by ID. For safety, maybe returns an option if id is out of range.
  - `fn findNode(key: []const u8) ?NodeId` – Looks up a node by its key (using `nodeIndexMap`). Returns `null` if not present.
- **Dependency Management:**
  - `fn addDependency(nodeId: NodeId, depId: NodeId) !void` – Adds an edge from `nodeId` to `depId` (meaning nodeId depends on depId’s output). This updates the node’s `deps` list and potentially adjusts a dependency counter. If `depId` was not previously a dependency, increment some indegree counter (or effectively, update `dependentsCount` on the dep, if we track reverse edges). If adding this edge creates a cycle, handle as per above (throw or log). This function might be used during graph construction when some dependencies are discovered after node creation (e.g., we create a FileModule node, then later resolve its imports and add those dependencies once those nodes exist).
  - `fn resolveImport(importSpec: []const u8, importer: NodeId) NodeId` – This is a **critical integration point with the plugin system**. It resolves a module specifier (like `"./foo.ts"` or `"react"`) in the context of an importer module. It should:
    * Call any registered **plugin onResolve hook**. The Graph can keep references to plugin callbacks (perhaps via the Coordinator or a PluginManager). It will check each plugin's onResolve filters and invoke as needed. This might produce a custom resolution (path or namespace).
    * If a plugin yields a result (like a new path or special loader), use that. Otherwise, do default resolution (file system lookup, Node.js-style resolution for node modules, etc., likely leveraging Bun’s existing resolution logic).
    * The result is a concrete path plus optional **namespace/loader**. For example, a YAML file import might resolve to `namespace="yaml"` and path unchanged.
    * Once resolved to an actual file or virtual module, Graph checks if a node for that path (with appropriate type/namespace) already exists (via `findNode`). If not, it creates a new `FileModule` node (or other type if namespace indicates e.g. a virtual text content). If the resolved resource is not a file but provided content (some plugin might directly give `contents` in onLoad), then the Graph might instead create a `TranspiledModule` directly with those contents.
    * Returns the NodeId of the resolved dependency node. Also calls `addDependency(importer, returnedId)` to link them.
    - This function likely lives partly in Graph or in a higher-level loader module, but since it ties into the DAG creation, we include it here. It ensures all module imports become edges in the graph.
  - `fn loadNode(nodeId: NodeId) !void` – Triggers loading of a node’s content if needed. For example, for a `FileModule` node, this would initiate reading the file from disk (possibly async). For a node with plugin-defined content, this may call plugin onLoad hooks:
    * If a plugin onLoad is registered for the file’s type/namespace, call it and get `contents` or transformed output.
    * If no plugin handles it, perform default file read (for source code, read text).
    * Store the content in a suitable place (maybe in the node’s output or a separate staging buffer).
    * Potentially, immediately create a follow-up node: e.g., after reading a `FileModule`, you often need to parse/transpile it. The Graph could directly create a `TranspiledModule` node that depends on the `FileModule` (raw content) node. Alternatively, the approach could be that the `FileModule` node itself, when executed, will produce transpiled output. However, separating raw file read vs transpile into two nodes offers clarity and the ability to cache file reads separately. But combining them is simpler (fewer nodes). We might choose:
      - **Option 1:** `FileModule` node read file and parse it (so it outputs transpiled code). This means no separate node for transpile – simpler graph, but less granular caching.
      - **Option 2:** `FileModule` node only reads content (output = file text), then a `TranspiledModule` node takes that text and performs parse/transform. This is more granular (could skip re-reading file if content unchanged and focus on parse changes e.g., if compiler version changes, though that scenario is rare).
      - We lean toward Option 1 for now for efficiency, but will keep NodeType distinctions in case (like we may still label it differently).
    * The Graph handles plugin onLoad and file IO asynchronously via the Executor or via internal async – see Executor for how this is scheduled. Graph’s role is coordinating the creation of necessary nodes after resolution.
- **Build Orchestration Helpers:**
  - `fn markReady(nodeId: NodeId) void` – Marks a node as ready to execute (no pending deps). Typically called when a node’s dependency count reaches zero. This pushes `nodeId` into the `readySet`. The Executor will pop from `readySet` to execute tasks. This might also check the node’s `isUpToDate` status: if the node is ready but its `isUpToDate()` returns true (meaning nothing changed for it), we might *skip* execution. In that case, we would immediately mark it Completed without scheduling (and call markDependencyComplete on its dependents). This function thus might incorporate incremental short-circuiting.
  - `fn markDependencyComplete(depId: NodeId, succeeded: bool) void` – Signals that a dependency node has finished. This will iterate all nodes that depend on `depId` (we might maintain a reverse adjacency map or each SchemaNode could have a `dependentsCount` or list). For each such node, decrement its unresolved dependency count. If the dependency succeeded and after decrement the count is zero, call `markReady` on that node. If the dependency **failed** (`succeeded == false`), then mark the dependent as blocked/failed as well (or at least do not schedule it). Essentially, propagate failure: nodes waiting on a failed dep cannot run (unless we allow partial builds, but typically one error fails the whole build).
  - `fn getReadyNode() ?NodeId` – Retrieves the next node from the ready set (according to insertion order or priority). This will be called by Executor threads to fetch work. This function should be thread-safe (multiple threads might call to get distinct ready nodes). It may use a mutex or atomic operations to pop from the queue. Alternatively, the Executor might maintain its own thread-local queue fed by Graph’s markReady notifications.
  - `fn resetForIncremental()` – Prepares the graph for a new build iteration (such as in watch mode or a rebuild). This could iterate over all nodes and, for any that were Completed, set them back to Pending/New if they are marked dirty. Or it could clear certain transient state (like readySet, error flags) while keeping the nodes and their versions. Essentially, it resets scheduling without dropping cached results for unchanged nodes.
  - `fn dumpGraphStats()` – Gathers metrics like number of nodes, number of edges, perhaps memory usage, and number of nodes skipped vs executed in the last run. This can be used for logging performance or debugging large DAGs.

**Integration with Plugins & Macros:**  
- The Graph is the locus where plugin and macro integration happens:
  - **onResolve**: As described, during dependency resolution (triggered typically when parsing a file import), Graph calls plugin `onResolve` callbacks. We must ensure this is done at the appropriate time. Implementation detail: The plugin system in Bun provides a JS callback; calling it may be asynchronous (could return a Promise). The Graph method `resolveImport` will likely hand off that work to the Executor or an async mechanism rather than blocking (see Executor for how plugin tasks are handled). A possible design is: when `resolveImport` is called, Graph creates a temporary `PluginTask` node representing that resolution, which depends on nothing and is executed by the Executor via the plugin’s JS runtime. But that might be overkill if resolution is quick. Alternatively, treat `onResolve` as a synchronous call into JS that our engine will await. Given Bun’s plugin API allows promises, we handle it asynchronously: Graph initiates onResolve (maybe via `Executor.schedulePluginResolve(plugin, importer, spec)`), and returns a placeholder node or defers adding dependency until resolution finishes. To simplify the DAG structure, we might *not* model onResolve as a separate node; instead, we incorporate it as part of adding a dependency. We can still reflect it via logging.
  - **onLoad**: Similar to onResolve, when Graph loads a file (in `loadNode`), it will invoke `onLoad` plugin hooks if any match the file type/namespace. If an onLoad returns content, that content is used instead of reading from disk. We may model an onLoad transformation as part of the file node’s execution or as its own node (e.g., a `PluginTask` node that takes a file path and yields content). For clarity: we can treat the plugin onLoad as just part of the file node’s operation (i.e., when executing the file node, check plugins first), rather than a separate node in the DAG. This keeps the graph simpler (the file node encapsulates “load from plugin or disk”).
  - **Macros**: When parsing a file’s content (during the transpile step), the Graph or a Parser will detect imports with `with {type: "macro"}` and calls to those imported functions. Handling macros in the DAG:
    * We create a node for the macro’s module (the file where the macro function is defined) as a normal module (it might be marked specially as macro-containing).
    * We then create a `MacroInvoke` node for each distinct macro call. This node depends on the macro’s module node (ensuring the macro code is loaded) and possibly on the call site’s context if needed (though likely just the code and arguments).
    * The MacroInvoke node, when executed, will run the macro function in Bun’s JS engine and produce a result (which is typically a literal value or snippet to inline). The output of this node is then used by the original module’s compilation.
    * The original file’s compile node (the transpile) should depend on the MacroInvoke’s completion so it can inline the result. That means we likely need to pause parsing when encountering a macro call, schedule the MacroInvoke, wait for it, then resume/complete the transpilation. Implementing that requires the parser to yield or the build to be staged. A simpler approach: perform a first pass parse to identify macro calls, create MacroInvoke nodes for them, execute all MacroInvoke nodes (which can run in parallel since they are independent if they don’t share state), then substitute results and finish generating the code. This can be orchestrated by the Coordinator or a specialized MacroExpander.
    * In Graph terms, the file’s `TranspiledModule` node will list any MacroInvoke nodes as dependencies. So it will not be marked ready until all its macros are done. Once macros are done, the file node can produce final output.
    * This ensures macros remain “automatically parallelized” as Bun intends – our DAG will indeed run multiple MacroInvoke nodes concurrently if they are independent, utilizing threads or worker contexts.
    * If a macro function reads external data (e.g., from a DB or network), that’s outside the DAG’s knowledge (we consider it a pure function from the build’s perspective). It won’t create new dependencies in graph, but such macros are considered non-deterministic by content version (their output could change without input file changes). By default, the DAG will assume the macro output is only a function of its inputs (the macro code and arguments). If needed, users can force rerunning macros via a versioning trick (e.g., include an environment variable in the version hash for that macro node). We mention this as a constraint: if macro outputs need refreshing, the system may require manual invalidation unless the macro’s inputs (code) changed.
- **Ensuring No Regression:** The Graph’s plugin integration logic ensures that any scenario previously handled by Bun’s bundler still works:
  - Multiple plugins with overlapping filters: Graph will try them in order of registration (just like current Bun does) for onResolve/onLoad.
  - Namespaces and virtual modules: Graph supports the concept of namespace by including it in node keys (like `"yaml:./file.yaml"` vs `"file:./file.yaml"` to differentiate). This ensures a plugin can provide a virtual module content that doesn’t collide with a real file node.
  - onStart hooks: Not handled in Graph per se (that’s Coordinator’s job to call at the beginning).
  - Macros disabled (`--no-macros` flag): Graph or Coordinator should check a global config; if macros are disabled, Graph should either not create MacroInvoke nodes and instead treat macro calls as normal function calls (which likely leads to runtime errors), or immediately error. Bun’s current behavior is to error on macro usage if disabled. So Graph/Coordinator will, upon encountering a macro import, either prevent node creation and mark build failed with that error if macros are globally off.

**Concurrency and Thread Safety:**  
- Building the graph (adding nodes/dependencies) primarily happens in the Coordinator’s single-threaded phase initially. However, during execution, some dynamic additions may occur (like a MacroInvoke discovered mid-build). If a worker thread needs to add a node, the Graph provides thread-safe addition:
  - The `lock` mutex is used around modifications to `nodes`, `nodeIndexMap`, and adjacency structures. 
  - `getReadyNode` and `markDependencyComplete` also require thread-safe operations since multiple executor threads coordinate through these.
  - Using atomic operations for counters (like an atomic int for each node’s unresolved dep count) can reduce locking. For example, each node could have `pendingDeps: Atomic(usize)` that tracks how many dependencies are not yet completed. markDependencyComplete can atomically decrement this and check for zero.
  - We ensure that heavy structural modifications (like adding new nodes) are relatively infrequent or batched, to avoid contention.
- The Graph’s data (node list, maps) can grow large for big projects. We ensure lookups (like `findNode` using a hash map) are efficient and possibly pre-sized. Memory usage is a consideration: each node holds some metadata. For very large DAGs (thousands of modules), this should be fine; Zig’s efficient memory should handle it. If needed, we might use an arena allocator for node allocations to improve cache locality.
- **Large DAG optimization:** We avoid deeply nested recursion by using iterative algorithms for dependency resolution and graph traversal where possible. For example, if thousands of files are connected, we prefer queue-based processing to recursion to avoid stack issues.

**Edge Case Handling & Fallbacks:**  
- **Module Resolution Failures:** If `resolveImport` cannot find a module (no plugin handled it and default resolution fails), Graph should mark the importer node as failed (cannot resolve dependency). This yields a clear error to the user (e.g., "Module not found"). The engine must not panic; it reports via the normal error propagation (set node state Failed and propagate).
- **Cycles:** As discussed, cycles in the dependency graph are logged. The Graph might break the cycle by not incrementing dependency count for one edge. For instance, if A depends on B and B depends on A, when adding B’s dependency on A, Graph detects A is an ancestor of B and will not treat A as a pending dependency of B (since A is already executing). It could mark in B that one dependency is cyclical. The net effect: A starts, triggers B; B sees A in progress and proceeds (this requires that at runtime, A’s partial exports are available to B, which the JS module system allows). The DAG scheduler then effectively ran both in parallel-ish. This is tricky but we ensure at least it doesn’t deadlock. If cycles occur in other contexts (like plugin tasks depending on each other, which should not happen ordinarily), Graph will throw an error.
- **Missing Plugins or Macros:** If a macro import is present but the macro is never called, the Graph will still create a MacroInvoke node only when a call is actually encountered. If a macro is imported and called in a dead code branch (not executed), currently Bun’s bundler would still execute it at bundle time if the call is present in code. We follow the same semantics – if the call syntax is there, we treat it as to-be-executed (since static analysis of dead code is complex).
- **Out-of-band changes:** If a file is modified on disk during a build (e.g., user saves file while build is running), our file reading might get partial content. To guard, we could hash after reading and if the file changed mid-read (hash mismatch on second read attempt), possibly retry or warn. But as a fallback, watch mode will catch subsequent changes and rebuild anyway.
- **Graph Persistence:** If loading a persisted graph state (from a previous run’s cache) fails (file format mismatch or partial), the system should fall back to a full rebuild (ignore the cache and recompute all nodes).
- **Namespace conflicts:** If two plugins produce the same key for a virtual module (e.g., both create `virtual:foo` node), the Graph’s `nodeIndexMap` will cause one to reuse the existing node. We ensure keys include plugin identification or unique prefixes to avoid such collisions.

**Integration with Other Components:**  
- The Graph works closely with **Coordinator** (which directs what high-level tasks to add, e.g., “bundle these entry files”) and **Executor** (which uses graph’s ready queue and dependency info to schedule execution).
- It also uses **Versioning** to store and retrieve node versions. When a new node is added or an existing node’s content is loaded, Graph may consult Versioning to see if we have a cached output for this version. For example, if a file’s hash matches a previous build’s, we might skip even reading/parsing if we cached the compiled output – an advanced optimization.
- Graph might expose an interface to **Introspection** for reading the entire node list or graph structure for logging. For instance, `Introspection.dumpGraph(graph)` could iterate over `graph.nodes` and produce a human-readable DAG listing (or a JSON).
- **Logging/Tracing:** Graph emits events to logging system for major events (node added, dependency resolved, cycle detected). These logs can be toggled by a verbose flag. They help in diagnosing build performance or correctness issues.

In summary, `Graph.zig` is the backbone of `bun-meta-dag`: it houses the DAG data and ensures all the relationships are correct and efficiently queryable. By providing robust methods for node addition, lookup, and dependency resolution (with plugin and macro integration), it guarantees that the rest of the engine can focus on execution and coordination, confident that the underlying graph is sound.

## src/meta_dag/Executor.zig

**Purpose & Scope:** Oversees the execution of nodes in the DAG, scheduling tasks, handling concurrency, and utilizing system resources effectively. The Executor is essentially the **scheduler and worker manager** of the DAG engine. It pulls ready nodes from the Graph, dispatches them to threads or worker processes, and manages synchronization (ensuring a node runs after its deps). It’s also responsible for balancing workloads, limiting concurrency to avoid oversubscription, and integrating asynchronous operations (like I/O or waiting on plugin responses) into the DAG execution flow. 

**Key Components & State:**
- **Thread Pool:** The Executor maintains a pool of threads for parallel execution of node tasks (for CPU-bound tasks). For example, if running on an 8-core machine, it might create ~8 threads dedicated to DAG execution. The thread pool could be a fixed array of worker threads launched at Executor start, each running an event loop pulling tasks from a queue.
- **Task Queue / Scheduler:** A concurrent queue of ready tasks (node IDs) waiting to be executed. This could be as simple as the Graph’s `readySet` accessed under lock, or a more complex work-stealing mechanism for threads. Given the likely scale (a build can produce hundreds or thousands of tasks), a simple global queue with a mutex is sufficient and simpler. Each worker thread waits for a task on this queue.
- **Active Task Tracking:** A counter or list of tasks currently executing. This helps in shutting down or in throttling (if we wanted to limit e.g., to certain memory usage or number of heavy tasks at once). Also used for detecting when all tasks are done (when active count zero and queue empty, build is complete).
- **Task Context / Execution Units:** For each node type, Executor knows how to execute it:
  - It might use function pointers or a `switch(node.type)` to decide what action to perform.
  - It leverages specialized modules or functions (e.g., calling parser, calling macro function, etc.). Possibly those are implemented in existing Bun code or in new helper modules.
- **Asynchronous I/O integration:** Many tasks involve I/O (reading a file, waiting for a plugin response). Instead of blocking a thread, Executor should integrate with Bun’s event loop or Zig’s async/await. Zig allows async functions to be executed which do not tie up threads while waiting. We can implement node execution as async functions for tasks that involve I/O:
  - e.g., an async function to read a file and parse it. The file read yields to event loop, allowing the thread to take another task or simply not block others.
  - For simplicity, each Executor worker thread can have its own event loop fiber (Zig’s async runtime). Alternatively, we run all tasks as Zig async in a single thread but that wouldn’t use multiple cores for CPU work. So a mix: heavy CPU tasks on threads, but those threads can still await I/O without blocking.
- **Synchronization with Graph:** Executor communicates back to Graph when a task finishes (success or fail) so Graph can update dependencies and mark new nodes ready.
- **Error Propagation:** If any node fails, Executor will ensure that information is propagated (through Graph’s `markDependencyComplete(succeeded=false)`) and typically skip scheduling of further dependents. It may also coordinate with Coordinator to decide whether to abort the entire build or just mark it incomplete.

**Functions & Behavior:**
- `fn init(graph: *MetaDag, numThreads: usize) Executor` – Initializes the executor. Creates the thread pool (spawning worker threads, each running an internal `workerLoop` function), and stores a reference to the `graph` it will operate on. `numThreads` might default to CPU count or be configurable. Also initializes any synchronization primitives (e.g., an `Atomic(usize)` for active tasks count, a mutex/condvar for the task queue).
- `fn enqueue(nodeId: NodeId) void` – Adds a node to the task queue for execution. This is typically called by Graph’s `markReady` when a node becomes ready. If using a condition variable or similar, this will signal one of the waiting worker threads. This function must be thread-safe.
- `workerLoop(threadIndex: usize) -> void` – The function each worker thread runs. It continuously waits for a ready node (e.g., by locking the queue and popping or waiting on a condvar). When a node ID is retrieved, it executes `runNode(nodeId)`. After completion, it repeats until signaled to exit (like after build complete).
- `fn runNode(nodeId: NodeId) !void` – Executes the task corresponding to the node. Pseudocode:
  ```zig
  node = graph.getNode(nodeId);
  // double-check node is not up-to-date or already done (could happen if skip logic)
  node.state = .Active;
  switch(node.type) {
    .FileModule, .TranspiledModule => processModule(nodeId),
    .MacroInvoke => processMacro(nodeId),
    .PluginTask => processPluginTask(nodeId),
    .TestTask => processTest(nodeId),
    .VirtualNode => processVirtual(nodeId),
    // etc for any other specialized types
  }
  ```
  Each `processX` function will perform the actual work and either return successfully or throw an error. If success, we gather the output and store it in `node.output` (and possibly do post-processing like queue dependents). If error, we propagate it.
  - The execution functions likely call out to existing Bun components:
    * `processModule`: For a source file node (or combined parse/transpile node), this will read the file content (if not already loaded), parse the JavaScript/TypeScript (using Bun’s built-in parser, possibly the same one used by Bun’s transpiler which is based on esbuild), perform any transforms (JSX, etc.), and produce a JS AST or code. It will also detect imports:
      - For each static import found, call `graph.resolveImport(spec, currentNodeId)` which will add dependency nodes (and those might not be ready until this parse completes? Actually, parse can list imports but does not need their content to finish parsing structure, unless it needs to inline macros or type info from them? Typically not for bundler, except macros we handle separately).
      - For each macro call found, create the MacroInvoke node via Graph (which will be scheduled separately). The parse might need to pause or yield at this point, which is complex. More likely: the parse does not evaluate macro calls; instead, after parse, we schedule macro nodes. So `processModule` might only parse up to an AST with markers for macro calls.
      - We likely do this in two phases: (a) Parse into AST, (b) schedule macro invocations found, (c) after macros complete, resume/complete code generation by replacing AST nodes with macro results. The Executor can orchestrate this by, for example, within `processModule` performing step (a), then **waiting** for macro deps:
         + After parsing, mark current node as waiting. Actually, better: when Graph added MacroInvoke deps, this module node will not have been marked ready in the first place until macros done (because we set it to depend on macro nodes). So maybe we do need separate node for final codegen. Alternatively, we can have `FileModule` node do file reading and AST parse, and a separate `TranspiledModule` node depend on both the AST and macros to produce final code. However, managing AST across nodes is heavy since AST is memory. Maybe simpler: the worker that parses can then release the thread and wait for macros, but that complicates thread usage.
         + A simpler method: parse fully but in places of macro calls, instead of needing macro results to finish, stub them out (like put a placeholder literal or something), and then continue codegen. This yields an output that still contains calls to macros (which is not final).
         + But Bun’s bundler specifically needs to inline macro results exactly for correctness. So we really want the actual macro values.
         + Another approach: do a partial execution – run macro calls immediately in the same thread context. But that would break the DAG principle and parallelism of macros.
         + Perhaps the macro parallelism is not extremely critical if it complicates too much. But the spec requires we support it.
      - We might implement macro handling by suspending the module execution: The Executor sees that for a module node with macros, it will spawn subtasks (MacroInvoke nodes) and *pause* the module’s execution until those are done. We can simulate this by not completing the node, leaving it Active, and scheduling the macros. Once macros finish, we resume.
      - In practice, implementing this pause/resume might require threads to have the ability to yield tasks. Maybe easier: break the module processing into two node types:
         1. A `ParseModule` node (reads and parses to AST, outputs AST + macro call list).
         2. For each macro call, MacroInvoke node as usual.
         3. A `GenerateModule` node that depends on both the parsed AST and all MacroInvoke results, and then produces final code.
      - This triple structure ensures no actual thread pausing is needed; the DAG covers the sequence. But does Bun’s current structure allow easily storing and reusing AST? Possibly not trivial in Zig (we could store AST in a node output).
      - However, since spec asks for complete integration, it's worth the complexity to mention. We'll assume either approach is possible. For clarity, we can simply state the Executor handles macro by waiting or splitting tasks, focusing on the end result that macros run concurrently and results are inlined.
    * `processMacro(nodeId)`: This will run a macro function. Likely implemented by invoking Bun’s JavaScript engine (JavaScriptCore) with the target function. We have to ensure the macro’s module code is loaded. The macro’s module node would have executed earlier or is executing concurrently; but better, we ensure macro module is loaded in memory by the time macro call runs:
      - Possibly when macro module node completed, it left behind a JS function pointer or environment that can be invoked.
      - Implementation: The macro module could be required in a separate JS context (like how Bun does it internally – maybe they evaluate the macro file in a Bun VM context and get references).
      - The MacroInvoke node will call that function with provided arguments (if any, in many cases macros might not take external args, just use global state).
      - This can be done by calling into Bun’s JS runtime via FFI or an existing API. Since Bun can run JS (itself being a JS runtime), it might simply call the function in the same process. If concurrency is concern (multiple macros concurrently), it could spin up separate JS contexts or use locks around the JS engine if it’s single-threaded. Possibly Bun’s JS engine can run on multiple threads if separate contexts are created.
      - On completion, capture the return value (which might be a number, string, object). Ensure it’s serializable to be inlined (Bun macros likely restrict to certain types like string/number because they have to be inlined as code).
      - The result is stored in MacroInvoke node’s output (e.g., as a string representing the code literal).
    * `processPluginTask(nodeId)`: If we modeled some plugin actions (like an external plugin code that needed to run in a separate process or thread), this would handle it. For example, if a plugin’s onResolve or onLoad is heavy or asynchronous, we might have created a PluginTask node for it. In that case, this function either:
      - If plugin code is to run in the same process: it calls the JS callback (via Bun’s JS API) and awaits result (like a Promise).
      - If plugin code is run in a separate worker process (which we might prefer for isolation/performance), then this function will delegate to the ProcessPool: e.g., send a message to a plugin runner process and wait for the IPC response. The waiting can be done asynchronously (the thread can yield).
      - Once response is received (path or content), the PluginTask’s output is set (like resolved path or loaded content).
      - The Graph then uses that output: e.g., if it was an onResolve yielding a path, Graph will then create the dependency node and link it (this linking might happen outside Executor, possibly by Coordinator after plugin tasks finish, but could also be done here: after plugin returns, call Graph.addNode for the resolved target).
    * `processTest(nodeId)`: For a test task, likely we want to run a test file in an isolated environment. This might involve using the ProcessPool to run the test in a separate Bun process (to simulate a separate JS environment like Node’s child_process or Worker). The Executor could either directly spawn a process for the test or hand it to ProcessPool:
      - If using ProcessPool, we send a “run this test file” message to a waiting process. That process runs the tests (likely via Bun’s existing test runner logic, but focused on that file), then returns results (pass/fail, logs).
      - The Executor thread will wait for the result via IPC or a future/promise mechanism. Once done, mark the TestTask node as completed (or failed if any test failed, depending on semantics).
      - Alternatively, we could run tests in the same process if isolation isn’t needed (but Bun likely isolates tests).
    * `processVirtual(nodeId)`: VirtualNode might represent grouping tasks (like a final bundling step to concatenate outputs, or a meta task like "All tests complete"). For example, we could have a Virtual node that depends on all TestTask nodes and whose execution simply aggregates results or triggers end-of-build events (like onEnd plugin hooks if we had them).
  - Each processX function should handle errors: catch any exceptions or error codes from subsystems. If an error occurs, it should set the node’s state to Failed and record the error (maybe as a string or error object).
  - On successful completion, mark state Completed and store `node.output` if applicable.
  - After finishing, it calls `graph.markDependencyComplete(nodeId, succeeded)` to notify dependents (including possibly a global "build done" for entry nodes).
- `fn run() !void` – Kicks off execution on the graph. This would typically:
  - Start the thread pool (if not already started in init).
  - Possibly put initial nodes (like entryNodes that have no deps) into the queue.
  - Then wait until all tasks are done. This could be implemented by waiting on a condition variable or polling the active count. It should also handle graceful shutdown: if an error occurs, we might break out early.
  - After completion, join all threads or reuse them for next build. (We can keep threads alive for subsequent watch builds to avoid re-spawning them each time).
- `fn shutdown() void` – Cleans up the thread pool. Signals threads to exit (e.g., by pushing a special sentinel task or using an atomic flag that breaks their loop). Joins all threads. This is called when the entire build process is ending (like bun build finishes or watch mode is being stopped).
- **Thread Pool Reuse:** The Executor can be reused across builds if running in watch mode – threads remain alive and the graph is reset/updated between runs. If so, `run()` can be called multiple times (after graph modifications). Need to ensure that tasks from an old run are cleared and no lingering active tasks exist before starting a new one (shouldn’t if properly waited).
- **Concurrency limits & fairness:** By default, if many tasks become ready at once (say 100 files with no dependencies), Executor will feed them to available threads. If more tasks than threads, others queue up. This is fine. We might not need complex priority scheduling, but if desired, easy tasks (like small macros) could be prioritized to finish quickly (though not strictly necessary).
- **Integration with Bun’s Event Loop:** Because Bun runs an event loop for async operations (like fetch, etc.), we have to ensure our use of threads and async does not conflict. In a standalone bun build process, Bun likely isn’t serving other tasks, so we can safely use threads. Each thread can run blocking operations since it’s separate. However, if a plugin or macro uses Bun’s own async operations (like an HTTP request inside a macro, or `await` something), that complicates things:
  - If macro code uses `await fetch()`, that means within that macro’s JS context, an event loop is needed to drive it. If macro is run in the main thread’s JS context, the main thread event loop must tick. But main thread might be busy running other things. Possibly macros should ideally run in separate Worker threads or processes to allow their own event loop to proceed.
  - Possibly simpler: disallow macros from doing long async operations (not sure if Bun imposes that; it said macros can do DB reads which could be sync or using bun:sqlite which is sync).
  - We can accommodate by running macro in a Worker (which has its own loop) or by running them synchronously (if user does asynchronous, it will need awaiting – perhaps Bun’s macro doesn’t allow top-level await? It might allow macro function to return a Promise, but then bundler would need to await it, which they likely support as plugin hooks support promises).
  - We likely need to support async macros to not limit. So our `processMacro` should detect if the returned value is a Promise and await it (which could yield that thread or use an event loop).
  - Implementation: If macro running in-process, we could call the JS function and if it returns a Promise, use Bun’s JS API to register a callback or poll completion. If macro in separate thread or process, easier: just wait for message (like treat it like plugin tasks).
  - The Executor can incorporate an **async task** concept: e.g., `runNode` can be an `async fn` in Zig, and the worker thread drives it. That way, if `processMacro` awaits something, the thread is not blocked – Zig will schedule other tasks on that thread or it will be free to pick another ready task (if we integrate fully with async).
  - However, mixing Zig async and manual thread pooling is advanced. Alternatively, simpler: treat each macro or plugin call as blocking from the thread’s perspective but since we have multiple threads, others can progress. If a macro does network IO, the thread running it is idle waiting, but other threads can still run other tasks. That’s acceptable if we have enough threads relative to concurrently waiting tasks.
  - If extremely needed, we might increase thread count for those waits or use smaller async tasks, but probably not needed beyond scope.
- **Process Pool Integration:** The Executor will coordinate with `ProcessPool` (discussed later) for tasks offloaded to separate processes:
  - Possibly via functions: `processPool.spawnTask(taskType, data) -> Future<Result>` that Executor can call.
  - For example, in `processPluginTask`, instead of directly executing plugin code, it might do `let result = try processPool.requestPlugin(onResolveFn, args);` which sends an IPC message to a plugin worker process and waits for result. The waiting can be a blocking wait on a condition variable or a promise that is completed by an IPC callback. The Executor thread either blocks (less ideal) or yields. But as reasoned, blocking one thread is okay if others exist.
  - The process pool likely has its own internal thread listening to IPC. Alternatively, Bun’s `Bun.spawn` with `ipc` triggers a JS callback on messages in main thread context (which in our design might conflict if main thread is busy or nonexistent). We might implement IPC at a lower level (OS pipes).
  - We’ll detail process pool separately; from Executor’s view, it’s just a service it calls and waits on.

**Logging and Tracing Hooks:**  
- The Executor should log important events, especially in verbose mode:
  - When a node execution starts (with node key and perhaps which thread).
  - When a node finishes or fails (with duration if possible and any error message).
  - Thread start/stop events.
  - A summary at end: e.g., “Build completed: X nodes executed, Y cached, total time Zms”.
- These logs integrate with Bun’s logging infrastructure (perhaps using Bun’s `console` or writing to stderr). They could also feed into the Introspection module for structured data (like a timeline).
- The Executor might also expose a hook for tracing systems: e.g., if Bun has a global tracer or telemetry, we could wrap each task execution with a trace span (including node type and key), which is useful for performance analysis.

**Error Handling and Cancellation:**  
- If a node fails, by design we propagate that to dependents. The Executor or Coordinator might decide to **cancel** the rest of the build if an error is fatal. In bundling, typically one error stops the whole process (no output). In testing, maybe you continue other tests even if one fails, so context-dependent:
  - We likely implement a flag in Executor or Coordinator like `stopOnError`:
    * For bundler builds: `stopOnError = true` (so first error cancels subsequent tasks that aren’t started).
    * For test runs: `stopOnError = false` if we want to run all tests and gather failures.
  - Implementation: if `stopOnError` and a failure occurs, Executor can set a global cancel flag. Worker threads should check this flag when picking new tasks and skip scheduling further tasks, effectively draining.
  - Already scheduled tasks could still be running; we might let them finish or attempt to interrupt them if possible (interrupting is hard if they are CPU tasks or stuck IO).
  - Probably simpler: don’t schedule new tasks after error, but let running ones finish.
  - The Graph’s `markDependencyComplete(succeeded=false)` will prevent dependents of the failed node from ever enqueuing, so those won’t run. But unrelated independent tasks might still be running in parallel – we can either allow them to finish (harmless, just wasted work if we won’t use result) or cancel them. Possibly let them finish to keep system simpler.
- After all threads idle, we then finalize with error state.
- Ensure to free or mark incomplete outputs for failed nodes to not use partial data.

**Performance Considerations:**  
- The Executor uses threads to utilize multiple cores effectively, but we must avoid launching too many OS threads to not thrash the system. The pool is fixed-size by default (maybe slightly above core count if we want to dedicate some threads to I/O wait, but not necessary).
- For large projects, overhead in scheduling and locking should be minimal relative to actual work (parsing, etc). We use lock-free or low-lock designs where possible (like atomic counters for readiness).
- Because Zig is low-level, we can fine-tune affinity or thread priorities if needed (not likely needed).
- We also consider memory: parsing many files concurrently uses memory. Possibly limit concurrency if memory is constrained. This spec doesn’t require memory limiting, but as an edge consideration, in extremely large DAG, the coordinator or user might limit threads count or split build into chunks.

**Integration Points:**  
- **Coordinator** will call `Executor.run()` after Graph is prepared, and wait for it to complete.
- **Graph** interacts by providing ready nodes and receiving completion callbacks. Executor uses Graph’s API extensively (for retrieving nodes and marking completion).
- **ProcessPool** is invoked from Executor for offloading tasks (like plugin and test tasks).
- **Introspection** might hook here by getting events or even querying Executor state (like how many threads, etc.) – likely not needed beyond logging.
- **Existing Bun concurrency**: Bun’s runtime already has an event loop for JS. Our Executor threads should coexist. If the bundler runs as part of Bun’s CLI, we have essentially a native Zig program with an embedded JS engine. Our threads are doing mostly native work (parsing, IO). If they need to run JS (macros/plugins), they might call into JS engine. JavaScriptCore (if that’s what Bun uses) can be invoked from multiple threads if separate JS contexts are used; we must be careful to use the correct context bound to a specific thread or lock around usage of a single context. Possibly we designate one thread (maybe the main thread) as the only JS execution thread for macros/plugins, and others signal it. But that would serialize macros, losing parallelism.
  - Alternatively, create multiple JS VM instances (one per thread that might run macros) and pre-load macro modules into each. This is heavy.
  - Perhaps better: use Bun’s **Workers API** internally – spin up a worker thread (which is effectively a JS isolate) per macro or plugin. But controlling that might be complex.
  - Given Bun’s blog note about plugins running in separate process, we might offload most JS user code to either processes or at least one separate thread so that main bundler threads focus on parsing. We can adopt:
    * Macros: could run in main process but maybe sequentially (they usually aren’t the bottleneck unless heavy). But the spec says parallel macros, so okay.
    * We might decide macros run on a single separate JS thread sequentially or maybe a small pool. If parallel, ensure no two macros that touch same global (like reading env) conflict – probably fine.
    * Plugins: likely run out-of-process for safety, as blog suggests.
  - We will note in ProcessPool that plugin tasks and possibly macro tasks may use it (though macros likely in-process due to need to modify AST).
  
To sum up, `Executor.zig` coordinates running all tasks in the DAG concurrently where possible, bridging between the static graph and actual computations. It ensures that Bun’s build and run tasks complete efficiently, parallelizing heavy operations (like compilation) and correctly handling asynchronous events (file IO, plugin callbacks, macro calls) without sacrificing determinism or stability.

## src/meta_dag/Coordinator.zig

**Purpose & Scope:** The Coordinator is the high-level orchestrator that ties everything together – from initializing the graph, invoking plugins’ lifecycle hooks, to kicking off the Executor and finalizing results. It acts as the interface between Bun’s external commands (e.g. `bun build`, `bun test`) and the DAG engine. Its responsibilities include configuring the DAG engine according to the current build/test request, integrating with Bun’s CLI options (like watch mode, output paths, etc.), and ensuring that existing workflows (plugins, macros, etc.) are properly invoked at the right times. Unlike Graph and Executor, which handle generic mechanics, Coordinator knows about the **specific scenarios** (build vs test, etc.) and about **persisting state** (like caching and watch mode incremental loops).

**Roles and Functionality:**  
- **Lifecycle Management:** 
  - Calls plugin `onStart()` hooks at the beginning of a build, and (in the future) would handle any `onEnd` if defined (the plugin API doesn't specify onEnd, but if needed, Coordinator would be the place).
  - Configures the environment for the build: e.g., sets up any global defines, ensures output directories exist/are cleaned if necessary.
  - After execution, collects outputs and triggers any final steps (like writing files, printing test results, etc.).
- **Entry Point Handling:** 
  - Accepts a list of entry points (e.g., input files for bundler, or test files).
  - For bundling: for each entry file, instructs Graph to create a node (likely a TranspiledModule or at least FileModule) for it. Often bundlers produce one output per entry (unless code-splitting, but Bun’s bundler likely treats each entry separately or a single combined output – we can assume multiple entries => multiple outputs).
  - For testing: identify test files (Bun’s test runner probably finds all `.test.ts` etc.). Possibly the Coordinator for tests will itself gather test files (using existing Bun logic or globs) and then create a TestTask node for each. Alternatively, each test file might first be treated like a module to compile, then run – so potentially a node for compiling it and a node for executing it via worker.
- **Macro & Plugin Setup:** 
  - Possibly load macro and plugin definitions early. For example, if a plugin is provided via CLI, Coordinator will ensure it’s loaded (if plugin is defined in JS, maybe require it or ensure ProcessPool has the plugin code ready).
  - For macros, nothing special needed to start beyond normal module loading.
- **Integration with Versioning (Cache):** 
  - At build start, Coordinator interacts with the `Versioning` module to load any persistent build cache (if present). For example, read a `.bun-dag.json` file that has previous hashes of each file and last build outputs. It then provides that info to Graph (Graph could be passed a pointer to the cache).
  - It sets a flag whether incremental build is enabled (could be by default or via a `--incremental` CLI flag). If incremental is off, Coordinator may choose to ignore cache and always rebuild fresh (Graph would still compute DAG but skip reusing outputs).
  - After a successful build, Coordinator instructs Versioning to save updated hashes/versions of each node and possibly store any cached outputs (like compiled code) to disk for next time.
- **Process Pool Initialization:** 
  - If using `ProcessPool` for plugins or tests, Coordinator will initialize it. E.g., start N plugin workers and M test workers as needed:
    * For plugin tasks: possibly just 1 or 2 processes, since plugins are often serial tasks (the plugin hooks run at known points). But if plugin does a lot or we want isolation per plugin, maybe multiple. We can configure minimal for now.
    * For tests: perhaps pool size = min(numTestFiles, configured concurrency or CPU count).
  - Coordinator passes references of ProcessPool to Executor or the specific modules that need them.
  - It also ensures that processes are given needed context (like environment variables, preload scripts if needed).
- **Event Loop / Worker Integration:** 
  - If macros or some tasks internally use Bun’s Worker threads, the Coordinator might coordinate their creation as well (though likely not needed explicitly – MacroInvoke might spawn a Worker itself if implemented that way).
  - It keeps track of any background threads or processes started so that on completion or error it can terminate them.

**Detailed Behavior Flow (Build as Example):**  
1. **Setup Phase:**
   - Create a `MetaDag` graph via `Graph.init()`.
   - Initialize an `Executor` with thread pool sized to system (and link it to the graph).
   - Initialize `ProcessPool` if needed (with needed workers for plugins/test).
   - Register plugin hooks: Bun’s plugin API usage in runtime means by the time we call `bun.build({... plugins: [...] })`, the plugins are already loaded in memory (the user imported and called `plugin({...})`). Coordinator might retrieve the list of active plugin callbacks (Bun likely stores them in a global or accessible structure).
   - Invoke all `onStart()` plugin callbacks [oai_citation:18‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=onStart%28callback%3A%20%28%29%20%3D%3E%20void%29%3A%20Promise,void). These might perform setup tasks or just log. If they return a Promise, Coordinator awaits them (possibly using Bun’s JS event loop – since plugin onStart are JS, we might call them in main thread JS context and await). We ensure this completes before proceeding. If any onStart fails (throws), abort build with error.
   - Determine entry points: e.g., from CLI arguments or config. For each entry:
     * Normalize the path.
     * If incremental and we have a cache for it, see if we can short-circuit (unlikely for entry, we still need to output).
     * Create a node: likely a `TranspiledModule` node (which will handle full compilation of that entry). Alternatively, create a `FileModule` (read file) node and that will spawn a transpile sub-node, depending how we decided above. For simplicity, we might directly treat each entry as a top-level module node that will produce an output artifact.
     * Mark these nodes as entryNodes in Graph (Graph.entryNodes list).
     * If an entry has no dependencies (like a simple script), it could be ready immediately; but typically it will depend on its file read (which might be itself, depending on design). We likely consider an entry with no imports as still requiring one node of work (the entry itself).
   - At this point, the Graph is seeded with entry nodes (and maybe immediate dependencies if some resolution was done inline).
   - If watch mode: attach filesystem watchers for all files in the graph (or at least entry files initially). This often is done after the first build to know what to watch (including dependencies). Alternatively, rely on DAG to know dependencies and watch them (the DAG has all file nodes).
   - Log that build initialization is done and execution is about to start.

2. **Execution Phase:**
   - Call `Executor.run()` to begin processing. The Coordinator thread (which may be the main thread) can either block until done or periodically check status. In a CLI context, it will likely block but still pump event loop messages (for JS promises) – so possibly the Coordinator is actually a small async that waits on a signal from Executor that build finished.
   - Meanwhile, tasks run on Executor threads as described. Coordinator doesn’t micromanage this, but if we set stopOnError, the first error will be noticed. Possibly the Executor could call back a Coordinator’s function on error detection. Or simpler, the Coordinator knows build failed when Executor returns with an error or flagged error state.
   - During execution, if in watch mode, Coordinator might remain active to catch file changes. But if a build is running, we likely queue changes or ignore them until this build finishes (to avoid collision).
   - If a fatal error occurs, Coordinator may interrupt waiting. But likely easier to just let Executor finish normally (it will skip tasks after error). So from Coordinator perspective, `Executor.run()` returns, and then we check a success flag.

3. **Post-Execution Phase:**
   - If build succeeded:
     * If it’s a bundler build, gather outputs from Graph. For each entry node, get its `output` (which might be a `BuildArtifact` or code string). Write output files to disk if outdir was specified, or if output is memory-only, maybe directly use it (in dev server, maybe they keep it in memory).
     * Possibly invoke plugin onEnd-equivalent actions. Bun’s plugin API doesn’t have onEnd, but the blog did mention something about logging bundle time to a file as an example using onStart for end-of-build tasks (maybe using a separate plugin for that). If we had an onEnd, this is where we’d call it. Since not, maybe nothing here.
     * Print any build warnings or diagnostics collected. The DAG engine might accumulate warnings (like deprecated syntax warnings, plugin warnings, etc.) which we output now.
     * If incremental is enabled, update the persistent cache: write out new version hashes from Graph to disk (via Versioning). Perhaps also store the final artifacts for quick reload.
   - If build failed:
     * Determine cause: could be one or multiple errors. The Graph might store an error list or the first error. We present the error message (including file path and line if available for parse errors, etc.) to the user.
     * Possibly call plugin `onEnd` if it existed with an error flag (no official API, skip).
     * In incremental mode, we might still update cache for nodes that did succeed, but usually a failed build might not update cache to avoid inconsistent state. Simpler: leave cache as-is, or selectively update for completed nodes since they are technically valid. This can be decided – perhaps leave it, to be safe, require a full rebuild next time unless user specifically wants partial cache.
   - If in watch mode:
     * After processing one build (success or fail), do not exit. Instead, go into a loop waiting for file changes.
     * The Coordinator uses the Graph to know which files to watch. Likely by end of build, Graph has nodes for all modules that were involved. We can register watchers for each source file (and maybe new files added dynamically).
     * On a file change event, Coordinator will:
       - Determine which node corresponds to that file (via Graph.findNode by path).
       - Mark that node and its transitive dependents as dirty (`SchemaNode.markDirty` via Graph).
       - Possibly also reset their state in Graph and remove outdated outputs.
       - Then trigger an incremental rebuild: essentially go back to Execution Phase but without tearing everything down:
         + We could reuse the Graph with updated states.
         + We reuse Executor and threads (already running, but idle).
         + Only the dirty parts will actually execute because others are up-to-date.
       - This yields very fast rebuilds: e.g., user changes one file, only that file’s node and those depending re-run, everything else stays cached, achieving live reload quickly.
       - We then loop again for further changes.
     * Provide a way to break out (CTRL-C).
   - Ensure to shut down external resources when done watching or on normal build end:
     * Terminate any ProcessPool workers (unless we keep them alive in watch mode to respond to repeated tasks quickly – which we might do).
     * Join threads in Executor if not reusing (for a single-run CLI, we can join and free).
     * Free or preserve Graph depending on program termination or watch continuing.

**Concurrency & Multi-Process Considerations:**  
- The Coordinator mostly runs single-threaded (the main thread) to manage the high-level flow. It delegates parallel work to Executor and ProcessPool.
- It does, however, handle asynchronous plugin `onStart` calls which might require waiting on Promises. During that wait, it should not block the whole program. Possibly run `onStart` calls within the main JS event loop – since Bun’s CLI likely operates within a JS context for user code, `plugin.setup()` would be called in JS and return a promise, which Bun’s runtime can await. Our Coordinator in Zig can integrate by pumping the event loop until those promises resolve.
- Similarly, in watch mode, Coordinator can use an async I/O event for file watchers.

**Integration with Bun’s Existing Infrastructure:**  
- **bun build CLI:** The CLI command would invoke this DAG Coordinator instead of the old bundler routine. The `Bun.build()` JS API likely calls into Bun’s Zig code – we will map that to use Coordinator under the hood. The public API remains the same, but internally it now constructs this DAG and runs it.
- **bun test CLI:** Similarly, the test runner which was orchestrating tests will now create DAG nodes for tests and use the pool. We must ensure that any test runner features (like test reporters, lifecycle hooks) still work:
  - Bun’s test runner has hooks like beforeAll/afterAll, etc., likely implemented in the test framework in JS. Since we are possibly running tests in separate processes, those will still happen within those processes as they run the test file. That remains unchanged to the user.
  - The output (test results) need to be collected and aggregated. Coordinator can gather results from each TestTask node’s output (which might include counts of passed/failed). Then Coordinator can feed that to Bun’s test reporter code (which might be a JS implementation expecting structured results).
  - If Bun’s test runner previously ran tests sequentially in one process, we are changing behavior. We must integrate carefully: possibly make parallelization optional or at least ensure the same reporter output format.
- **bun run (runtime module loader):** One interesting integration is that Bun’s runtime could benefit from the DAG engine. For example, if `bun run` is executing a TypeScript file, currently Bun transpiles it on the fly. With DAG engine, we could pre-build it. However, that might not be in scope now. But cross-process DAG awareness could mean:
  - If a Worker thread is created (`new Worker("./worker.ts")`), and the main thread already compiled that module, we could share the compiled code (cache) with the worker rather than recompiling in the worker. This would require communication: maybe the DAG’s cache of compiled modules is available globally. Bun might already do something like caching transpiled modules to memory/disk.
  - We can mention: The DAG engine’s cache (Versioning) could be used at runtime such that if a module is loaded that was previously built in the bundle, Bun can fetch the compiled output from cache instead of recompiling. This ensures consistency and speeds up scenarios where you spawn workers or child processes – they can reuse compiled artifacts if available. Coordination wise, if the worker is a separate process, it could read from a shared on-disk cache (like `.bun-cache` or memory-mapped region).
  - This addresses “runtime DAG-aware across worker boundaries”: meaning the global view of what’s compiled is shared. Implementation could be: upon spawning a Bun worker or subprocess, pass it an identifier of the build cache or a copy of needed compiled code (maybe via IPC).
  - The Coordinator might not handle this directly, aside from ensuring the Versioning writes cache to a location accessible by others. The Worker startup code can then check the cache.

**Error Handling & Fallbacks:**  
- If a plugin `onStart` fails, Coordinator aborts before even building graph. It should output the error (which plugin name, what message).
- If Graph construction fails (like an immediate resolution error or a cycle that cannot be allowed), Coordinator stops and prints error.
- If Executor fails mid-run, ensure all threads join and resources free. Possibly the Coordinator will catch the error thrown from `Executor.run()` and handle messaging.
- In case of an unexpected exception (panic) within the DAG engine (e.g., a bug causes an assertion), Coordinator should attempt to gracefully shut down threads and processes to avoid orphaned processes. This is more about robustness – not a normal case to document in spec, but we can mention that the design aims to avoid leaving stray processes (the process pool processes have parent links, so if main dies, they might auto-exit or need explicit kill).
- Fallback path: If for some reason the DAG engine is misbehaving or user wants to disable it, we might provide an option `--no-dag` to use the older path (if maintained). But the prompt says no regression or feature loss, implying we fully replace it. We can mention an **escape hatch**: an environment variable or hidden flag to revert to old bundler if severe issues, just as a precaution for early adoption. But since they didn’t ask, maybe skip.

**Coordination with existing macros and plugins (Summary):**  
- Macros and plugins require no user-facing changes. The Coordinator ensures their hooks run (onStart at beginning, onResolve/onLoad in Graph during, macro calls in graph during execution, and their results correctly applied).
- It ensures no macro or plugin code is missed: e.g., if plugins modify config or provide initial virtual modules (some frameworks might use onStart to add initial nodes), we support that. If onStart needs to inject a virtual module, how would that work? Possibly a plugin’s onStart could call some Bun API to add an entry or so. The Coordinator could expose an API (maybe via JS) to add nodes to bundler. But no known use-case – likely fine.
- The unified plugin API (runtime + bundler) remains unified because we have integrated it into bundler DAG, and runtime still uses some parts. E.g., if a plugin adds a loader for `.toml` files, it will work in bundler and also via Bun’s runtime import because the plugin logic can be invoked either way. With DAG engine, we ensure that the plugin’s onLoad for `.toml` is used both if bundling and if running normally. Possibly we achieve that by making the module loader at runtime also use Graph.resolveImport (which calls plugins). That means even `import toml from './config.toml'` at runtime (if Bun supports that through plugin) will go through the DAG resolution. If not doing full DAG at runtime, perhaps not; but we could still leverage the same plugin logic code path.

In conclusion, `Coordinator.zig` is the conductor of the DAG engine, making sure that everything happens in the right order and context: plugins start first, then the DAG of build tasks is executed, and results are collected, cached, and output. It is where high-level policy lives (incremental on/off, parallel test on/off, error policy) and it serves as the bridge between Bun’s user-facing commands and the internal DAG machinery.

## src/meta_dag/Versioning.zig

**Purpose & Scope:** Manages version hashes, caching, and persistence of build artifacts for the DAG engine. The Versioning component ensures that each node’s outputs can be safely reused when inputs haven’t changed, enabling true **incremental builds** across runs. It provides services for computing content hashes, storing a build manifest of node versions, and (optionally) caching compiled outputs (like transpiled code) to avoid recomputation. This module encapsulates how version IDs (as stored in `SchemaNode.version`) are generated and compared, and handles reading/writing a cache file on disk.

**Key Responsibilities:**  
- **Hash Computation:** Provide functions to hash file contents, ASTs, or other relevant data consistently.
- **Build Manifest:** Maintain a mapping from node keys to last known VersionId and possibly output metadata (like file modification time, output file path or hash of output).
- **Cache Storage:** If outputs are cached (e.g., store transpiled JS of a TS file to disk or in an in-memory cache), manage that storage: where it is saved, how it’s retrieved, and invalidation when versions change.
- **Integration with Graph Execution:** The Graph/Coordinator will query Versioning to decide if a node can be skipped and to retrieve cached outputs. After execution, Versioning is updated with new versions and outputs.
- **Cross-Process Cache Sharing:** By storing cache on disk (or a memory-mapped file), separate processes (like workers, or subsequent runs of Bun) can access compiled artifacts. This addresses the cross-worker DAG awareness: e.g., if a child process knows a module’s hash and finds a compiled artifact in cache, it can use it instead of recompiling.

**Data Structures:**
- **`struct BuildCache`** – Represents the loaded cache data for the current project. Fields might include:
  - `entries: HashMap<[]const u8, CacheEntry>` – Map from node key (e.g., file path or macro ID) to cached info.
  - `cacheDir: []const u8` – Path to directory or file where cache artifacts are stored (could default to something like `node_modules/.bun-dag/` or a global cache in `~/.bun` keyed by project).
  - `cacheVersion: u32` – A version number for the cache schema to invalidate old caches when Bun updates (so a different structure or output format doesn’t reuse incompatible cache).
- **`struct CacheEntry`** – Data about a particular node’s last build:
  - `version: VersionId` – The content hash of the node’s inputs at last build.
  - `outputHash: u64` – (Optional) a hash of the output content. This can be used to quickly verify the output in cache hasn’t been tampered or to compare with newly produced output for debugging.
  - `artifactPath: ?[]const u8` – If the output is stored on disk (for large outputs like compiled JS code), this gives the relative path in cacheDir. Some outputs might not be stored if easily reproducible or small (like a number or short string for macro, which could be directly inlined in the manifest).
  - `timestamp: u64` – (Optional) file modification time (for file nodes) to help detect if content changed without reading (we can compare mtime to see if likely changed before reading file).
  - `meta: ?SomeMeta` – Could hold type-specific extra info, e.g., for test tasks it might store last run results summary, though probably not needed to cache test results.

**Main Functions:**
- `fn computeHash(data: []const u8) VersionId` – Compute a strong hash (e.g., SHA-256 or a faster non-cryptographic hash like xxhash) of the given data. Used for hashing file content or other byte sequences. We might use a cryptographic hash to avoid collisions since reliability is paramount. Possibly Zig’s std has a hash implementation we can use.
- `fn computeCompositeHash(baseHashes: []const VersionId) VersionId` – Compute a combined hash from multiple inputs, e.g., to incorporate dependency versions. This might simply concatenate or use a rolling combination. For example, if we want a node’s version to include its file content and also a relevant config (like the TS compiler options), we could hash the file content and the config string together.
- `fn loadCache(projectRoot: []const u8) !BuildCache` – Reads cache manifest from disk. Perhaps a JSON or binary file (like `.bun-dag.json` or `.bun-dag.bin`). If file not exist, returns an empty cache (with possibly creating new structure). If the file exists but cacheVersion mismatches, ignore it (or migrate if simple).
- `fn saveCache(cache: *BuildCache, projectRoot: []const u8) !void` – Writes the current cache manifest to disk. This is done at end of a successful build (or periodically if needed). It should write atomically (write to temp then rename) to avoid corruption.
- `fn lookup(key: []const u8, newVersion: VersionId) ?CachedOutput` – Check if the cache has an entry for `key` and if the version matches `newVersion`. If yes, returns a `CachedOutput` which might include:
  - A flag that output is available.
  - Possibly the output itself or a handle to retrieve it.
  If the version doesn’t match or no entry, returns null.
- `fn store(key: []const u8, version: VersionId, output: ?[]const u8) !void` – Store the result of a node in the cache. If `output` is provided and large, write it to a file in `cacheDir` and record the path in `CacheEntry.artifactPath`. If output is small or not provided, we may assume it can be reconstructed (some tasks like macro result string might not need storing if we can just recompute by running macro, but the whole point is to avoid running macro, so better to store that too).
  - This updates the in-memory `BuildCache.entries` map. The actual writing to manifest on disk might be deferred until `saveCache`, except large artifact which we write immediately to a file.
  - Handle any errors (e.g., disk full, no permissions) gracefully: we can degrade to not caching that item and perhaps warn the user.
- `fn clearExpiredEntries(nodes: []SchemaNode) void` – Optional: prune cache entries that are no longer relevant, e.g., if a file was deleted or no longer imported. It can compare the current Graph’s keys with cache entries and remove those not in use. This prevents indefinite growth of cache with stale entries.
- `fn makeArtifactPath(key: []const u8) []u8` – Generates a safe filename or path for storing a node’s output. Could use a hash of the key or a sanitized version of the key (like path with directory structure mirrored under cacheDir). For example, if key is `/project/src/util.ts`, artifact path might be `cacheDir/project_src_util_HHHH.js` (with HHHH some hash to avoid collisions on same name). We must ensure different types (like macro vs file) don’t clash if they have same key strings by including type in naming or separate subfolders per type.
- Possibly functions for specific types:
  - `fn hashFile(path: []const u8) !VersionId` – Opens file and streams content to hasher. Could use Bun’s `Bun.hash` if available internally [oai_citation:19‡github.com](https://github.com/oven-sh/bun/issues/19574#:~:text=GitHub%20github,CryptoHasher%20%2C%20and%20incrementally) (though that is a JS API; in Zig, better do directly).
  - `fn hashJSON(value: JsonValue) VersionId` – if we needed to hash structured data like package.json content (just a possibility).
  - `fn validateCacheEntry(key: []const u8, entry: CacheEntry) bool` – Verifies that a cached artifact file exists and perhaps quick-check its content hash (if we stored outputHash).
  - This could be used to guard against a scenario where the cache manifest says we have an artifact, but the file was deleted or changed. In such case, we treat as cache miss.

**Usage in Build Flow:**  
- When Graph is adding a node and computes its `VersionId` (e.g., using `SchemaNode.computeVersion()` which might call Versioning’s hashing if it needs to read file content):
  - Immediately after computing the version, Graph/Coordinator can call `Versioning.lookup(key, version)` to see if we have a cached result.
  - If a cache hit and output is available, we can mark the node as up-to-date:
    * Perhaps set `node.state = Completed` right away and attach `node.output` from cache (if we have it in memory or read from artifact file).
    * Alternatively, mark `node` with a flag that it can be skipped. The Executor will then not execute it, but still treat it as if it executed.
    * If we choose to not load the output until needed (lazy), we could mark the node as Completed without filling `output` and only retrieve output if a dependent actually needs it. However, dependents typically do need it (like compiled code), so probably simpler to load it now or mark a method to load later.
  - If a node is skipped due to cache, we must still ensure its dependencies are present or consider them implicitly satisfied. Actually, if this node is skipped, we assume its dependencies were also up-to-date (or else we wouldn’t skip because version would differ). 
  - For safety, might still run through dependency creation logic but not execution.
- When Executor finishes running a node, and node is Completed:
  - Call `Versioning.store(node.key, node.version, node.output)` to update cache.
  - Possibly do this only if incremental mode is on. If off, we might skip caching entirely or maybe still populate but not use (not critical).
- After entire build, Coordinator calls `saveCache` to persist the manifest.
- In cross-process use:
  - Suppose a worker process needs to compile a module. We can design it to check a shared cache:
    * Perhaps the worker also has an instance of Versioning that points to the same cache directory. It computes the file’s hash and checks the manifest on disk (or an in-memory shared memory if we did that, but simpler is disk).
    * If found, it can directly read the compiled output from cache file and execute it (for a JS file, just load it into the JS VM).
    * If not found, worker compiles and optionally writes to cache (though if it's ephemeral maybe main process will do it too).
    * To keep consistency, maybe only main process writes to cache to avoid race. Or allow workers to write but use file locking or a simple coordination (could be complicated).
    * Simpler approach: in a scenario like tests running in separate process, caching might not be as beneficial per worker – but for repeated test runs or loaded code, maybe.
    * Given complexity, we might say: cross-process caching is supported via the on-disk cache manifest that all processes consult. We assume a single writer (main process on build) for simplicity. Workers mostly read.
  - The ProcessPool could share an OS-level memory map of compiled modules. If Bun uses JavaScriptCore, they might share the JIT cache or bytecode – likely not trivial. But possibly they plan something like that.
  - We mention that versioning and caching allow a scenario where one process compiles a module and another process can reuse it if the version matches, by reading the cached artifact.

**Concurrency & Thread Safety:**  
- The Versioning module mostly operates in Coordinator/Graph context (single thread) when reading/writing manifest, so not heavily concurrent.
- The hashing functions (e.g., computing file hash) can be done in parallel by Executor threads as part of `computeVersion` if needed. For example, reading many files and hashing can be parallel. We should ensure our hash function is thread-safe (if using a global context, but likely each thread will have its own hasher instance).
- If multiple threads want to write to cache (which generally we avoid – let coordinator do final write), some locking needed. But better: only update cache entries from main thread after tasks complete. We can accumulate the updates and apply sequentially at end.
- If we allow immediate writes of artifact files from worker threads (to avoid holding large output in memory), we should coordinate file naming to avoid collisions and possibly lock per file. But we can also funnel artifact writes through main thread: e.g., worker returns a buffer, coordinator writes it out.
- The cache directory creation: ensure once per run (like if not exists, create). This is done in Coordinator setup or first store usage.

**Edge Cases & Constraints:**  
- **Cache invalidation on tool upgrade:** If Bun’s compiler output changes (say a new Bun version with different JS output), old cached outputs might not be binary identical for same input. This is why we keep `cacheVersion` and can incorporate Bun version or a signature of relevant configuration into VersionId. E.g., include the compiler version in the hash of a transpiled module node, so that even if source is same, the version will differ because environment changed. Alternatively, bump `cacheVersion` which invalidates entire cache on major changes.
- **Non-deterministic inputs:** If a node’s output isn’t purely from inputs (like our earlier mention of a macro that returns Math.random()), the caching system will treat it as deterministic and skip re-running if input file didn’t change, thus reusing the old random value. This is a limitation the user must be aware of. We could note: by default, we assume determinism. If necessary, the user can force re-execution by touching the file or disabling cache for that part. We won’t try to solve this automatically.
- **Storage size:** The cache could grow large if outputs are big (like minified bundle code). Possibly provide an upper bound or policy (maybe only cache intermediate transpiled modules, not final bundle, since final is output anyway). But we can assume disk space is not huge concern or user can clean .bun-dag if needed.
- **Security:** If the cache is in a shared location, ensure keys (which might contain file paths) are handled carefully (no path traversal issues). Likely cache inside project avoids that.
- **Failure during cache save:** If writing manifest fails (IO error), we should warn but not fail the build result (the build succeeded just caching didn’t). Similarly, if reading cache fails (corrupted file), we log and proceed without cache.
- **Multiple concurrent bun processes building same project:** Rare scenario, but if it happens, could conflict on cache file. Perhaps use file locking or just accept that the cache might get inconsistent. This is out-of-scope to fully handle – we assume one build at a time per project typically.
- **Watch mode memory buildup:** In watch mode, if we don’t reset the graph each time but mark dirty, the Graph retains all nodes. If user adds new files (via new imports), those nodes accumulate. That’s fine. But if they remove imports, nodes might stay. We might not remove nodes on removal (could be complex), so memory might slowly increase if lots of changes. Acceptable for moderate usage. Or do a full graph rebuild on certain large changes if needed.
- **Large DAG hashing cost:** Hashing very large files (MBs) each run can take time. But if file modtime can be trusted, we could skip hashing and use modtime to decide changed vs not. However, modtime can be unreliable (e.g., if user does VCS checkout, modtime might be older but content changed, etc.). Many build systems prefer content hash for reliability [oai_citation:20‡softwareengineering.stackexchange.com](https://softwareengineering.stackexchange.com/questions/319359/why-incremental-builds-in-make-dont-use-hashing-algorithms#:~:text=Why%20incremental%20builds%20in%20,timestamp). We stick to hashing for correctness. If performance becomes an issue, one could incorporate file size and modtime as a quick check and then hash only if those changed (a common optimization).
- **No Regression:** Ensure that enabling incremental doesn’t change output. For example, in non-incremental mode, a macro that returns a new random number each build would produce different outputs each build. In incremental mode, if nothing else changed, it produces the same number as last time (since we skipped re-running macro). This is technically a difference but arguably not a “regression” in functionality, just in non-deterministic behavior. For things that matter (like code content), it should be fine. We assume macros are used for deterministic code embedding (like compile-time constants or config).

**Integration with Others:**  
- Graph uses Versioning to decide node skip/execute.
- Executor might use it indirectly when loading outputs for skipped nodes.
- ProcessPool/Workers may consult the on-disk cache; we might implement that as part of ProcessPool logic (like when a worker is about to compile a module, it calls into a small cache-check function perhaps shared via a memory map or separate copy of Versioning code in the worker process).
- Introspection could report cache usage stats (like how many nodes were cached vs rebuilt in a run).
- Bun’s global cache: Bun’s package manager has a global cache for downloaded modules, not directly related, but our build cache could potentially reside alongside (maybe in bun’s global cache dir). But likely per-project is simpler to avoid mixing concerns.

In summary, `Versioning.zig` empowers the DAG engine’s incremental and version-aware capabilities by robust hashing and caching. It ensures that Bun does not redo work unnecessarily, and that even across separate runs or separate processes, the build artifacts can be reused given the same input versions. This module closes the loop in making Bun’s DAG engine truly production-grade, supporting quick rebuilds and cross-process efficiency.

## src/meta_dag/Introspection.zig

**Purpose & Scope:** Provides reflective introspection and logging utilities for the DAG engine, allowing both developers and potentially end-users (via debug modes) to inspect the state of the meta-dag and trace its execution. This includes functions to pretty-print the DAG, emit debugging information, and integrate with Bun’s existing logging systems. It also may expose a programmatic API (perhaps via Bun’s JS runtime or CLI flags) to query the DAG structure or execution metrics.

**Features:**
- **DAG Visualization:** Generate a human-readable representation of the DAG: each node with its ID, type, key, state, version, and dependencies listed. Possibly output as text (indented list) or JSON for consumption by tools.
- **Execution Tracing:** Hooks in Executor/Coordinator that call into Introspection to log events (node start, node finish, etc.). Introspection can format these events consistently, including timestamps or durations.
- **Performance Metrics:** Gather timing information (execution time per node, overall critical path, etc.) and possibly memory usage of graph. Provide a summary after build.
- **Interactive Queries (if applicable):** Perhaps allow (in a debug REPL or via a special Bun API) queries like “get dependencies of module X” or “has module Y been built in this run?” – This could be done by exposing Introspection functions to JS (e.g., on `bun` object in dev mode).
- **Safety:** Ensure that introspection operations do not modify the graph (read-only access) and can be toggled off in production for zero overhead.

**Data and Utilities:**
- Possibly define a `struct NodeInfo` mirroring key SchemaNode data but in simpler form for output (string fields etc.).
- `fn dumpNode(node: *SchemaNode) []const u8` – returns a formatted string describing the node (or writes to a buffer). It might include:
  - `NodeID, Type, Key, State, Version (maybe truncated), DepIDs, Output summary`.
- `fn dumpGraph(graph: *MetaDag) []const u8` – returns or logs the entire graph. Could iterate `graph.nodes` and use `dumpNode` on each. Might also sort by id or group by state.
- `fn logEvent(eventType: EventType, nodeId: NodeId, details: ?[]const u8)` – Called by Executor at key points. EventType could be START, COMPLETE, FAIL, SKIP, etc. Introspection will format a log line like “[DAG] START Node 5 (Type=FileModule, Key=src/app.ts)”. It would use Bun’s logging facility (maybe `stderr` or a special logger tag).
- Perhaps a mechanism to throttle logging if too verbose (some builds might have thousands of nodes, and logging every one could slow it down massively). So, maybe only log in verbose mode or provide summary stats.
- `fn collectStats(graph: *MetaDag) BuildStats` – Iterates nodes to count things like total nodes, executed nodes count, skipped nodes count, total dependencies, maybe deepest chain length (for parallelism measure), etc. `BuildStats` struct might have fields for those and for total time which Coordinator provides.
- `fn printStats(stats: BuildStats)` – Nicely prints a summary, e.g.: “DAG Stats: 120 nodes (30 up-to-date, 90 executed in 0.5s, parallelism 4 threads), Critical path 0.2s, 2 warnings, 0 errors.”
- Integration with Bun’s **Console/Inspector**: If Bun has an inspector or debug mode (like how Node has `--inspect` but that's for debugging code), not sure if needed here. But maybe a `--dag-visualize` CLI could dump the DAG to a DOT file for visualization. We can note the possibility: e.g., an env var `BUN_DAG_DUMP=1` triggers writing `bun-dag.dot` that graphically represents dependencies. Not required, but could be trivial with Introspection functions assembling edges.
- Logging macro usage: Possibly track macro execution times or plugin times; could identify slow macros or plugins by printing them.
- Provide information to plugin developers: The introspection could allow a plugin (in code) to query build info. For example, a plugin might want to know all modules processed or gather some analytics. While not a current feature, our system could allow a plugin to call some Bun API that returns the DAG graph or stats. This might be beyond scope, but reflective introspection suggests at least internal usage; exposing to plugin might be tricky to keep stable, so likely internal.

**Usage & Toggle:**  
- The Introspection features should be mostly no-op unless enabled (to avoid overhead in normal runs). We likely enable them when a debug flag is set (`--verbose` or a specific `--dag-info`). 
- Coordinator would check config and then:
  - If verbose, attach Introspection logging to Executor events.
  - Possibly call `dumpGraph` after building graph but before execution (for debugging dependency structure).
  - After build, call `collectStats` and `printStats`.
- If an error occurs, we might use introspection to dump the subgraph around the error for context (like what depended on the failed node).
- For watch mode, if verbose, each cycle can output changes: e.g., "File X changed -> marking Node Y dirty, rebuilding...".

**Concurrency:**  
- Logging from multiple threads: If Executor worker threads call `logEvent` concurrently, we need to synchronize logs to not intermix lines. Possibly use a mutex or channel for log events. Or use atomic appends to a thread-safe output (Zig’s debugging output maybe not thread-safe by default).
- Alternatively, collect logs in thread-local buffers and have main thread flush them. But simplest: lock around writing to console in `logEvent`.
- The overhead of locking on each event might be fine if not thousands per second. If performance is a concern, we limit logs to verbose mode anyway.

**Integration with Existing Logging:**  
- Bun probably has a `Console` or logger for its runtime. But since bundler is lower-level, they might just print to stderr. We can integrate by using Bun’s `console.log` if available (but that requires being in JS context).
- Instead, use Zig's `std.debug.print` or something for now. Ensure not to break formatting for other output. Possibly prefix lines with a tag `[bun-dag]` or such to distinguish.
- If Bun has a concept of warnings (like during build if some minor issue), Introspection might be where we handle them. E.g., if the parser produces warnings (unused variables, etc.), we funnel those through introspection to output nicely.

**Example Output (for clarity in spec):**  
If enabled, user runs with verbose:
```
[DAG] Bundle started (Entry: src/index.ts)
[DAG] onStart plugins done. Building graph...
[DAG] Added Node 1: FileModule("src/index.ts") deps: [2,3]
[DAG] Added Node 2: FileModule("src/util.ts") deps: []
[DAG] Added Node 3: FileModule("src/data.json") deps: []  // maybe handled via plugin loader
[DAG] Node 2 ready (no deps)
[DAG] Node 3 ready (no deps)
[DAG] Node 1 waiting on deps [2,3]
[DAG] START Node 2 (src/util.ts)
[DAG] START Node 3 (src/data.json)
[DAG] COMPLETE Node 3 (0.5ms)
[DAG] COMPLETE Node 2 (1.2ms)
[DAG] Node 1 all deps done, ready
[DAG] START Node 1 (src/index.ts)
[DAG] ... MacroInvoke Node 4 created for random() in index.ts ...
[DAG] START Node 4 (macro random() from random.ts)
[DAG] COMPLETE Node 4 (macro result "0.68")
[DAG] COMPLETE Node 1 (15ms)
[DAG] Bundle completed in 17ms (4 nodes executed, 0 skipped)
```
This hypothetical log shows how introspection logs could look, including macro node creation mid-flow. These help developers verify the DAG execution.

**Ensuring No Impact on Release Build:**  
- Introspection code should be compiled out or inactive unless needed (maybe using a compile-time flag or runtime check). In release mode with no debug flags, calls to logEvent should be minimal overhead (quick branch check).
- Reflection data (like node schema descriptions) are used for introspection; ensure they don’t bloat memory too much. Perhaps generate descriptions on the fly rather than storing a large string for each node permanently. We already have enough in node fields to describe them.

**Edge Cases:**  
- If introspecting a very large graph, printing everything could be unwieldy. Possibly Introspection could detect and avoid printing thousands of lines by default. Or provide a way to filter (like only print failed nodes and their neighbors).
- If used programmatically (like a plugin calling an Introspection API at runtime), ensure it cannot accidentally modify the graph – ideally the API returns copies or const data.
- If used after multiple watch builds, perhaps allow distinguishing the state per build or printing only changed nodes.

**Integration with Tools:**  
- We could optionally output data in a machine-readable form for tooling. E.g., outputting a JSON of the DAG after build, for external analysis or debugging. This might not be needed by Bun devs but can be helpful for analyzing build performance or verifying that incremental is working (by comparing DAG between runs).
- If implemented, maybe triggered by an env var like `BUN_DAG_JSON=filename` to dump.
- But likely not a priority unless for our own test.

In summary, `Introspection.zig` complements the DAG engine with transparency. It ensures that as we introduce this complex system, we have the means to observe and debug it, which is crucial for maintaining a category-leading tool. By integrating with Bun’s logging and providing structured insights into the DAG’s workings, we can both verify correctness and optimize performance over time.

## src/meta_dag/ProcessPool.zig

**Purpose & Scope:** Manages a pool of worker processes (or long-lived subprocesses) that execute certain tasks isolated from the main Bun process. This component addresses **cross-process coordination** for tasks that either need a separate JS context or benefit from parallelism beyond threads – such as running tests in isolation or executing plugin code in a separate environment. ProcessPool handles spawning, reusing, and communicating with these worker processes, ensuring the DAG engine extends **across process boundaries** seamlessly.

**Design Overview:**  
- Bun’s main process (running the Coordinator/Executor) will act as the **master**, and it can spawn multiple **worker** processes (which are essentially additional instances of Bun with a special mode or script).
- Each worker process runs a minimal event loop waiting for tasks from the master. Communication is done via Inter-Process Communication (IPC), likely using message passing (e.g., OS pipes or IPC sockets). Bun’s spawn API supports an `ipc` callback for subprocess messages [oai_citation:21‡bun.sh](https://bun.sh/docs/api/spawn#:~:text=const%20child%20%3D%20Bun.spawn%28%5B,received%20from%20the%20sub%20process); we can leverage that.
- The ProcessPool maintains a pool for each type of task or a unified pool that can handle different tasks (with task type indicated in messages). For clarity, possibly separate pools:
  - `pluginPool` for plugin tasks (onResolve/onLoad code execution),
  - `testPool` for test execution,
  - (In the future, maybe others like an eval pool for running user-provided code at build time).
- Pool size can be configured or defaulted. For example, pluginPool might default to 1 (since plugin tasks typically run sequentially along with build steps), whereas testPool might default to number of CPU cores or user-specified concurrency.

**Data Structures:**
- **`struct ProcessPool`** – The main structure, possibly templated or with sub-pools:
  - `workers: []WorkerHandle` – an array of active worker process handles. Each `WorkerHandle` contains info about the process:
    * `proc: BunSubprocess` – (If available from Bun’s API) a handle to the spawned process, through which we can send messages (maybe via `proc.stdin` or an IPC channel).
    * `busy: bool` – whether the worker is currently running a task.
    * `type: PoolType` – if we maintain one pool that can handle multiple roles, we may tag each worker or separate by pool object.
    * `lastUsed: u64` – timestamp of when it last finished a task, for possible idle timeout (if we want to shut down idle workers after a while to save resources).
  - `taskQueue: Queue<TaskRequest>` – if all workers are busy and a new task comes, queue it (especially for limited pool sizes).
  - `maxWorkers: usize` – capacity of pool (max number of processes it can spawn).
  - `activeCount: Atomic(usize)` – how many workers currently busy, or tasks in progress.
  - `processScript: []const u8` – the script or mode that the worker processes run. This could be a path to an internal JS file that knows how to accept commands. For example, Bun could include a `"bun:plugin-host"` or `"bun:test-host"` module that the worker runs to initialize an IPC listener.
  - `poolType: PoolType` – an enum to distinguish pools, e.g., `.Plugin` or `.Test`. This could be used if we instantiate two separate pools via the same struct type.

- **`struct TaskRequest`** – represents a task to be sent to a worker:
  - `type: TaskType` – e.g. `.Resolve`, `.Load`, `.RunTest`, `.EvalMacro` etc.
  - `payload: []const u8` – The data needed by the worker. Could be a serialized JSON or a small binary:
    * For plugin resolve: might include specifier and importer path.
    * For plugin load: path and maybe content? (Though plugin onLoad might read file itself or get content from main? Possibly main already read file content; we could send content to avoid re-read in worker).
    * For test: path of test file and any flags.
    * For macro (if we decided to run macros in separate process, payload might be function code and args, but likely keep macros in main).
  - `responseTx: Sender<Response>` – a channel or callback to deliver result back to whoever requested (likely the Executor thread waiting). If using Bun.spawn’s `ipc`, we might have to set a global listener rather than direct channel, so perhaps we map an ID to this sender.
  - `id: u32` – an identifier for matching responses. The master will include this ID in the message, and the worker will echo it back with the result, so we can correlate which task the response belongs to.

- **`struct TaskResponse`** – represents result from worker:
  - `id: u32` – matches the request.
  - `success: bool`
  - `resultData: []const u8` – e.g., resolved path or loaded content or test results.
  - `errorMessage: ?[]const u8` – if success is false, an error string.

**Communication Protocol:**  
- Likely JSON messages over IPC for simplicity, or a simple line-based protocol:
  - Master sends: `{"id":1,"type":"resolve","spec":"./foo","importer":"/path/to/file.ts"}\n`
  - Worker receives JSON, processes (calls plugin callback), then sends response JSON:
    `{"id":1,"success":true,"path":"/path/to/foo.ts"}\n`
  - We can define this formally but the idea is straightforward. JSON is human-readable for debugging; binary might be slightly faster but not necessary due to small message sizes relative to task cost.
- We need to ensure that the Bun worker processes are launched with the right environment to handle IPC:
  - Using `Bun.spawn([...], { ipc: (msg)=>{...} })` in Zig is one way if Bun’s spawn provides a way to integrate with Zig (since Bun’s spawn API is in JS typically; in Zig, we might use platform APIs or if Bun exposes a C API).
  - Another approach: since Bun’s runtime is written in Zig, we likely have internal functions to spawn processes. We might directly use OS calls (like `fork`/`exec` or CreateProcess on Win) to spawn a new Bun process with certain args. Or use a library if already present.
  - Given time, we can abstract: ProcessPool uses Bun’s spawn logic to create worker processes.
- The worker process should run a minimal script:
  - For plugin tasks: perhaps an internal script that loads the user’s plugin definitions (so it has the plugin callbacks in memory), then listens for requests to execute them. Alternatively, the worker could re-import the plugin code on each request, but that’s slow. Better: when we spawn plugin worker, we pass it the entire plugin configuration (maybe via preload scripts or environment).
    * Possibly Bun’s plugin system can produce a serialized representation of plugin (or the plugin code can be loaded via `bun --plugincode script.js` or something).
    * Could simply start the worker with the same project environment (so it will run the user’s plugin registration on startup).
    * If plugin has side effects or global state, multiple tasks in same worker could affect each other; but likely fine as they sequentially handle tasks.
  - For test tasks: the worker likely just waits for a message telling which test file to run, then runs `import('/path/to/test')` inside that worker (which triggers Bun’s test runner for that file and collects results).
    * Or maybe better: use Bun’s existing capability to run a single test file (maybe `bun test file.test.ts` does only that file).
    * Possibly we can spawn bun with `bun test --worker file.test.ts` which runs it and outputs result via IPC. But integrating into one process with multiple tests might require a custom harness.
    * More straightforward: have worker script that listens for a `runTest` message with a file path, then programmatically import that test module, run it (Bun’s test framework has hooks to run tests discovered), and then gather results to send back.
    * This requires our own minimal test runner logic in the worker script, or maybe Bun’s test runner can be invoked programmatically. We might integrate an existing internal function that runs tests in current process but restrict to one file.
  - The worker script could be written in JS (since Bun is good at JS and we might reuse existing APIs). It's loaded via the Bun process we spawn. We can bundle that script with Bun’s distribution so it's accessible (like `bun:internal:dag_worker.js`).
- **Lifecycle:**  
  - `fn init(poolType: PoolType, max: usize) ProcessPool` – Start the pool. It will not necessarily spawn all processes immediately (could spawn on demand). For plugin, likely spawn 1 at start to handle initial onResolve quickly. For tests, spawn `min(max, numTests)` at start to distribute.
  - `fn getWorker() !WorkerHandle` – Acquire an idle worker or spawn a new one if under max. If none available, block until one frees (or queue the task).
  - `fn submitTask(request: TaskRequest) !TaskResponse` – Main entry to use the pool: provide a TaskRequest, the pool finds a worker (or waits), sends the request, and waits for the response. This could be synchronous (block current thread until response arrives). In our design, Executor’s thread calling this will block, which is fine as other tasks can run on other threads. We just need to integrate waiting with IPC properly.
    * Possibly use a condition variable or channel that the IPC callback triggers.
    * Implementation: when sending task, store the `responseTx` (like a promise or condvar) in a map keyed by request id. The worker will respond with id, and the pool’s IPC listener will find the entry and signal the waiting thread with the data.
    * Ensure unique request IDs, perhaps a counter.
  - `fn onMessage(workerIndex: usize, msg: []const u8) void` – Callback invoked when a worker process sends a message. This will parse the message (likely JSON) into TaskResponse, then find the waiting TaskRequest (via id) and deliver result:
    * Mark that worker as `busy=false` (task done).
    * If a queued task is waiting, immediately assign it to this now-free worker (to maximize throughput).
    * If not, worker stays idle but alive for future tasks.
    * Signal the thread that requested this task (via the `responseTx`).
  - `fn terminateIdle() void` – Optionally, if a worker has been idle for some time (and maybe memory is a concern), we could kill it. For example, after build complete, we might want to shut down plugin workers (though they’ll exit when main exits anyway). In watch mode, maybe keep them alive to respond faster to next change. This could be a strategy: e.g., keep idle for up to 5 minutes, then shut down to free memory.
  - `fn shutdown() void` – Kill all worker processes in the pool (e.g., in Coordinator cleanup or on program exit). Use `proc.kill()` or send an “exit” message and wait for them to exit, then close.

**Integration in Build Process:**
- **Plugins:** Coordinator will initialize `pluginPool = ProcessPool.init(.Plugin, 1)` if plugin system indicates need (maybe always if any plugin registered). The Graph’s onResolve/onLoad handling (or Executor’s `processPluginTask`) will use `pluginPool.submitTask` to perform the plugin callback:
  - E.g., in Graph.resolveImport, instead of calling JS directly, we create a TaskRequest `{"type":"resolve","spec":..., "importer":...}` and call pluginPool.submitTask. That will block until the worker responds with a resolution.
  - The response gives either new path or none. Then Graph continues with that info.
  - Similar for onLoad: we might send path to plugin, get back contents.
  - The plugin worker process on startup should load the user’s plugin definitions. Possibly we pass the entire config via an environment variable or a file. Alternatively, after spawning, we could send an initial task with type "setupPlugins" containing the plugin code (like a serialized representation or instruct it to import the user’s build file where they called plugin()).
  - Given Bun’s plugin API requires the plugin to be defined in user code, one way: The plugin is likely defined in the main process (the user’s script calls `plugin({...})` which registers it in Bun’s global registry). We can transfer that to worker by writing a temp file with plugin config or by re-importing the user’s plugin module. Perhaps easier: require that the user’s plugin file is known; then in the worker, do `import "<user plugin file>"` which will run the same plugin registration (but that might require bundler context? Hmm, but Bun runtime plugin API would register in that worker’s context something – but that worker doesn’t actually run the bundler itself aside from our commands).
  - Could also have the master send a distilled version: e.g., send the filter regex and an identifier for the callback function (maybe the plugin object can’t be directly serialized though).
  - For now, we assume worker can replicate plugin environment by running some initialization code. We'll ensure in spec that plugin tasks in worker have the same capability as if running in main.
- **Testing:** Coordinator will init `testPool = ProcessPool.init(.Test, N)` where N is number of concurrent test processes desired. When Executor goes to execute a `TestTask` node, instead of doing it on thread, it will call `testPool.submitTask({"type":"runTest","file":<path>})`. That blocks until test done.
  - The worker runs the tests and sends back a summary. We then mark the TestTask node complete or failed accordingly (and might store results to report).
  - If a test crashes the worker or times out, we handle that: maybe detect no response in X time, kill and spawn new worker, mark test failed/time-out. Implementing timeouts is possible by a Timer in master side. We can mention a default time-out for tests perhaps.

**Error Handling & Robustness:**
- If a worker process exits unexpectedly (e.g., crash or killed):
  - The ProcessPool should detect that (maybe BunSubprocess onExit callback triggers, or a read of IPC channel EOF).
  - It should mark any pending task on that worker as failed (deliver an error "Worker died") and possibly respawn a new worker to replace it if needed.
  - If frequent crashes, maybe stop using that pool and propagate error out (failing the build/test).
- If a worker is unresponsive (e.g., stuck in an infinite loop in user code):
  - We might implement a timeout. For plugin tasks, maybe not needed typically, but one could hang.
  - For tests, likely needed to avoid hang.
  - Implementation: when dispatching, record start time; if exceeds threshold, take action (kill process).
  - Communicate timeout as an error in response to waiting thread.
- Resource usage:
  - Each worker is a full Bun process, consuming memory. Limit how many to spawn (that's why pool has max).
  - Possibly allow user to configure via environment or CLI (like `--parallel-tests=N`).
  - Worker processes share OS resources but not memory (except via COW initially). We attempted to mitigate cost by forking after main is warm (copy-on-write):
    * We can do this on Unix by calling fork() in Zig after plugins loaded or such. But Zig manual management might be tricky. If we do Bun.spawn in the normal way, it will start a fresh process that loads everything from scratch (no COW benefit).
    * If we had a way to start a worker via fork (since Bun is Zig, maybe call `std.os.fork`?), then run a function in child to become the worker loop. That might share memory (like loaded ASTs or JIT memory).
    * But doing so in a runtime with GC (JS engine) might not be safe without special handling (JSC might not like being forked while running).
    * This might be advanced to implement safely. Maybe not do it now, but mention that using OS copy-on-write via fork could drastically reduce startup cost for workers, and is a potential enhancement.
    * At least ensure that if Bun.spawn is used, it’s launched with some flags to not reload the whole environment? There might not be such a mode.
  - For now, say we spawn normally but it's relatively lightweight because Bun is fast at startup. The blog did say “lightweight Bun process that starts fast” [oai_citation:22‡bun.sh](https://bun.sh/blog/bun-bundler#:~:text=,for%20Bun%27s%20runtime%2C%20improving%20running), implying they optimized startup for plugin processes.
- Cleanup:
  - After the build, if those workers are no longer needed, ProcessPool.shutdown kills them to not leave zombies.
  - In watch mode, we keep plugin worker persistent (so it doesn’t restart on every rebuild, speeding up repeated plugin calls). Test workers in watch mode might be reused between test runs as well.
- No interference:
  - Ensure the workers do not themselves start a nested DAG build inadvertently. They should be in a mode where they obey master’s commands only. Perhaps run with an env var `BUN_DAG_WORKER=1` that our code checks and prevents some normal behaviors.
  - If a user’s plugin tries to call `Bun.build` (starting another bundler) inside the worker, that would be bizarre – we could not allow that or it would nest. Possibly not a real concern.

**Public API vs Internal:**  
- ProcessPool is internal. We don't expose it to user directly.
- However, it’s tightly integrated with Executor/Coordinator:
  - The Executor’s `processPluginTask` and `processTest` will call it.
  - It needs to hook into Bun’s event loop or OS events to receive messages. Possibly in Zig, reading from a pipe might be done with async IO. Or if Bun’s spawn supports an event callback, we might incorporate that with either JS or an internal loop. Because we're in Zig, likely we will use async I/O on the file descriptor for the subprocess’s stdout (which carries JSON responses).
  - We might dedicate one thread or an async task in main thread to listen for all worker messages and dispatch to appropriate handler. This could be set up by Coordinator after spawning processes.
- If Bun has a Worker thread mechanism, ironically we might use that to handle process I/O: i.e., spawn a Zig thread that loops reading from all pipes and calls `onMessage`. Simpler: maybe each WorkerHandle could have an OS pipe for IPC and we do non-blocking read in the main thread event loop using Zig’s async.
- For spec brevity, say we set up asynchronous readers for each worker’s IPC. They invoke ProcessPool.onMessage.

**Modifications to Existing Code** (Integration):
We have described new components in detail. Now we must ensure all necessary changes to Bun’s existing source are accounted for, so that `bun-meta-dag` integrates without breaking current features:

### Modifications to Existing Bun Source Code

#### src/bundler/entry_points.zig (and related bundler files)
**Purpose of changes:** Replace the current bundler’s module traversal with the DAG engine. The existing code likely parses entry files and recursively processes imports. We will modify it so that:
- Instead of recursively bundling, it creates a `MetaDag` graph and uses `Coordinator` to drive the build.
- Remove or bypass old data structures (like any previous module graph or list of modules in bundler).
- Ensure output writing remains (possibly reuse code that writes out final bundle, but now using results from DAG).
- Integrate plugin and macro calls via Graph (removing old direct calls to macro functions or plugin code in-line).

**Specific Changes:**
- Initialize the DAG Coordinator when `bun build` is invoked. For example, in the function that handles the CLI, replace calls to old bundler with:
  ```zig
  const coordinator = try MetaDag.Coordinator.init(buildOptions);
  const result = try coordinator.runBuild(); // This will use Graph, Executor, etc.
  // result contains artifacts or status
  if (result.success) {
      // write files if outdir specified
      // or print to stdout if that was behavior (bun build prints to stdout if no outdir)
      for (artifact in result.artifacts) {
          try writeFileOrStdout(artifact);
      }
  } else {
      // handle error (already logged perhaps)
      std.process.exit(1);
  }
  ```
- Remove or adapt any loop that was iterating over modules to apply transforms. Now those transforms happen inside DAG nodes.
- **Macro handling:** The bundler likely had logic like “if import has attribute macro, mark that module for macro eval” and “when encountering call to macro, execute it”. We will remove direct execution and instead rely on nodes:
  - Possibly remove any eval calls to JavaScript for macros in the parser; instead, parser should record an AST node and let our DAG handle it. We might integrate with the parser to get macro call info.
  - Ensure the `import with {type:'macro'}` is still parsed and the info passed to Graph to create MacroInvoke nodes.
- **Plugin API integration:** The bundler had code to call `onResolve` and `onLoad` JS callbacks (likely via Bun’s JS engine). Remove those calls and instead rely on Graph’s new integration which uses ProcessPool or direct calls. Also remove any global state that tracked “currently resolving import” if present.
  - Keep the plugin registration mechanism working: i.e., `Bun.plugin({name, setup(fn)})` in JS should still populate structures the Graph uses. Probably in Zig, there’s a global or singleton that stores plugin callbacks (like a list of onResolve handlers with their filter regex etc.). We will reuse that storage: Graph will retrieve from it. We might need to expose those to ProcessPool (maybe by serializing them to the worker).
- **Hot Reload / Watch Mode:** The file `src/bundler/watch.zig` or similar might currently simply restart the bundler process on changes [oai_citation:23‡bun.sh](https://bun.sh/docs/runtime/hot#:~:text=Watch%20mode%20%E2%80%93%20Runtime%20,process%20when%20imported%20files%20change). We will change that logic to utilize the incremental DAG:
  - Instead of restarting, call Coordinator’s incremental rebuild functions. This likely involves hooking the file watcher events to call `Coordinator.markDirty(filePath)` which internally uses Graph.markDirty and re-executes needed nodes.
  - Remove or disable the automatic full-restart on watch; replace with our in-process update.
  - Ensure that any events like “Add file” or “Remove file” are handled: Graph can add nodes for new files if discovered, or if file removed, either treat as error or remove node (not implemented thoroughly, maybe just error out if required file is missing).
- **Output generation:** If Bun’s bundler combined modules into one output file (maybe using an ordering or module wrapper templates), we need to replicate that. Possibly the existing bundler produces a single string that concatenates all modules or uses an AST linking. We should adapt:
  - DAG provides each module’s transformed code (likely still with import/exports resolved or converted to something).
  - We likely still use esbuild-like bundling logic to wrap modules in a closure or something (for ESM -> IIFE or CJS).
  - Possibly Bun reused esbuild’s linker; if so, we might keep that but feed it data from DAG outputs instead of it doing parsing.
  - Or simpler, after DAG, we have a list of modules with code and a dependency graph. We can perform a topological sort (Graph already has one) and then join the code with whatever module loader runtime Bun uses.
  - The coordinator might handle this final step, possibly by calling an existing function that was used in old bundler to assemble the final bundle from a module list.
- **Sourcemaps & other bundler features:** Ensure that if Bun bundler had options for sourcemap, minify, target, they are still applied:
  - These likely integrated in the transformation step (maybe esbuild’s transform supports minify).
  - We incorporate those as settings passed into Node execution (e.g., when parsing/transpiling a module, pass the target and whether to minify).
  - That could be stored in Graph’s context or passed to each relevant node (like a global config accessible).
  - After build, ensure sourcemaps are emitted if requested (if they were combined in output, etc.).
- **Macro security rules:** In docs, macros cannot run from node_modules [oai_citation:24‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=). The bundler likely enforced that by checking file path before executing macro. We must enforce similarly:
  - When Graph is about to create a MacroInvoke node, check the macro function’s module path. If it is within `node_modules`, throw an error (the docs show an error message example). We replicate that error logic in Graph or Coordinator (likely at Graph.resolveImport for macros).
  - Also enforce `--no-macros` flag: likely `options.disableMacros` boolean. Coordinator should refuse to create MacroInvoke and instead error out if a macro import is encountered.
- **Stability:** The new system should not reduce determinism or performance for typical uses. We test equivalence: a simple bun build should produce same output as before, just faster possibly. We ensure output ordering and content are the same:
  - E.g., if previously modules in bundle were in a certain order, ensure our topological sort yields same (topologically sorts are not unique if cycles, but in cycles, either order could appear; we might define a stable ordering by insertion or by alphabetical to avoid differences).
  - If any subtle differences, consider adjusting for backward compatibility.

#### src/bundler/plugins.zig or similar (Plugin integration code)
- Make necessary adjustments so that plugin definitions feed into our DAG:
  - Likely, Bun’s `plugin()` JS function stored the callbacks and filters in some Zig structure like `PluginManager`.
  - We will extend or modify `PluginManager` to support usage by Graph:
    * Perhaps add methods to get the list of onResolve callbacks given an importer path (Graph will iterate and test regex filters).
    * If using ProcessPool for plugins, we might instead serialize these to the plugin worker, but the master still needs to know which plugin to ask.
    * Actually, possibly simpler: the plugin worker runs the actual JS callback so the master just forwards the data. It doesn’t need to know filter logic if it just sends every unresolved import to the worker and the worker itself can decide which plugin handles it by checking filters (since it has the actual plugin objects loaded). That might be easier – offload plugin matching to the worker.
    * If so, Graph.resolveImport could simply send `"resolve": {importer, spec}` to worker, and the worker internally runs through the plugin’s onResolve chain and returns one answer (perhaps the first plugin that returns a result).
    * That centralizes plugin logic in one place (the worker).
    * Similarly for onLoad: worker can try each plugin’s onLoad.
    * Yes, that seems consistent: treat plugin worker as a mini bundler that just does resolution/loading for the master on demand.
    * The main process’s Graph doesn’t need to know details of filters, just that worker will do it.
  - So modifications: ensure the plugin definitions are available to the worker. Could be done by having the worker import the user’s code where `plugin({...})` was called, which will register plugins in the worker’s global just like in main. For this, might need to know which file(s) define plugins; if user registers plugin in their build script or bunfig, maybe pass that file.
  - Possibly Bun’s CLI has a way to specify plugin via config or JS. If user uses `bun.build({ plugins: [...] })` from JS, those plugin objects might be closures. That complicates sending to worker. In such case, maybe main process itself should handle plugin by calling those closures (they exist in main’s JS context).
    * Actually, if the user calls Bun.build in code, they might not expect separate process. But since bun build CLI likely is implemented in C++… Actually, Bun is CLI so user typically doesn’t supply plugin as runtime object except via a config or calling plugin() which is global.
    * The recommended usage in docs is to call `plugin({ name, setup() { ... } })` at runtime, which registers it globally for bundler and runtime.
    * So yes, the plugin definitions are somewhat static, not just closures passed in an options struct.
  - Considering time, we assume plugin definitions can be reproduced or moved to worker easily.

#### src/test/runner.zig (Test Runner integration)
- Bun’s test runner likely enumerates test files, then either runs them in one process or maybe in parallel (not sure if Bun 1.0 did parallel testing).
- We modify it to create DAG nodes for tests and possibly use ProcessPool:
  - Instead of loop over each test file executing it, we:
    * Initialize a MetaDag graph in test mode.
    * For each test file found, create a `TestTask` node (possibly also ensure it depends on compiling that file if needed, though Bun might run TS directly).
    * Actually, if Bun can execute TS in runtime, maybe no separate compile needed. But if wanting to speed up repeated runs, we could compile tests too. Perhaps out of scope to compile to JS then run – Bun likely directly executes TS by JIT.
    * We could still treat the test execution as a node that implicitly compiles as needed (the worker running it will do JIT).
    * So maybe we don't need separate compile nodes for test, rely on Bun’s runtime to do it.
    * Just having TestTask node that goes to process.
    * Mark all TestTask nodes as dependents of a Virtual "AllTests" node if we want an aggregate.
  - Use Executor with concurrency (the tasks will block waiting on external process, but multiple threads can run multiple tests concurrently each waiting on its external process).
  - Or we could let ProcessPool handle multiple tests concurrently with fewer threads (since processes themselves parallelize).
  - Possibly simpler: do not use Executor threads for test tasks at all, just let ProcessPool manage concurrency:
    * i.e., we could bypass Executor for tests and do an alternate approach. But to keep it unified, treat each TestTask as a node scheduled on an Executor thread, which then calls ProcessPool and waits.
  - After execution, gather results:
    * The worker could send structured results (count passes, fails, names of failed tests, etc.).
    * The main process’s Introspection or Coordinator can use that to print test summary and feed into reporters.
    * If Bun’s test reporter is in JS (maybe it prints nice lines per test), perhaps the worker itself prints output of tests (like each test name and ok/failed) to its stdout? That would be in worker’s console, which we could capture and forward.
    * But simpler: aggregate in main and use main’s console to output results in order. Possibly reorder if ran parallel.
    * Could also feed partial results live (e.g., stream logs as tests run). Achieved by worker sending messages for each test or printing through IPC. Could be an improvement but not required initially.
  - The main test runner code that applied filters (like only run tests matching name) might still apply but likely handled inside worker (the worker uses Bun’s test framework which handles filtering).
  - In summary, test runner code becomes a user of DAG: it basically delegates execution to DAG and collects outcome instead of executing tests directly.

#### src/runtime/worker.zig and src/runtime/spawn.zig (Child process improvements)
- To support ProcessPool, we might need to expose some capabilities:
  - Possibly allow spawning a Bun process with an IPC channel easily from Zig. If not already, we implement within ProcessPool using OS-specific calls.
  - The Bun.spawn API shown in docs [oai_citation:25‡bun.sh](https://bun.sh/docs/api/spawn#:~:text=Spawn%20a%20process%20%28%20Bun,) is JS-level. We might use the same underlying functionality in Zig (like in Node, child_process.fork uses IPC).
  - Alternatively, since Bun is our own runtime, we can add a new internal function to spawn a process with a pipe:
    * e.g., `Subprocess BunSpawn(const char* argv[], bool ipc)` that returns a handle and a way to send/receive messages. Perhaps Bun’s code has `src/bun/node/bindings/subprocess.zig` that we can reuse.
  - Once we have that, we can implement onMessage by hooking to the file descriptor. Possibly using Zig’s IO system (registering the fd in an event loop).
  - We might add a small event loop integration: e.g., in Coordinator, after spawning all workers, do:
    ```zig
    for (worker in pool.workers) {
       AsyncIO.spawn(async {
           while (true) {
             const msg = try worker.proc.stdoutStream.readUntilDelimiterByteAsync('\n');
             ProcessPool.onMessage(workerIndex, msg);
           }
         });
    }
    ```
    (Pseudo-code using Zig async to continuously read lines from the process).
  - If not using Zig async, maybe a simpler approach: in the main thread, use blocking reads in a separate thread or use select. But Zig’s async would be neat if available.
  - Possibly we need to tweak how Bun’s main event loop runs during bundler operation to integrate these reads.
- Additionally, consider pooling at OS level:
  - If many processes are spawned and destroyed frequently (maybe for tests if not reusing), it's expensive. ProcessPool avoids that by reuse.
  - If we want to pre-fork with copy-on-write:
    * Could modify how Bun’s process image spawns. Perhaps a specialized call that forks the current process (after initialization) to create many workers quickly.
    * That's complex in cross-platform context and likely not done now. We mention it's a future possibility (especially on Unix, e.g., prefork test workers to have all modules loaded).
  - If we do nothing special, Bun’s fast startup claim suggests maybe it's okay.

No existing file likely has to change for the test concurrency, because test distribution is new logic, aside from disabling any previous concurrency mechanism. If Bun test was serial, fine.

#### src/options.zig (or global flags definitions)
- We may add new CLI flags or config options:
  - e.g., `--no-dag` (for fallback) – if we include an escape hatch.
  - `--dag-trace` or `--verbose` might already exist (bun has `-v` perhaps).
  - `--no-macros` exists (already handled).
  - `--parallel [N]` for tests or builds, if we allow customizing threads or processes count.
  - Possibly an option to disable incremental caching (if someone wants fresh build always).
  - Coordinator will read these from options and configure modules accordingly.

#### src/cli.ts (if Bun’s CLI main is in Zig or partly in TS)
- Ensure that `bun build`, `bun test` commands trigger our new code. If some of these were partially implemented in JS or config, align them.
- If any output format changed or messages (like previously bundler printed “Bundling X…” maybe in CLI), replicate those prints if desired (or use introspection logs).

Given the instruction not to mention time/effort or phases, we’ll not outline transitional steps or partial integration – this spec is as if we fully replace things in one go.

We should now compile this into a final structured Markdown with headings for each file (we already have that format above).

We have to ensure citations remain for anything we referenced:
We inserted citations in a few places:
- plugin intercept and macros lines from docs
- plugin fast process line
- maybe something about DAG definition.

Let's double-check that all citations in analysis text are properly included in final:
We have e.g. ` [oai_citation:26‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=Bun%20provides%20a%20universal%20plugin,both%20the%20runtime%20and%20bundler)` after "Plugins intercept imports..." – we should keep that.
We have ` [oai_citation:27‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=Macros%20are%20a%20mechanism%20for,directly%20inlined%20into%20your%20bundle)` after macro mention – keep that.
We have ` [oai_citation:28‡bugfree.ai](https://www.bugfree.ai/knowledge-hub/designing-a-dag-based-workflow-engine-from-scratch#:~:text=Understanding%20DAGs)` in architecture for DAG concept – keep.
We have ` [oai_citation:29‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=,before%20a%20module%20is%20loaded)` for plugin hooks – yes included.
` [oai_citation:30‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=type%20PluginBuilder%20%3D%20,void%3B%20onLoad%3A)` similarly.
` [oai_citation:31‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=When%20to%20use%20macros)` macros parallel mention – we referenced that concept of macros parallel but did we cite it? In introspection log example, not needed, but in macros handling, we did mention parallel and referenced that macros are automatically parallelized from docs line 169.
Actually [29] L169-L177 says if you run a lot of code at build time, etc. It's a narrative but okay. We can cite it to underline macros can be parallel.
We have ` [oai_citation:32‡bun.sh](https://bun.sh/blog/bun-bundler#:~:text=,to%20extend%20Bun%27s%20runtime%20capabilities)` for "plugins executed in lightweight process" – did we reference that? Possibly in plugin pool discussion we said blog suggests separate process.

We should incorporate that citation where we mention it:
In ProcessPool or plugin integration, mention that Bun’s design already indicates plugin runs out-of-process.
Maybe in ProcessPool or plugin part of Graph:
Yes, in Graph integration, we have comment:
"Bun provides a unified plugin API ... executed in a lightweight Bun process that starts fast" – we should incorporate that line in context.

We can add:
*(From Bun’s bundler blog: “Plugins are executed in a lightweight Bun process that starts fast.” [oai_citation:33‡bun.sh](https://bun.sh/blog/bun-bundler#:~:text=,for%20Bun%27s%20runtime%2C%20improving%20running) This aligns with our process pool design.)*

Maybe in the plugin integration section of Graph or ProcessPool.

Better in ProcessPool intro:
"Bun’s architecture already runs plugins in separate processes for performance isolation [oai_citation:34‡bun.sh](https://bun.sh/blog/bun-bundler#:~:text=,for%20Bun%27s%20runtime%2C%20improving%20running); our ProcessPool builds on this by pooling those processes."

Yes, let's add something like that with the citation [40] or [34] whichever.

Check [40] content:
It shows "Fast plugins. Plugins are executed in a lightweight Bun process that starts fast."

Better to cite [40†L1-L4] after saying "lightweight Bun process".

We should also consider if any other external citations needed? Possibly not, we covered internal docs mainly.

One potential: The concept of using DAG for incremental might cite a known fact, but not needed I guess.

One more: we mention that incremental builds often rely on content hash vs timestamp, maybe cite that or just trust it's fine.

We have a lot of content, which is good (the user wanted very comprehensive).

Ensure to keep paragraphs short and bullet lists as we did.

The final structure:
We have:
- Title (#)
- Architecture Overview (##)
- Then for each file or major component, we used "##".
- At the end we combined existing modifications under a heading "Modifications to Existing Bun Source Code" with sub-sections for each file or area.
But our prompt said: provide complete file/component list including new files and changes to existing code. It did not explicitly say to separate them, but likely listing changed files too.

It said each entry must include etc. Possibly they expected listing also existing files changes as entries.
So doing that as sub-sub sections is fine.

We used "###" for each changed file under a "## Modifications" heading. It's consistent.

Alternatively, we might make each changed file also as "##", but that might clutter top level. "Primary subheadings (##) for main sections" - the main sections here are new components and modifications could be one main section with its sub.

Given readability, grouping modifications under one heading is cleaner.

Yes, because new files was many sections, now modifications are another category.

This is okay as done.

We must double-check paragraphs lengths:
We tried to limit to 3-5 sentences each.

Some paragraphs like in Graph and Executor might be slightly long. We'll need to see if any run 6 or more lines:
The analysis content likely has some large paragraphs.

We should scan and break if needed:
- The "Integration with Plugins & Macros" in Graph, might be long. Possibly break subpoints with bullets? But we did bullet subpoints.

- "In summary" lines I have, maybe a bit long.

Better to break a bit more.

We can break paragraphs around natural transitions:
Often after a semicolon or so, or enumerate things in bullets to shorten.

The bullet lists are fine as is.

Maybe the Architecture Overview might be a bit long but it’s just 7 bullet points preceded by a short intro line. That should be okay.

Maybe we can bullet the "key architectural features include:" but we already have bullet for each feature.

The main risk is some bullet items themselves being long, but bullet items can be a few sentences I think it's fine as long as not huge block.

However each bullet is recommended to be one idea short ideally, but our bullet items often enumerated things with subpoints separated by semicolons or parentheses.

Maybe it's acceptable; they didn't specify bullet length explicitly, just paragraphs.

Bullets can be multi-sentence if needed but maybe keep to maximum 3 sentences.

We might scan for any bullet that goes beyond that:
- Graph.resolveImport bullet seems a bit long? But it's multiple sub-bullets which is fine.

We did break many with sub-lists.

Check "Edge Case Handling & Fallbacks" bullet in Graph, it's a mini-paragraph but bullet.

It's okay though.

Alternatively, we could break that into sub-bullets for each edge case:
like cycles, missing plugin, etc. But we already used bullet inside with headings "Module Resolution Failures:", etc using ** which is effectively making them bold inline.

That might be fine.

At least we bolded each sub-topic in that bullet.

Yes, that is readable.

Time to finalize and ensure formatting consistent:
We used triple backticks for code examples, which is good.

We italicized some hypothetical commentary? Did we?
We used italic for a hypothetical environment var, it's fine.

One more: The instructions on images:
We didn't embed any images, so that's okay.

We just have citations. And we used them properly I think.

Alright, let's go through final check quickly for any broken line:
We should ensure each source references are in correct format `【source†Lx-Ly】`.

We'll check ones included:
- Architecture: ` [oai_citation:35‡bugfree.ai](https://www.bugfree.ai/knowledge-hub/designing-a-dag-based-workflow-engine-from-scratch#:~:text=Understanding%20DAGs)` after "ensuring no cycles".
- Then bullet "Plugins intercept imports..." we had ` [oai_citation:36‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=Bun%20provides%20a%20universal%20plugin,both%20the%20runtime%20and%20bundler)`.
- "Macros still execute at bundle-time" ` [oai_citation:37‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=Macros%20are%20a%20mechanism%20for,directly%20inlined%20into%20your%20bundle)`.
- Int that, fine.
- Graph integration with plugin: we have references `[32]` etc in text:
We mention "onResolve/onLoad hooks [oai_citation:38‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=,before%20a%20module%20is%20loaded) [oai_citation:39‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=type%20PluginBuilder%20%3D%20,void%3B%20onLoad%3A)", good, two citations in one line, allowed.

- Macros automatically parallelized cite: not sure if we explicitly cited it, we mention concurrency for macros with "Bun macros are automatically parallelized" maybe citing [29].
We did cite [29†L169-L177] where we mention parallel macro context in design:
Yes, in Graph macros "ensures macros remain automatically parallelized as Bun intends [oai_citation:40‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=When%20to%20use%20macros)." Actually we have text:
"This ensures macros remain 'automatically parallelized' ... – our DAG will indeed run multiple MacroInvoke nodes concurrently if they are independent, utilizing threads or worker contexts."

We could put citation around "automatically parallelized" referencing [29†L169-L177].
We have that doc text: "it runs with rest of build, it is automatically parallelized..." about macros. Yes line 171-174.

So add ` [oai_citation:41‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=When%20to%20use%20macros)` after that phrase. I'll incorporate that.

- The process pool design and plugin separate process:
We should add ` [oai_citation:42‡bun.sh](https://bun.sh/blog/bun-bundler#:~:text=,for%20Bun%27s%20runtime%2C%20improving%20running)` where we mention it:
In Graph integration, we talk about plugin tasks possibly separate process but not directly mention blog line there.

Better in ProcessPool:
We wrote: "Bun’s architecture already runs plugins in separate processes for performance isolation [oai_citation:43‡bun.sh](https://bun.sh/blog/bun-bundler#:~:text=,for%20Bun%27s%20runtime%2C%20improving%20running); our ProcessPool builds on this by pooling those processes."

Yes, let's include that at top of ProcessPool.

- On test concurrency, maybe no external citation needed.

I think these are enough.

We'll implement these final touches and finalize the answer content.# Bun's DAG-based Engine (`bun-meta-dag`) – Technical Implementation Specification

## Architecture Overview

Bun’s new DAG-based engine (`bun-meta-dag`) introduces a directed acyclic graph (DAG) to manage build and runtime tasks. This engine models each build step (like parsing a source file, executing a macro, or running a test) as a node in a DAG with explicit dependencies, enabling **incremental builds**, concurrency, and introspection. All modules, transformations, and tasks become nodes with edges representing dependencies, ensuring that tasks execute only after their prerequisites and never more than necessary [oai_citation:44‡bugfree.ai](https://www.bugfree.ai/knowledge-hub/designing-a-dag-based-workflow-engine-from-scratch#:~:text=Understanding%20DAGs). The engine integrates with Bun’s existing bundler, plugin, and macro systems so that plugins still intercept imports [oai_citation:45‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=Bun%20provides%20a%20universal%20plugin,both%20the%20runtime%20and%20bundler) and macros still execute at bundle-time [oai_citation:46‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=Macros%20are%20a%20mechanism%20for,directly%20inlined%20into%20your%20bundle), but now with DAG-driven scheduling. Key architectural features include:

- **DAG Representation:** All build tasks form a graph of nodes (e.g. file reads, transpilation, macro execution). There are no cycles in the *dependency* graph, preventing deadlocks; cyclical module imports are detected and handled gracefully (see edge case handling below).
- **Concurrent Execution:** Independent nodes execute in parallel when possible, using a mix of asynchronous I/O and thread pools for CPU-bound work. This parallelism accelerates builds and test runs, while honoring dependency ordering.
- **Incremental Build & Caching:** Each node tracks a content **version** (hash or timestamp) to detect changes. Unchanged nodes are reused across builds, and only **dirty** (modified or affected) nodes are recomputed. This enables fast re-builds by skipping up-to-date work.
- **Integration with Plugins/Macros:** The DAG engine invokes plugin hooks (onResolve, onLoad, etc.) and macro functions as part of node execution, preserving all existing functionality [oai_citation:47‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=,before%20a%20module%20is%20loaded) [oai_citation:48‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=type%20PluginBuilder%20%3D%20,void%3B%20onLoad%3A). The plugin API and macro semantics remain the same, but orchestrated by the DAG for correctness and speed.
- **Reflective Introspection:** The engine provides interfaces to inspect the DAG at runtime – listing nodes, dependencies, states, and versions. This allows tools and plugins to query build graphs and enables rich logging/tracing of build progress. Internally, node definitions are designed with self-descriptive fields (a “schema”) to support reflection.
- **Dynamic Task Generation:** Nodes can spawn new nodes during execution (dynamic recursion). For example, a macro that writes out a new file will add a new file node to the graph on the fly. The engine handles this gracefully, extending the DAG and executing the new nodes as needed.
- **Immutability & Copy-on-Write:** Core DAG data structures are treated as mostly immutable – additions produce new node entries without altering completed ones. This enables safe concurrency and potential use of copy-on-write techniques for efficient multi-process forking. For instance, a worker process can be forked from the main process after initial graph construction, leveraging OS-level copy-on-write memory sharing to avoid reloading identical data in each worker.
- **Cross-Process Coordination:** The engine supports distributing work across multiple OS processes when isolation is needed (e.g. running tests in isolation or executing plugin code in a separate JS context). A **process pool** is maintained to spawn and reuse worker processes. The DAG engine orchestrates tasks on these workers and collects results via IPC, so that Bun’s runtime becomes “DAG-aware” even across process boundaries. For example, multiple test files can run in parallel in pooled processes, coordinated by the main DAG scheduler.
- **Integration with Bun Runtime:** The DAG engine hooks into Bun’s runtime and build pipeline at key points (startup, module resolution, file loading, etc.) without regressing any existing features. It augments Bun’s module loader and watch mode to leverage the DAG: watch mode can trigger partial rebuilds of the graph instead of full restarts, and the runtime’s module system can optionally consult pre-built DAG artifacts for faster startup. Child processes or worker threads can share compiled artifacts through the DAG’s versioned cache, making module loading more efficient across worker boundaries.

Below we specify each component/file in detail, including its purpose, types, functions, and how it collaborates with others. New files are introduced under `src/meta_dag/`, and changes to existing Bun source files are described to ensure seamless integration. All structures and functions are designed for Zig, following Bun’s code conventions and performance focus.

## src/meta_dag/SchemaNode.zig

**Purpose & Scope:** Defines the core data structures for DAG nodes and their “schema”. A *SchemaNode* represents a unit of work (e.g. parsing a source file, executing a macro, etc.) along with its dependencies and current state. This file establishes the **types, enums, and invariants** that all nodes must follow, providing a foundation for DAG execution. The term "Schema" implies that each node’s structure (inputs, outputs, metadata) is well-defined and introspectable, aiding reflection and versioning.

**Types and Structs:**  
- **`enum NodeType`** – Enumerates all node categories in the DAG: e.g. `FileModule`, `TranspiledModule`, `MacroInvoke`, `PluginTask`, `TestTask`, etc. Each type indicates the kind of work and output:
  - `FileModule` – Raw source file input (JS/TS/CSS etc.).
  - `TranspiledModule` – A compiled/transformed module (output of parsing/transpiling a FileModule).
  - `MacroInvoke` – Execution of a macro function at bundle-time.
  - `PluginTask` – Execution of a plugin hook (like onResolve/onLoad) that yields data.
  - `TestTask` – Execution of a test file or test case.
  - `VirtualNode` – A synthetic node for grouping or finalizing (e.g. an entry-point aggregator or a final bundle output node).
- **`struct SchemaNode`** – The primary struct representing a node in the DAG. Key fields include:
  - `id: NodeId` – A unique identifier (could be an integer index or pointer) for this node within the graph.
  - `type: NodeType` – The category of the node (from the enum above) determining its role.
  - `key: []const u8` – A unique key for the node’s content or task. For file-based nodes, this might be the normalized file path or module specifier. For others, it could be a derived string (e.g. `"macro:${filePath}#${funcName}"` for a macro call).
  - `deps: []SchemaNode` – An array (or slice) of references to this node’s dependency nodes (the nodes that must complete before this one can run). This encodes the adjacency list of the DAG. The graph structure ensures no duplicate or conflicting dependencies here.
  - `dependentsCount: usize` – (Optional) count of how many other nodes depend on this node. Maintained for quick reverse lookups (used in invalidation or scheduling ready nodes). This effectively tracks the number of unmet dependencies each node has (or conversely, how many dependents to notify on completion, depending on perspective).
  - `state: NodeState` – An enum or flags capturing this node’s state in the build process:
    * `New` (not yet processed),
    * `Pending` (deps resolving or queued),
    * `Active` (currently executing),
    * `Completed` (done successfully),
    * `Failed` (execution resulted in error),
    * (Optionally `Skipped` for up-to-date nodes that were not executed in the current run).
  - `version: VersionId` – A content/version identifier for the node’s inputs. This could be a hash of the node’s input data or a composite of dependency versions. It’s used to detect changes between builds and decide if re-execution is needed.
  - `output: ?OutputData` – The result produced by this node, if any. This is type-dependent:
    * For a `TranspiledModule`, this could be a reference to the generated code or a handle to a `BuildArtifact`.
    * For a `MacroInvoke`, it may hold the value returned by the macro (e.g., a literal or code snippet to inline).
    * For a `TestTask`, it could be the test results or status.
    * Many nodes (like `FileModule`) might not store a direct output besides feeding into others; or they may store raw content if read.
    * `OutputData` can be a union or generic container; large outputs might be stored externally (e.g., in a cache or file) with only a reference here.
  - **Invariants:** Each SchemaNode’s dependencies must refer to nodes that exist in the graph and represent prerequisite tasks (no circular references in this list). The `id` is unique and stable within a graph instance. Once a node is `Completed`, its output is final (immutable), and its version represents the content that produced that output. If a node is marked `Dirty` (via incremental invalidation), it indicates that its output is no longer valid and it must be recomputed before use.

- **`struct VersionId`** – Represents a version or hash of node inputs. Likely a fixed-size value (e.g. 128-bit or 256-bit hash) for content-addressing. Used to quickly compare if a node’s inputs have changed since last computation. This might wrap a byte array or a simpler 64-bit fingerprint if collision risk is acceptable (conservative approach uses a cryptographic hash for safety).
- **`enum NodeState`** – Represents execution state as described (`New`, `Pending`, `Active`, `Completed`, `Failed`, etc.). This could be a simple enum or a set of bitflags if multiple states can overlap (though here they are mostly exclusive). It helps manage synchronization (e.g., preventing a node from executing twice concurrently).

**Public Functions & Methods:** (Methods on SchemaNode or static helpers operating on nodes. These manage node lifecycle transitions and data access.)
- `fn init(type: NodeType, key: []const u8, deps: []SchemaNode) SchemaNode` – Constructs a new node. Performs validation such as ensuring no duplicate deps and that adding this node won’t introduce a cycle in the dependency graph (the Graph will also double-check globally). Sets initial state to `New` and computes an initial `version` if possible (e.g. for file nodes, could compute hash immediately if content is known; often, full version computation might happen after loading content).
- `fn computeVersion(self: *SchemaNode) VersionId` – Calculates the current content signature of this node based on its inputs. Implementation depends on node type:
  - For `FileModule`: hash the file’s content (this may involve reading the file; in practice, Graph/Executor will handle I/O, and version might be set post-read).
  - For `TranspiledModule`: combine the source file’s content hash with compiler options, or derive from the AST/content (or inherit file’s version if transformation is deterministic and solely based on content + fixed config).
  - For `MacroInvoke`: combine the macro definition file’s version and the callsite context (e.g., arguments or a hash of the string representation of the call if needed).
  - For `PluginTask`: could be based on input (like the path being resolved or loaded) and perhaps plugin code version, but since plugin tasks are usually quick, we might not need a version – we always run them when triggered, or treat the plugin code’s version as part of overall pipeline version.
  - This function should not have side effects other than computing a hash. It may delegate to the Versioning module (see `Versioning.zig`) for consistent hashing algorithms.
- `fn markDirty(self: *SchemaNode)` – Marks the node as needing re-computation. This sets `state` back to `Pending` or a special `Dirty` state and clears/invalidates any cached `output`. This would typically be called when an upstream dependency changed or an external invalidation (like a file changed on disk in watch mode). If this node was `Completed` before, its dependents should also be marked dirty. The Graph will provide functions to propagate such invalidation through the dependency graph (e.g., recursively mark all transitively dependent nodes as dirty).
- `fn isUpToDate(self: *const SchemaNode, cacheVersion: ?VersionId) bool` – Determines if the node can be skipped because it is already up-to-date. This compares the node’s current `version` to a saved version (from a previous build or cache). If `cacheVersion` is provided (e.g., from persistent cache), it checks equality. If no external cache given, it could fall back to comparing `self.version` with a stored `lastBuiltVersion` (which might be kept in memory from a prior run). A true result means the node’s inputs match what was previously built, so its `output` can be reused without re-execution. (Note: For safety, if `output` is not readily available, we might still re-run or require cache retrieval. This function mainly aids the decision to skip execution.)
- `fn getOutput(self: *SchemaNode) OutputData` – Retrieves the node’s output (once `state == Completed`). If the node is not yet completed or was skipped, this may trigger an assertion or return an empty value. It’s primarily used after execution to pass results to dependents. For example, a `TranspiledModule` node’s output might be fetched to feed into the bundler linking stage or to provide code to a test execution.
- `fn describe(self: *const SchemaNode) DebugInfo` – Produces a descriptive struct or string for debugging/introspection. This might include the node’s key, type, current state, version (perhaps truncated), and list of dependency keys or IDs. This is used by Introspection utilities to log or print the graph for analysis.

**State Management & Invariants:**  
- **State transitions:** Valid transitions include `New/Pending` -> `Active` -> `Completed/Failed`. A node can go from `Completed` back to `Pending/Dirty` if invalidated by an upstream change. We ensure that no node goes back to `Active` without first being set to `Pending` and scheduled normally. The engine will not execute a node that is already `Completed` and up-to-date. In case of `Failed`, typically the build stops, or that node’s dependents won’t run; we might allow continuing in test mode.
- **Dependency integrity:** The list `deps` should not contain the node itself (no self-loop) and ideally is acyclic. Cycles are handled at Graph level by not creating second instances of same module and by special-case logic rather than actual cyclic `deps` links (see Graph edge cases). Each dependency should be unique in the list. If a dependency is repeated or if a node indirectly depends on itself, that indicates a logic error in graph construction.
- **Immutability:** Once a node is marked `Completed` with an output and version, those should not change unless the node is invalidated (which is effectively treating it as a new node version). We do not modify a Completed node’s output or version arbitrarily. Instead, a change in inputs leads to a new version and new execution.
- **Thread safety:** SchemaNode data may be accessed by multiple threads (e.g., Executor threads checking states). We use atomic operations or locks for state changes if needed. Typically, marking state and writing output will happen in the context of a single thread (the one executing the node), while other threads might read states. Care is taken to avoid data races (for example, `state` could be an `AtomicEnum` type or protected by a mutex in Graph’s scheduling).
- **Memory management:** If `output` holds dynamic data (like a buffer for code), define who owns that memory. Possibly the Graph or a cache owns it and `SchemaNode.output` is just a reference. We must avoid memory leaks when invalidating nodes (free or allow reuse of old output if overwritten).
- **Identifier uniqueness:** NodeId (which could be an index in Graph.nodes list) uniquely identifies a node. Even if a file is rebuilt (due to changes), it might either reuse the same node entry with updated version or create a new node entry. We decide approach: likely reuse the same Node object and update its version/state to avoid needing to update dependents’ references. Thus NodeId remains stable for a given module across rebuilds within one process execution. Across separate runs, NodeIds can differ but that’s fine.

**Integration & Usage:**  
- `SchemaNode` is the fundamental unit manipulated by the **Graph** (see `Graph.zig`). Graph will allocate and store SchemaNodes, manage their relationships, and use the functions above for updating states and versions.
- Execution logic in **Executor** will read from and write to SchemaNode fields (like setting state to Active, populating outputs). There may be helper methods in SchemaNode for some of these, but state changes might also be done via Graph to ensure consistency (e.g., Graph.markReady might set state).
- The node types defined allow the DAG engine to distinguish behavior in **Executor**. For example, Executor might switch on `node.type` to decide how to execute it (parse file vs run macro vs run test, etc.).
- The clear schema also supports **introspection**: by having explicit types and structured data, we can reflect on nodes for debugging or logging. Each node carries the information needed to identify what it represents (key, type) which is used in log messages or debugging output.
- **Plugins & Macros:** When a node represents a macro call or plugin task, it will tie back to the actual JS function to execute via some identifier:
  - For macro nodes, we might store additional info like a function name or pointer in an associated structure (since SchemaNode is meant to be general, such specifics might be in a separate map keyed by NodeId, or encoded in the `key`).
  - Similarly, a PluginTask node might carry which plugin and hook to run. This could be derived from context (e.g., onResolve vs onLoad and filters).
  - SchemaNode can have minimal info (type + key), with specifics handled during execution by looking up details from the environment.
- **Error handling:** If an error occurs during execution, the executor will set the node’s state to `Failed` and may attach an error message (perhaps in `output` as a special variant or a separate `error` field). SchemaNode doesn’t define `error` explicitly, but we can handle it via output or a global error reporter. The key integration point is that a Failed state will be checked by Graph/Executor so dependents won’t run expecting valid output.
- **Large DAG considerations:** If there are extremely many nodes (thousands), SchemaNode structures are lightweight (a few fields each). Memory usage should be manageable, but we ensure not to put excessive large data in each node (store large content externally). The design with `key` string for each node is convenient but duplicates possibly file path strings; we might optimize by interning strings or referencing a global canonical path string to avoid duplicates across nodes (e.g., many nodes might share the same directory path prefix).
- **Copy-on-write usage:** In a scenario where we fork the process for workers, the SchemaNode structures in memory would be duplicated on write but shared on read. Since workers likely won’t modify the master’s graph (they will have their own), this is fine. If a worker process wants to mark something dirty, it would either communicate back or operate on its copy – in our design, workers do not modify the master's graph, they just execute tasks assigned.

In summary, `SchemaNode.zig` provides the definitions for DAG nodes – the building blocks of the meta-dag system – capturing everything about each task’s identity, inputs, and results in a structured, introspectable way.

## src/meta_dag/Graph.zig

**Purpose & Scope:** Manages the collection of `SchemaNode` instances and the relationships (edges) between them. This is the **central DAG management component**: it offers APIs to create or query nodes, enforce DAG invariants, and perform dependency tracking and traversal. The Graph is responsible for building the dependency graph from entry points, maintaining maps for quick lookups, and supporting operations like topological ordering, marking nodes ready for execution, and incremental invalidation. Essentially, `Graph.zig` is the substrate that connects nodes into a coherent DAG structure and ensures the rules of the DAG are maintained throughout modifications and execution.

**Core Data Structures:**  
- **`struct MetaDag`** (Graph): The container for all nodes and their connectivity. Key fields:
  - `nodes: ArrayList(SchemaNode)` – A dynamic array of all nodes in the graph. Each node’s `id` can be its index in this array. This allows iteration and random access by id. Using a contiguous list benefits cache locality when scanning nodes.
  - `nodeIndexMap: HashMap<[]const u8, NodeId>` – Maps a node’s unique key to its NodeId. This is used to ensure no duplicate nodes representing the same logical resource are created, and to quickly retrieve an existing node by key. For example, if a module file path is encountered again, we find the existing node instead of adding a new one.
  - `adjacency: ArrayList([]NodeId)` – (Optional auxiliary structure) Parallel to `nodes`, this could store each node’s dependency list as NodeIds for faster traversal without pointer chasing. We might maintain adjacency solely via each SchemaNode’s `deps` field; an explicit adjacency list is conceptually redundant but can be useful for certain algorithms or serialization.
  - `reverseDeps: HashMap<NodeId, ArrayList(NodeId)>` – (Optional) A mapping from a node to the list of NodeIds that depend on it. This can accelerate marking nodes dirty or scheduling dependents once a node completes. We can compute reverse deps on the fly by scanning all nodes, but maintaining this map incrementally is more efficient.
  - `entryNodes: ArrayList(NodeId)` – The list of entry point node IDs (such as the top-level modules or tasks requested by the user). These nodes have no dependents outside the graph (they are roots of DAG). The build outputs or final results correspond to these nodes. This list helps identify what results to collect at the end and can also serve as starting points for scheduling (though their execution may depend on other nodes).
  - `dirtyNodes: ArrayList(NodeId)` – (Optional) A list or set of nodes currently marked dirty in incremental scenarios. This is used in watch mode to know which nodes (and their dependents) need rebuilding.
  - `readyQueue: Queue<NodeId>` – A thread-safe queue of nodes whose dependencies are all satisfied and are ready to execute. This works in tandem with the Executor: Graph enqueues nodes here when they become ready, and Executor threads dequeue for execution.
  - `options: BuildOptions` – Configuration context (could include things like whether to produce sourcemaps, target environment, macro enabled/disabled flag, etc. as needed by node processing logic).
  - `pluginManager: *PluginManager` – Reference to plugin/macro configuration (if needed for Graph to call plugin hooks directly; we may handle plugin via ProcessPool, but Graph might still need to know if any plugin is interested in certain file types to decide default loader).
  - (Any synchronization primitives needed, e.g., a mutex to protect data structures if multiple threads will modify Graph concurrently. Many Graph operations happen before execution starts or in a controlled single-thread phase, but marking completion and adding new nodes dynamically might need thread safety.)

**Key Methods (Public API):**  
- **Graph Initialization & Finalization:**
  - `fn init(options: BuildOptions) MetaDag` – Creates a new Graph instance. Initializes internal arrays and maps (e.g., reserve capacity if known approximate node count to reduce reallocation). It also might take a reference to global plugin/macro data or create its own plugin manager. If incremental builds are enabled and a cache exists, this function (or a subsequent loadCache call) could pre-populate known nodes/versions.
  - `fn free(self: *MetaDag)` – Cleans up the graph, releasing memory. This would be called if the graph is destroyed (e.g., end of build in a one-off scenario). In watch mode, the Graph persists, so this might not be used until the process exits.

- **Node Creation & Lookup:**
  - `fn ensureNode(self: *MetaDag, type: NodeType, key: []const u8) NodeId` – Retrieves the NodeId for a given `key`, creating a new SchemaNode if it doesn’t exist. It handles insertion into `nodeIndexMap` and `nodes` list. The new node is initialized with no dependencies yet; dependencies can be added via `addDependency` or during graph construction routines. Returns the NodeId of existing or new node.
  - `fn addDependency(self: *MetaDag, nodeId: NodeId, depId: NodeId) void` – Adds a directed edge in the DAG: node `nodeId` depends on node `depId`. Updates the `deps` of `nodeId` (and adjacency list if used) and optionally updates reverseDeps for `depId`. It will increment a counter of how many dependencies `nodeId` has (or the node’s own count of unresolved deps). If this edge would introduce a cycle, this function should detect it (for example, if `depId` is an ancestor of `nodeId`). Cycle detection can be done via a DFS or by checking if `depId` already depends on `nodeId` (using reverseDeps). If a cycle is found:
    * If it’s a *module import cycle* (common in JS), we log it but do not treat it as fatal – those cycles are handled by execution semantics rather than graph structure (we avoid infinite loops in scheduling by not increasing dependency count for the second edge of a cycle, effectively breaking the cycle in the scheduling sense).
    * If it’s an invalid cycle (like a plugin task depending on something that eventually depends back on it), we throw an error.
  - `fn findNode(self: *MetaDag, key: []const u8) ?NodeId` – Looks up an existing node by key. Returns `null` if not found. Useful for checking if a dependency is already created.
  - `fn getNode(self: *MetaDag, id: NodeId) *SchemaNode` – Returns a mutable pointer to the SchemaNode with the given id. (This is used by Executor to access node details.)

- **Dependency Resolution & Graph Building:**
  - `fn resolveImport(self: *MetaDag, spec: []const u8, importer: NodeId) !NodeId` – Resolves an import specifier in the context of an importer module. This function integrates with Bun’s module resolution and plugin system:
    * It first invokes any **plugin onResolve** callbacks [oai_citation:49‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=type%20PluginBuilder%20%3D%20,void%3B%20onLoad%3A) via the Plugin system. If using a separate plugin process, this will communicate with it (e.g., by delegating to ProcessPool – see below). If a plugin provides a resolution (e.g., a new path or a namespace), use that.
    * If no plugin handles it, perform default resolution (file system lookup, Node.js-style resolution for packages, etc., using Bun’s resolver logic).
    * The result is a fully qualified module path (and possibly a `namespace` or special loader type). For example, `"./foo.ts"` might resolve to `"/abs/path/foo.ts"` with namespace `"file"`. A bare spec like `"react"` might resolve to some `react/index.js` in `node_modules`.
    * Check if a node for that resolved path+namespace exists via `findNode`. If not, create one with appropriate NodeType:
      - If namespace indicates special handling (like `"file"` vs `"json"` vs `"yaml"`), we might mark node type or store loader info. Possibly we set NodeType based on expected processing (e.g., `.json` files might directly be data and not require transpilation node).
      - Otherwise, default is a `FileModule` node for source files.
    * Link the importer to this dependency via `addDependency(importer, depId)`.
    * Return the NodeId of the resolved dependency.
    * Error handling: If resolution fails (file not found or ambiguous), return an error (which will propagate and likely abort the build).
    * This function ensures that the Graph reflects all module import relationships by adding nodes and edges as necessary. It may be called during the initial graph build for entry points and during parsing of modules when new imports are discovered.
  - `fn loadContents(self: *MetaDag, nodeId: NodeId) !void` – Loads the content for a given node if applicable. For a `FileModule`, this reads the file from disk (using Bun’s FS APIs or Zig I/O), possibly asynchronously. For other node types that need initial data (like maybe a `DataFile` node for JSON), it similarly loads the raw content.
    * If a **plugin onLoad** is registered for this file’s type/namespace [oai_citation:50‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=%29%20%3D,void), call the onLoad via plugin system (likely using ProcessPool). If the plugin returns `contents` or an alternate `loader` type, use that content or create appropriate transformed node.
    * If no plugin handles it, perform default loading: read the file bytes. The raw text (or binary for e.g. images) can be stored in the node’s output or passed along to a subsequent transform step.
    * After content is loaded, if the node represents source code (JS/TS), the Graph might immediately schedule its parsing/transformation. We have two design choices:
      - **Option 1:** The `FileModule` node when executed will itself handle parsing/transpiling (meaning that after load, the node’s task is complete if we treat it as just reading file, but not really – we need parse). More straightforward is to treat the `FileModule` node as encompassing "read + parse + transform". In that case, `loadContents` will not fully execute parsing; instead, execution in Executor will do parse after calling file read. So `loadContents` might simply be part of that execution, not a separate Graph step.
      - **Option 2:** Split into two nodes: one for reading file (producing raw content), and one for parsing/transpiling (producing final code). The read node would be a dependency of the parse node. This is granular but might be overkill unless we want to cache raw reads separately.
      - We lean towards combining file read and parse in one execution for simplicity. Thus `loadContents` might just be a helper used inside Executor when executing a file node.
    * If the file is binary or handled by a custom loader, we might create a Virtual node or store the content in output directly without further processing.
    * Note: The plugin onLoad could conceivably provide not just content but also an `exports` object (as Bun’s plugin API allows returning `exports` for virtual modules). We must handle that if needed by populating node’s output accordingly (e.g., a JS module that exports certain data).
    * Errors (file not found, read error) should set node to Failed with a descriptive error.
  - `fn addEntryPoint(self: *MetaDag, path: []const u8) !NodeId` – Adds a given entry file to the graph. This will ensure a node exists for it (likely a `TranspiledModule` or `FileModule` node) and mark it as an entry (store in `entryNodes`). It will then trigger loading and resolution for that node’s imports:
    * Potentially by calling `resolveImport` for each import in the file (which requires parsing the file to find import statements – so this ties into actually executing that node).
    * Perhaps the Coordinator/Executor handles parsing after scheduling the entry node, so `addEntryPoint` might just set up the node and not delve into its imports immediately.
    * Alternatively, perform a quick parse just to collect top-level imports (like a scan for import strings) to build out the graph eagerly. Bun’s existing bundler likely does parse to discover the graph synchronously. We could mimic that to construct the graph fully before executing any code (except macros).
    * We should ensure that all dependencies are added to the graph either upfront or during execution in a controlled way. The approach can be hybrid: parse entry synchronously to get the initial set of modules, add them, then allow Executor to parse others as they come.
  - `fn markNodeReady(self: *MetaDag, id: NodeId) void` – Marks a node as ready for execution (meaning all its dependencies are completed or it has zero dependencies). This pushes the node into the `readyQueue`. It should be called when:
    * A node is added with no dependencies (like an entry with no imports, or a resource like a CSS file loaded by plugin).
    * Or when a node’s last dependency finishes (Graph will track counts).
    * It will set the node’s state to `Pending` (if not already) indicating it’s queued.
    * If the node was up-to-date (and we decided to skip it), we might directly mark it Completed instead of queuing; in such case, we would immediately call `markDependencyComplete` for its dependents rather than executing it.
  - `fn markDependencyComplete(self: *MetaDag, depId: NodeId, success: bool) void` – Called by Executor (or ProcessPool) when a node finishes execution (or is deemed finished). This notifies all dependents of `depId` that one of their requirements is done:
    * The Graph can decrement an internal counter of how many dependencies each dependent still has pending. (We might maintain `SchemaNode.dependentsCount` as the number of unfinished deps.)
    * For each dependent node of `depId`, decrement its unresolved dep count. If it becomes zero and if `success` is true, call `markNodeReady(dependentId)`.
    * If `success` is false (meaning `depId` failed), mark the dependent as failed or at least do not schedule it. Possibly we propagate failure to all transitively dependent nodes to avoid spurious waiting.
    * If we allow partial builds on failure (like in test runs continue other tests), we might not propagate failure beyond marking those nodes unexecutable.
  - `fn markDirty(self: *MetaDag, id: NodeId) void` – Marks a node (and by necessity its dependents) as dirty for incremental rebuild. This will:
    * Set the node’s state to `Pending`/dirty and clear any cached output.
    * For each node that depends on this node (reverseDeps), if they are not already dirty, mark them dirty as well (recursively).
    * Optionally, add these to `dirtyNodes` list for tracking.
    * This does not immediately enqueue anything; it just invalidates. In watch mode, after marking dirty, Coordinator would call something like `scheduleDirtyNodes()` to actually re-execute them.
    * Ensures that entryNodes or any root will be eventually re-run if needed because of this invalidation.
  - `fn topologicalOrder(self: *MetaDag) []NodeId` – (Optional utility) Returns the NodeIds sorted in a topological order (each node comes after its dependencies). Useful for certain analyses or for consistent output ordering. Not used in scheduling (we use dynamic scheduling), but could be used at end to output modules in a deterministic order in bundle file. If there are cycles, this can group strongly connected nodes or break ties by key order to have stable ordering.

**Integration with Plugins & Macros:**  
- The Graph orchestrates calls to the plugin system as needed during graph construction:
  - **Plugin onResolve:** handled in `resolveImport`. This function uses Bun’s plugin infrastructure. In implementation, it might package the `spec` and importer info and send to the plugin ProcessPool, which returns a resolution (path/namespace) or null if not handled. If a resolution is returned, Graph uses it; if not, does default. By abstracting plugin calls here, the rest of graph building remains the same regardless of plugin presence.
  - **Plugin onLoad:** handled in `loadContents` (or in Executor’s execution of a file node). Similarly, Graph/Executor will call the plugin to get file content or transformation. For example, if importing a `.scss` file, a plugin might provide compiled CSS content and a flag that no further processing is needed. The Graph could then create a Virtual node representing that CSS text if needed, or store it as the output of the `.scss` file node.
  - The Graph holds a reference to plugin configurations (`pluginManager`). This contains the filters (regex) and callbacks. If using a plugin worker process, the Graph might simply always defer to the worker instead of matching filters in Zig. **Alternatively**, for performance, Graph could check `pluginManager` filters to decide if a file likely has a plugin handling it, to shortcut certain flows.
  - **Macro integration:** Macros (imported with `type: "macro"`) cause special node creation:
    * When Graph processes an import with attribute `macro`, it knows the imported module is a macro definition. The Graph will mark that imported module’s node (the macro file) as a normal module (it’s just code that will run at build time, but we treat it as a module to load).
    * Additionally, each call to the macro function in some importing file must produce a `MacroInvoke` node. How do we detect macro calls? During parsing of the importer file, when encountering a function call to an imported symbol that is flagged as macro, the parser/transformer will pause and inform the Graph (or directly create a MacroInvoke node):
      - Possibly the Executor’s parse phase will call a Graph method like `registerMacroCall(importerId, macroModuleId, macroName)` which creates a MacroInvoke node (key could be e.g. `"macroCall:${macroModuleId}:${callSiteLocation}"` to uniquely identify).
      - The MacroInvoke node will depend on the macro’s module node (ensuring the macro code is loaded) and perhaps on nothing else. The importer’s TranspiledModule node will then depend on that MacroInvoke node (so it waits for macro result).
    * This means the importer’s node will not be marked ready until macro nodes complete. The macro nodes, once ready (their only dep is macro module code, which is likely loaded quickly), will execute (running the actual JS function) and produce a result.
    * The result is then inlined: The Executor, after macro node completion, will substitute the call in the AST with the returned value. The mechanism could be:
      - The MacroInvoke node’s output holds the literal result (as text or AST node).
      - The importer’s parse/transpile function checks for macro nodes outputs and replaces call sites.
      - Or simpler, the importer’s code generation step fetches those outputs and uses them.
    * The Graph needs to ensure no duplication: if the same macro function is called in 10 places, should that be one MacroInvoke node or 10? Likely each call (with potentially different arguments) is a separate execution, so 10 nodes. If a macro is pure and identical calls could reuse result, that’s an optimization not currently needed; simpler is treat each call independently.
    * Graph might assign each macro call node an ID at parse time. We must incorporate that nodes can be added *after* initial graph construction (since macro calls are discovered while parsing entry modules). Our design allows dynamic addition: Graph functions like `ensureNode` can be called from Executor thread, but we must lock for thread safety when modifying structures at runtime. We will handle that concurrency by locking or scheduling addition on main thread. Possibly we parse in main thread to avoid concurrent modification.
    * Macros cannot cause infinite recursion if they generate imports, because those would be new nodes but eventually bottom out (though a macro could theoretically write a file that imports another macro, etc.; the DAG can expand but as long as no cycles, it's fine).
    * **No regression:** Bun’s macros presently execute at bundle-time and inline the result [oai_citation:51‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=Macros%20are%20a%20mechanism%20for,directly%20inlined%20into%20your%20bundle); our system does the same. They are executed in parallel if possible (and our DAG indeed allows concurrent MacroInvoke nodes, matching Bun’s note that bundle-time code is automatically parallelized [oai_citation:52‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=When%20to%20use%20macros)).
  - Graph must enforce macro restrictions:
    * If a macro import is from `node_modules`, we must error as Bun does (for security) [oai_citation:53‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=). Graph can check the path of macro module nodes and, if it matches `node_modules/*`, refuse or mark as invalid.
    * If macros are disabled by `--no-macros`, the Graph/Coordinator should not create MacroInvoke nodes at all; instead, encountering a macro import or call should produce an error node that fails the build.

- **Ensuring No Regression:** The Graph’s plugin and macro integration replicates the behavior of Bun’s current bundler:
  - All plugin hooks fire in the correct order and timing. For example, `onStart` is handled by Coordinator before Graph building, `onResolve` happens during dependency resolution, and `onLoad` during file loading. The results of plugins are applied exactly as they would be in legacy bundler (just that now the Graph coordinates it).
  - Macros produce the same build output as before, just scheduled via DAG. If a macro throws or fails, we output the same error and stop build (unless `--no-macros` which would have stopped earlier).
  - The Graph does not break any existing feature: e.g., tree-shaking (if Bun bundler had any) would still be possible (though Bun's bundler might not do aggressive dead-code elimination yet).
  - Source map generation: The Graph will carry through source map data. Possibly each transpiled node can carry mapping info which later is combined. We need to ensure that still works (this might rely on reusing esbuild's logic which we can still do, but feeding it source content via DAG).
  - Build performance: A slight overhead in managing the graph is expected, but the concurrency and incremental improvements should outweigh it. We will ensure Graph operations (hash maps, etc.) are efficient (maybe preallocate given we know number of files roughly from module resolution).

**Concurrency and Thread Safety:**  
- The Graph is constructed mostly in a single thread (Coordinator) initially. However, dynamic additions (like macro nodes discovered during parse, or a plugin that triggers adding a virtual module on the fly) could happen on an Executor thread. We therefore implement thread-safe modifications:
  - Use a mutex or lock around operations that mutate `nodes`, `nodeIndexMap`, etc. For example, `ensureNode` and `addDependency` must lock if called from potentially multiple threads. Alternatively, we funnel all graph modifications to one thread (e.g., Executor signals the main thread to add a node). But locking is simpler and given addition is not extremely frequent, overhead is minimal.
  - Reading from Graph (like dependency lists) happens concurrently in Executor threads. The `deps` slice in each SchemaNode is mostly read-only after initial creation (except when building it up, which we do before execution of that node begins typically). We ensure that once a node is being executed, its `deps` list is complete and not modified further.
  - When a node completes, updating dependents’ counters (`dependentsCount` or reverseDeps) may involve atomic operations or locks. We can maintain a separate atomic counter of pending deps for each node (initially set to number of deps). Each `markDependencyComplete` does an atomic decrement on dependents’ counters and checks for zero, making it thread-safe without global locks.
  - The readyQueue needs to be thread-safe (multiple threads will pop from it). We can use a lock or a lock-free queue for this. Zig’s std might offer a channel/queue or we implement a simple mutex-protected queue since contention should be low (threads block if empty anyway).
  - The Graph data (nodes list, maps) can grow, but maximum size is number of tasks in build (bounded by number of files + macros + plugin tasks). This could be a few thousand in large projects, which is fine. Memory usage for graph overhead is negligible compared to source code and compiled code sizes.

**Edge Case Handling & Fallbacks:**  
- **Module Resolution Failures:** If `resolveImport` cannot find a module (no file and no plugin provided one), Graph will mark the importer node as failed (most likely by creating a dummy node for the missing import, with an error state). The Coordinator or Executor will then report an error like "Module not found: '<spec>' imported by <importer file>". This stops the build (in bundling).
- **Missing Entry:** If an entry file path doesn’t exist, Graph should error out early.
- **Circular Dependencies:** Graph detection of cycles ensures we don’t infinite-loop building the graph. For legitimate JS cycles:
  - Example: A -> B, B -> A. We will create Node(A), Node(B). When adding dep A->B, fine. When adding dep B->A, detect that A is an ancestor; we then handle by *not* increasing B’s dep count for A (so that B can execute even though A isn’t fully done). We might still record the edge for reference or skip it entirely. Essentially we break the cycle by saying one direction is a soft dependency.
  - Execution: One of them will execute first (say A started, then during A's execution it triggers B, B sees A is in progress and doesn't wait). This mimics Node’s module loading (B will get a partially initialized A). We must ensure that in bundling output, such cycles are allowed (they usually are as bundlers wrap modules and allow runtime cyclic loading).
  - Graph-wise, we might mark the cycle edge specially (so introspection can show "cycle between A and B"). The invariant of DAG is slightly violated but we manage it.
  - If a cycle occurs in tasks that cannot be resolved by runtime semantics (e.g., a plugin task that somehow depends on its own output), we treat that as an error to avoid deadlock.
- **Macro recursion:** If a macro indirectly imports itself or calls itself recursively (via generating code that triggers itself), this could lead to cycle too. We should detect if a MacroInvoke node tries to invoke a function that is already in call stack. Possibly limit macro recursion by runtime check (like if macro calls itself, it just recurses as normal code until maybe stack overflow). It's more a user issue than graph – graph would treat each call separately.
- **Large Projects:** In extremely large projects, initial graph construction could be heavy if we parse everything upfront. Our approach can build incrementally as needed. If memory becomes an issue, we could release node outputs for modules that are not needed until linking (but that’s micro-optimization).
- **Fallback to Non-DAG:** We might include an internal escape hatch to use the old bundler if `BUN_NO_DAG` env is set, for debugging. This would bypass Graph entirely. This is only for emergency and not part of normal operation, so not documented to users.
- **Incorrect Plugin Behavior:** If a plugin resolves an import to a path that doesn’t actually exist or returns malformed content, the Graph might treat it as provided. For example, plugin onResolve could generate a fake path "virtual:xyz". We handle that by creating a node for "virtual:xyz". Later, either a plugin onLoad will provide contents for it, or if none, it might error "Failed to load virtual:xyz". Essentially, we trust plugin outputs and only error if it ultimately leads to no content.
- **Concurrency Hazards:** Avoid double-adding nodes: using `nodeIndexMap` ensures we don’t create two nodes for same file. But consider two threads simultaneously resolving the same module from two different imports:
  - Thread1 and Thread2 both call `ensureNode("foo.ts")` at same time. Without lock, they could both not find it and then both create, duplicating. Hence ensureNode must lock the map or use atomic insertion. We'll implement it with a lock around the lookup-add sequence. The overhead is negligible relative to file I/O.
- **Consistent Ordering:** To ensure deterministic builds (for hashing or output), where nondeterminism might creep in (like iterating HashMap which is random order), we impose ordering when needed. For instance, when outputting multiple entry bundles, sort by entry name. Or ensure that if two independent tasks complete in different orders, final output is unaffected. The DAG inherently should yield same final artifacts regardless of execution order if no races.

**Integration with Other Components:**  
- **Executor:** Graph cooperates closely with Executor. The Graph produces ready tasks and receives completion notifications. Executor will call `getNode` to fetch node data to execute, then call `markDependencyComplete` when done. Graph’s thread-safe queue and counters are core to this interplay.
- **Coordinator:** Uses Graph APIs to build the graph (adding entries, triggering resolution) and to reset it for incremental builds. Coordinator also queries Graph for entryNodes outputs after build, and uses Graph to mark things dirty on file changes.
- **Versioning:** Graph calls Versioning to check if a node can be skipped (e.g., at node creation or just before execution, Graph/Executor uses `isUpToDate` with cache). Graph supplies the content to Versioning for hashing and uses cache results to decide skip. Graph might also update node.version from cached info without reading file (e.g., if cache says last build version of file).
- **Introspection:** Graph provides data to introspection functions. For example, Introspection may call `dumpGraph(graph)` which iterates `graph.nodes` and uses `SchemaNode.describe()`. Or it might traverse adjacency to print dependency chains. The Graph may offer methods to help (like a topological sort or a DFS from entry for visualization).
- **ProcessPool:** The Graph doesn’t directly know about ProcessPool, but its methods (resolveImport, loadContents) will call out to plugin system which might use ProcessPool behind the scenes. We document that interplay in Plugin and ProcessPool sections.
- **Existing Bun Infrastructure:** 
  - Bun’s module resolver (likely written in Zig already for Node compatibility) will be called in `resolveImport` for default cases. We reuse that code rather than rewriting resolution logic.
  - Bun’s internal representation of modules (if any existed outside bundler) might not be needed; Graph supplants it for bundling. But at runtime, Bun might still have a separate module loader (for `bun run`). We are not replacing the runtime module loader, but we are enhancing it: e.g., it could optionally query Graph’s cache or reused compile outputs if available.
  - Watch mode integration: Bun’s file-watching utility (probably in Zig or calling OS notify) needs to call our Graph.markDirty. We integrate via Coordinator managing watchers but Graph provides the dirty marking and incremental update logic.

In summary, `Graph.zig` is the backbone of `bun-meta-dag`: it houses the DAG data and ensures all relationships and states are consistently managed. By providing robust methods for node addition, lookup, resolution, and state changes (with full plugin and macro support), it guarantees that the engine has a correct model of the build, which the Executor can then operate on to perform the actual work.

## src/meta_dag/Executor.zig

**Purpose & Scope:** Oversees the execution of nodes in the DAG, scheduling and running tasks while respecting dependencies and maximizing parallelism [oai_citation:54‡bugfree.ai](https://www.bugfree.ai/knowledge-hub/designing-a-dag-based-workflow-engine-from-scratch#:~:text=3,execution%20in%20parallel%20where%20possible). The Executor is effectively the **scheduler and worker controller** for the DAG: it pulls ready nodes from the Graph, dispatches their work to threads or processes, and reports back completion or errors. It handles thread pool management, task queueing, and implements special logic for executing each type of node (file parsing, macro evaluation, etc.). Additionally, it coordinates with the ProcessPool for tasks that need to run in separate processes (like plugin code or tests), making the DAG execution seamless across threads and processes.

**Key Components:**
- **Thread Pool:** A pool of Zig threads that perform node executions. The size is typically set to the number of CPU cores or a user configuration. These threads are launched at Executor initialization and wait for tasks to run. Using persistent threads avoids creation overhead for each task.
- **Task Queue:** A concurrent queue of NodeIds representing tasks ready to be executed. This is fed by Graph’s `readyQueue` (Graph may directly provide a thread-safe queue). Workers will continuously take NodeIds from this queue when available.
- **Synchronization Mechanisms:** 
  - A condition variable or semaphore that workers wait on when the queue is empty, and that Graph signals when new tasks become ready.
  - Atomic counters to track how many tasks are in progress, mainly to know when all work is done.
  - Possibly a flag to indicate cancellation (if an error should stop further scheduling).
- **Task Execution Logic:** 
  - A central `executeNode(nodeId: NodeId)` function that determines how to run the task for that node. It will inspect the node’s type and call the appropriate handler (e.g., `executeFileModule`, `executeMacroInvoke`, `executeTestTask`, etc.).
  - Each handler function performs the actual work and uses other parts of Bun (parsing, compiling, running JS) to get the job done.
- **Integration with ProcessPool:** The Executor delegates certain tasks to the ProcessPool. For example, if executing a `PluginTask` node or a `TestTask` node, it will call the ProcessPool’s submit function and wait for the result rather than doing work on the current thread.
- **Error Handling:** The Executor is responsible for catching any errors thrown during task execution and marking the node as failed, then potentially signalling a build abort. It ensures that exceptions (like a thrown Zig error or a JS exception in macro) don’t crash the entire program but are caught and converted to a failure state for that node.

**Life Cycle Methods:**
- `fn init(graph: *MetaDag, numThreads: usize) Executor` – Initializes the executor with a reference to the Graph. It spawns `numThreads` worker threads, each running an internal loop waiting for tasks. It also sets up any necessary thread-local state (for instance, if each thread needs an isolated JS context for macros – though we might not use thread-local JS, but it’s a consideration).
- `fn start(self: *Executor) void` – Begins the execution loop. In a simple design, this could just signal threads that tasks may be available. In our design, tasks may already be queued by Graph as soon as it finds ready nodes (like entry points with no deps). So threads might actually start working as soon as they are spawned. We may not need a separate `start` if `init` handles thread creation and they wait on the queue immediately.
- `fn waitForCompletion(self: *Executor) !void` – Blocks until all tasks have been executed (or until an error triggers abort). This could be implemented by waiting on a condition variable that is signaled when the active task count goes to zero. It returns an error if the build was aborted due to a failure, or `void` if all tasks succeeded.
- `fn shutdown(self: *Executor) void` – Gracefully shuts down the worker threads. It signals them to exit (for example, by pushing a special sentinel value in the queue or using an atomic flag and waking them). Then it joins all threads. This is called when the DAG execution is completely done (and not in watch mode, or at program exit in watch mode scenario).

**Worker Thread Loop:**
Each worker thread runs something like:
```zig
while (true) {
  nodeId = self.taskQueue.pop(); // blocking wait if empty
  if (nodeId == SHUTDOWN_SENTINEL) break;
  self.executeNode(nodeId);
}
```
During execution, the thread might also handle asynchronous waits:
- For instance, if `executeNode` requires performing file I/O, it could use synchronous I/O (the thread will block on file read, which is okay as other threads can run). Alternatively, we could use Zig’s async within the thread to allow other tasks on same thread during I/O, but since we have multiple threads, a blocking read is acceptable and simpler.
- If `executeNode` calls a ProcessPool task (which is essentially making an IPC request and waiting), the thread will block until the response arrives. This again is fine because other threads can continue doing other tasks.

**Execution of Different Node Types (Core Logic):**
- **FileModule / TranspiledModule (Source code parsing & compilation):** 
  - The executor reads the file content (if not already loaded). If not memory-mapped, use Bun’s high-performance file reader (possibly memory mapping large files or using libuv’s threadpool if it were Node; but Bun likely does direct I/O).
  - If content was cached and node was marked up-to-date with output cached, we can skip parsing entirely: just load the cached output (e.g., a precompiled artifact) and mark node Completed. Executor should handle this: check `isUpToDate` before heavy work. The Graph/Coordinator could mark node as skipped earlier, but a double-check here ensures we don't waste time.
  - If not skipped, parse the file. Bun’s parser (from its transpiler, based on esbuild) can be invoked. Likely Bun has a function `parseFileToAST(source: []u8, options) -> AST`. Use it to get an AST or intermediate representation.
  - While parsing, collect import declarations. For each import, call `graph.resolveImport(spec, currentNodeId)` to ensure that dependency node exists. This might create new nodes. **Important:** Doing this while parsing in a worker thread means Graph is being modified concurrently. We must protect `resolveImport` with locks as discussed. Alternatively, Bun’s parser might allow a callback on each import string; we could accumulate them and resolve after parsing is done to minimize lock contention.
  - Also, identify macro imports (import attributes) and macro calls:
    * If an import is a macro type, we mark that dependency node specially (maybe store in SchemaNode a flag or separate list of macro-providing modules).
    * When encountering a call expression, check if the callee is an identifier referring to an imported macro. If yes, pause or handle:
      - We likely cannot fully evaluate without executing macro, so instead, create a placeholder in AST and schedule a MacroInvoke node. 
      - The Executor at this point calls something like `graph.ensureNode(.MacroInvoke, key="macro:${moduleId}:${callSiteId}")` to get a MacroInvoke node. It adds dependency: MacroInvoke depends on macro module node (which ensures the macro code is loaded).
      - It also adds dependency: current file’s node depends on this MacroInvoke (so current file will not complete until macro is done). To implement that mid-execution, we might need to mark current node waiting and yield this thread until macro completes. However, that’s complex to coordinate within a single thread.
      - Simpler approach: do a two-phase execution for modules with macros:
         1. Parse the file and collect macro call info.
         2. Pause the file’s compilation, schedule all MacroInvoke nodes found.
         3. Those macro nodes run (possibly on other threads).
         4. Once macros are done, resume the file’s code generation using the macro results.
      - Implementation: The Executor can detect after parsing that macro nodes were created. It then does not mark the file node Completed yet; instead, it sets it aside until macros finish. Perhaps set state to `PendingMacro` and notifies Graph that its dependencies now include those macro nodes (so it won't be considered ready until they finish).
      - Another thread (or possibly the same) will eventually get to execute the macro nodes. When they finish, Graph will mark the file node as ready again. Then a thread (maybe the same or another) will pick up the file node for the second phase.
      - We need to preserve the partially parsed AST through this period. So store AST in node’s output or a side structure. The SchemaNode for file could have a field pointing to an AST or intermediate representation. Alternatively, we could serialize the AST, but better to keep it in memory.
      - Then second phase: generate code (transpile to target JS, apply macro outputs by replacing AST nodes). Use esbuild’s print or Bun’s codegen to get the final JS (and source map).
      - Mark node Completed with the final code as output.
      - This approach is complex but ensures macros inline correctly. (This is effectively how a traditional compile would handle macro-like features by pausing until macro values known).
      - If macros do not produce code but e.g. produce data for an AST transform, similar handling.
  - If no macros, simpler: we parse and directly generate output.
  - After generating code, possibly run other transforms: e.g., minification if target demands, or apply Babel transforms if needed (Bun likely doesn’t except polyfilling depending on target).
  - Set node output (the final code string or Blob).
  - Notify Graph via `markDependencyComplete(nodeId, true)`.
- **MacroInvoke (Macro function execution):** 
  - The executor ensures the macro’s module is loaded (that module node will likely have executed just like any other file, but note: the macro module is not included in the final bundle output, it's only used at build time).
  - To execute the macro function: We need a JS environment. Bun’s main thread has a JavaScriptCore VM for the runtime. We have options:
    * Execute macro in the main JS thread context. But our bundler is running possibly in the same process that *is* the JS runtime for Bun, so we have JSC available. However, executing user code in the main thread might block it. But since bundler runs as part of CLI, maybe it's fine to use main thread for macro execution.
    * We could create a separate JS context on a separate thread for macro, but sharing JSC across threads might not be possible without locking.
    * Use Bun’s **Workers** internally: spawn a Bun Worker thread for each macro call. That seems heavy for each call.
    * However, Bun’s docs say macros are automatically parallelized [oai_citation:55‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=When%20to%20use%20macros), implying they might already run concurrently perhaps in multiple threads. Possibly Bun might spin a thread pool of JS contexts for macro execution.
    * We can similarly maintain a small pool of JS execution contexts for macros. For instance, allocate N JS virtual machines (maybe using JSC API to create multiple VM instances) and have macro calls assigned to them.
    * Simpler initial approach: run macro calls sequentially but still benefit from parallel file parsing on other threads. But to truly parallelize macros, we need concurrency.
    * Maybe we can leverage the ProcessPool for macros as well: treat macro execution like plugin tasks and spawn a Bun process to run the macro function, then return result. But overhead per call might be too high unless macros are expensive individually.
    * Given time, we might choose to run macros on the main thread’s JS context synchronously. That means our Executor thread would have to somehow invoke JS on the main thread. This could be done by scheduling a task on the main thread’s event loop and waiting. This complicates things.
    * Alternatively, require that the bundler is running on the main thread (since it's a CLI, main thread might be idle except event loop), and do macros there. But we have multiple threads for files, hmm.
    * Perhaps the better path: treat macros similarly to plugin tasks and run them in the plugin worker process. The plugin worker, having loaded the macro’s module (since it can import modules too), could call the macro function and send back result. That way macros execute in a separate JS environment (which is essentially Bun’s JS environment) off the main thread.
    * For now, let's assume we will execute macro in the same ProcessPool as plugin (since conceptually, macro is like a build-time code execution which could be done in plugin style).
  - So `executeMacroInvoke` might:
    * Prepare a message with macro module path, function name, and possibly serialized arguments (if any, though macros typically use no dynamic arguments – they just run and produce something).
    * Send to ProcessPool (perhaps the pluginPool).
    * Wait for response which contains the return value (likely JSON-stringified or as a primitive).
    * On success, convert result to a form that can be inlined: e.g., if number or boolean, turn to string; if string and meant to be code, possibly keep as is (the macro might even return an AST or code snippet).
    * Store this in MacroInvoke node’s output.
    * Mark node Completed.
    * Graph will then wake any dependent (the module waiting to use this result).
  - If macro throws an exception or fails, catch that: record error (maybe include stack trace if available), mark node Failed. The dependent module will then also fail to build.
- **PluginTask (onResolve/onLoad):**
  - These nodes represent calls into plugin code. We will always delegate them to the plugin worker process:
    * For onResolve: send specifier and importer info, get back resolved path/namespace.
      - If result is provided, Graph uses it to create the resolved node (this likely happened within Graph.resolveImport itself, possibly not needing a separate node in DAG at all. Actually, we might not model onResolve as a node, because it’s a very quick synchronous step in bundling).
      - It might be overkill to have a node for each onResolve; instead, Graph.resolveImport directly uses ProcessPool and is synchronous from Graph’s perspective.
      - So maybe `PluginTask` nodes are more relevant for onLoad (which could be heavier, e.g., transpiling via plugin).
    * For onLoad: plugin might produce file contents. That can be seen as part of file loading, not a separate DAG step but an implementation detail.
      - Perhaps we don’t need explicit PluginTask nodes at all; the work is encapsulated in file processing.
      - Unless we want to reflect them in introspection. But since they don’t produce something that other nodes depend on (except the file node that requested it), they could be inlined.
    * We can choose not to represent plugin calls as separate nodes to keep DAG simpler. Instead, Executor just calls ProcessPool when needed. So we might not implement a NodeType for PluginTask, and thus `executePluginTask` might not be separate from executeFile’s flow.
  - If we did have plugin nodes (say for a plugin-generated virtual module that needs some processing), `executePluginTask` would call the plugin, get result, and set output (like the content of the virtual module). Then maybe create a new node for that content’s next stage (like parse it if it’s code).
  - We will keep it simple: No separate plugin nodes; treat plugin calls as part of file node execution.
- **TestTask:** 
  - A TestTask node signals running tests in a file or a group of tests. We will definitely use ProcessPool for this to isolate the test environment:
    * The Executor will call e.g. `processPool.submitTask({type: "runTest", file: testFilePath})`.
    * The worker process will import the test file and run the tests (likely using Bun’s built-in test runner logic to execute all tests in that file).
    * It then sends back a summary (e.g., number of tests, failures, etc., and possibly detailed failure info).
    * The Executor receives this and marks the node Completed (maybe regardless of test failures, we consider the task itself succeeded because it ran to completion; but we might propagate a failure state if any test failed depending on how we treat test failures).
    * Possibly we treat any failing test as a build failure to exit with non-zero code. Bun’s `bun test` likely exits non-zero if tests fail. So we might mark the TestTask node as **Completed** (it did execute) but carry the failure info. The Coordinator can then decide to set overall outcome as failed due to test failures.
    * The node’s output could include the test results so they can be aggregated for reporting.
  - If a test times out or crashes (worker process dies), ProcessPool will report an error or no response. The Executor then marks node Failed, which Coordinator will handle by reporting that test as crashed.
  - Running tests in parallel: We spawn multiple processes up to `N`. The ProcessPool handles distributing different TestTask nodes to different processes concurrently.
  - The Executor threads just block waiting for each test’s result, which is fine.

- **VirtualNode (if used for grouping or final output):**
  - Example: we might have a Virtual node that depends on all entry points and whose execution just collates outputs (like producing an index HTML that includes all bundles, or simply signaling build done).
  - If we implement one (not strictly necessary), `executeVirtual` could be a no-op or do the final packaging (e.g., if bundler needs to concatenate outputs, this could be done here).
  - Alternatively, the Coordinator after Executor finishes can handle final packaging. So VirtualNode may not be needed.

**Concurrency & Parallelism:**
- The Executor aims to keep all CPU cores busy. It does so by having multiple threads picking up ready tasks. Because many tasks (file parsing) are CPU-heavy, this scales well.
- Some tasks (I/O waits, process IPC waits) will block a thread. With enough threads and tasks, others will fill in. If many threads all block on I/O (e.g., reading large files from disk simultaneously), the OS will handle scheduling and throughput. It's usually fine; we might tune by not launching too many heavy I/O tasks exactly simultaneously (but likely not an issue).
- We must avoid oversubscription: e.g., if we spawn too many OS threads beyond cores, context switching overhead rises. Default to core count is safe.
- If some tasks are much heavier, they might become the critical path. That’s fine; as long as others run in parallel, we minimize idle time.
- Thread affinity: probably not needed, OS does fine.

**Error Handling & Aborting:**
- If any node fails (error in parsing, missing file, plugin throw, etc.), the default for bundler is to abort the whole build (since you can’t produce output with a missing module or syntax error). The Executor should detect a failure:
  - Possibly maintain an `Atomic(bool) encounteredError`. 
  - When a node fails, set this and also mark all pending tasks that are not started as cancelled (Graph can check this flag when scheduling new tasks).
  - We can simply let current tasks finish but prevent new ones from starting. One way: stop pulling from readyQueue if error flag is set.
  - Alternatively, as soon as fail happens, signal all threads to stop after current tasks (e.g., by pushing sentinel or by having them check the flag each loop iteration).
  - We do need to make sure dependents of the failed node aren’t executed; Graph.markDependencyComplete on failure would have avoided readying them anyway.
  - So it might suffice to not schedule anything new and let running tasks end. This is simpler and ensures we free resources correctly.
  - The Coordinator’s `waitForCompletion` would return an error state if encounteredError is true.
- In watch mode, a failure in one build does not stop the watch process. So we cannot kill threads; we just report error and wait for file changes to potentially fix it. In that scenario:
  - We mark the failing node and its dependents as Failed for that round. We might keep them in graph.
  - The Executor can drain remaining tasks (or we might cancel them to let a new build start sooner).
  - It might be okay to let them finish even if error occurred, but their results will be thrown away. Alternatively, as soon as error, we could flush the queue of any tasks whose outputs are now irrelevant. But determining irrelevance dynamically is complex.
  - For simplicity: in watch mode, we could still complete all ready tasks even after one fails, just to clear out the pipeline. The output is discarded anyway. Or we could break out early. Either is acceptable; early break might make the system responsive faster if user fixes the error quickly.
  - We’ll choose to abort further scheduling when one fails to be safe and potentially faster to next iteration.
- The Executor has to handle internal errors as well (if our code throws due to a bug). We try to catch all expected error cases and translate them to node failures, but unexpected errors should ideally propagate out and likely crash (though we don’t want that; maybe wrap executeNode in a top-level `catch |err| { log and set node Failed; }` to handle Zig errors).
- When shutting down (normal completion or user abort via Ctrl-C), ensure to join threads and kill any process pool workers.

**Performance Considerations:**
- **Parsing/Transpiling Efficiency:** Using Bun’s optimized transpiler (written in Zig/C++) ensures very fast parse/emit. Running multiple in parallel scales linearly until memory or I/O bandwidth becomes bottleneck.
- **Memory usage:** Each thread will use memory for ASTs and such concurrently. Ensure we don’t exhaust memory on huge projects by maybe limiting concurrency if needed (not likely unless dozens of threads on a low-memory machine).
- **Thread pool overhead:** negligible compared to heavy tasks, and better than spawning threads per task.
- **Lock contention:** The main contended locks are the readyQueue and maybe some Graph locks when adding nodes. Usually tasks like parse will consume far more time than those locks, so contention is low. Also atomic operations for counters are quick.
- **Process communication overhead:** Using ProcessPool for plugin macros and tests has overhead (serialization, context switch). But these tasks are inherently possibly slow (running JS, running tests), so overhead is small relative. We keep communications minimal and ideally synchronous to simplify.
- **Balancing CPU and I/O:** If all threads are CPU-bound, I/O might queue, or vice versa. But since reading files is usually fast relative to compilation, and we do it as part of tasks, we likely saturate CPU more. It's fine.
- For extremely I/O heavy tasks (like reading thousands of small files), maybe reading asynchronously on a single thread could be considered. But since OS can cache disk reads and we can parallelize them, we likely see near best performance anyway.

**Integration with Logging/Tracing:**  
- The Executor will call **Introspection** hooks (if enabled) at important events:
  - Before executing a node, log “START Node X (type, key)”.
  - After finishing, log “COMPLETE Node X in Y ms”.
  - If error, log “FAIL Node X: [error]”.
  - These calls likely wrap `executeNode`. We gather a timestamp before and after to compute duration.
  - Also possibly log queueing: when Graph marks ready, could log “READY Node X”.
- If running with verbose flag, these logs will be printed by Introspection (with appropriate synchronization to not garble output).
- The overhead of logging is kept minimal when off (just a flag check).
- The Executor can collect timing data: perhaps keep a counter of total execution time per type of node, number of nodes executed, etc., which Introspection can use to output stats.

**Integration with Coordinator:**  
- Coordinator is the one to call `Executor.init`, then feed initial tasks (if Graph has some ready at start) or simply signal threads that they can start pulling from Graph’s readyQueue. (One approach: Graph’s readyQueue is populated as entries are added, so even before Executor starts, some might be waiting. That's okay.)
- Coordinator then calls `executor.waitForCompletion()`.
- If in watch mode, Coordinator might not destroy Executor after one build; it could reuse the same threads for subsequent builds (to avoid overhead of thread creation each time). That means we need to be able to reuse Executor:
  - Possibly keep threads alive, but idle between builds. We must handle that tasks from a new build will be enqueued and threads woken. It’s doable.
  - Alternatively, stop threads after each build and recreate for next – but that adds overhead, albeit maybe minor relative to build time. Reuse is nicer.
  - We'll assume reuse: so after one build, we can reuse the thread pool. Just ensure any global error flag is reset, and any lingering tasks are cleared (should be none).
- Coordinator handles process signals (like SIGINT). If user interrupts, we might want to abort current tasks promptly:
  - Possibly call `executor.cancel()` which sets a flag and wakes threads (so they check and break).
  - Also instruct ProcessPool to kill workers.
  - Then exit.
  - If not implementing explicitly, a Ctrl-C will kill entire process anyway.

**Summary:** The Executor is the runtime workhorse of the DAG engine. It ensures that all scheduled tasks (parsing files, running macros, etc.) are executed efficiently using all available cores and that the results are fed back into the graph for dependency management. It abstracts away whether a task runs locally or in another process, making those differences transparent to the Graph and higher-level logic. By carefully managing thread concurrency and synchronization with the Graph, it achieves parallel execution while preserving the dependency constraints of the DAG.

## src/meta_dag/Coordinator.zig

**Purpose & Scope:** The Coordinator is the high-level orchestrator that ties the DAG engine into Bun’s CLI commands and workflow. It manages the overall build or test process, from initialization to completion, using Graph and Executor internally. The Coordinator interprets user intentions (like “build these entry files” or “run tests with this configuration”), sets up the DAG engine accordingly, and handles events like plugin startup, watch mode file changes, and final output production. Essentially, it **coordinates** all components to work together in performing a complete build/test cycle.

**Responsibilities:**
- **Parsing User Inputs & Options:** The Coordinator reads the inputs (entry file paths for build, or finds test files for test runner) and configuration flags (from CLI or a config file like `bunfig.toml`). It encapsulates them in a `BuildOptions` structure that it passes to Graph/Executor.
- **Plugin Lifecycle Hooks:** Before building the graph, Coordinator triggers any plugin `onStart()` hooks [oai_citation:56‡bun.sh](https://bun.sh/docs/runtime/plugins#:~:text=onStart%28callback%3A%20%28%29%20%3D%3E%20void%29%3A%20Promise,void). These hooks may perform side effects (like logging or generating files). The Coordinator waits for all `onStart` callbacks (they can return Promises, so we must await them). This may involve calling into Bun’s JS runtime. Since our engine is primarily Zig, we might need to call an FFI or have an existing mechanism to run JS hooks here (Bun likely has an internal call that runs all onStart hooks when starting a bundler). We ensure to do this once and handle errors (if an onStart fails, abort build).
- **Graph Construction:** Using Graph API:
  - Initializes a new Graph with given options (like target environment, whether incremental caching is enabled, etc.).
  - Adds entry points to the Graph via `graph.addEntryPoint` for each provided entry file. If the user didn’t specify entry (like running `bun build` with a config that has entry defined), it loads those from config.
  - If running tests, the entry points might be each test file discovered. The Coordinator might need to discover test files (e.g., globbing for `**/*.test.ts`). Bun’s test runner likely already has logic to find tests. We can reuse or call that to get list of tests to run.
  - Possibly in test mode, instead of one DAG with all tests as separate entry nodes, we might treat each test file as an entry (TestTask node) and possibly have a Virtual node if needed. The Coordinator can also choose to run tests in batches if needed (but likely one per file in parallel).
  - The Graph will be populated with nodes and dependencies either immediately or as the Executor runs. The Coordinator doesn’t necessarily wait to fully expand the graph; it can proceed to execution once entry nodes are in place. However, if we choose to build the graph synchronously (like doing initial resolution and perhaps parsing imports), Coordinator might call some function to prepopulate (like a dry-run of parse only collecting imports).
  - In practice, we likely rely on Executor to expand graph dynamically. So Coordinator doesn’t do heavy lifting here beyond adding entry nodes.
- **Executor & ProcessPool Setup:** 
  - Initializes the Executor with a thread pool sized based on options (or default to CPU count).
  - Initializes the ProcessPool(s):
    * If any plugins are in use or macros are expected, start the plugin ProcessPool (which can also serve macro calls) with, say, 1 or 2 processes. Bun’s blog suggests at least 1 separate process is used [oai_citation:57‡bun.sh](https://bun.sh/blog/bun-bundler#:~:text=,for%20Bun%27s%20runtime%2C%20improving%20running).
    * If running tests, initialize the test ProcessPool with a number of workers (e.g., user might specify `--jobs=4` for 4 parallel, or default to CPU count).
    * When spawning these workers, Coordinator must load necessary context:
      - For plugin workers: preload the user's plugins. Possibly by running the same plugin registration code in the worker. If plugins were defined in a project script, we might call that script in the worker context.
      - Alternatively, send the plugin definitions over IPC. Simpler: have the worker start a Bun instance that reads the project’s `bunfig.toml` or uses the plugin list (maybe plugin definitions are captured in a global).
      - We'll assume the worker can `import` the entry file or a config that triggers plugin setup.
      - This may be handled automatically if the plugin is defined in `bunfig.toml` (the worker could read it), or if user called plugin() in their build script, we might have to replicate that. This is an implementation detail to solve but the Coordinator orchestrates it.
      - For macro execution, plugin worker can also handle that as discussed.
      - For test workers: ensure they load Bun’s test runtime (perhaps they just do `bun --test <file>` internally, or we give them a special flag to run in "test worker mode").
      - Possibly, the test worker will have to know how to listen for a "run this test file" message, then import it and run tests. We will implement a separate JS script for the test worker that sets up a minimal testing environment.
    * The Coordinator provides each ProcessPool with any necessary configuration (e.g., current working directory, environment variables like NODE_PATH or such, maybe a preloaded module list).
  - Once workers are ready (perhaps Coordinator waits for a handshake from them that they are initialized), it proceeds.
- **Execution and Monitoring:** 
  - Triggers the Executor to start processing. In practice, once entry nodes are added and markReady, the worker threads will start on them. So Coordinator can immediately call `executor.waitForCompletion()` to block until done.
  - Meanwhile, Coordinator can also handle other events:
    * In test mode, perhaps streaming output: we might want to print test results as they come in rather than after everything. We could achieve that by having test worker processes directly print to console or send IPC messages per test. To keep scope, maybe test output is only printed after each file completes (which we can do when each TestTask finishes, by outputting summary or failing tests details).
    * In build mode, possibly show a spinner or progress (maybe not needed, but we could show which file is currently compiling in verbose mode).
    * If running in watch mode (`--watch`), Coordinator sets up file watchers for all relevant files. Bun’s watch mode currently restarts the process on changes [oai_citation:58‡bun.sh](https://bun.sh/docs/runtime/hot#:~:text=Watch%20mode%20%E2%80%93%20Runtime%20,process%20when%20imported%20files%20change). Now we implement a smarter watch:
      - After initial build, use Graph’s node list to know which files were involved (all `FileModule` nodes). Register OS file watchers on those.
      - On a file change event, pause (debounce a few milliseconds to aggregate multiple saves).
      - Then initiate an incremental rebuild: 
        + Use Graph.markDirty on the changed file’s node (and that will propagate to dependents).
        + Reset states of those nodes to Pending/New.
        + Optionally drop outputs and cached data of those nodes.
        + Recompute versions for changed files (e.g., new hash).
        + Then either call Executor to schedule those dirty nodes (the Executor might remain running waiting for tasks).
        + Possibly simpler: tear down the old Executor and start a new one for the rebuild. But that loses caching unless we persist it in Graph.
        + Actually, since Graph is still in memory with outputs of unaffected nodes, we want to reuse that. So better to keep Executor alive:
           * We can push the now-dirty entry nodes back into the readyQueue (or directly call markNodeReady if their deps are satisfied aside from the changed ones).
           * But careful: if a file in the middle changed, all its dependents should be re-run. We have marked them dirty so their state is New. But their dependencies might still appear Completed (with old data). Ideally we also mark those dependencies as requiring re-validation (they may not re-run if not changed, but they might need to reconsume changed input).
           * Actually, in incremental compile, if file A changes, file B that imports A should re-run, but file C that doesn’t depend on A remains valid.
           * Our Graph.markDirty already handled that. So now entry nodes that ultimately depend on A are dirty and need execution; others can be skipped by isUpToDate checks.
           * We then invoke the Executor again. If threads were not stopped, they might be idle waiting. We can simply enqueue the dirty nodes as ready tasks. Worker threads will pick them up and execute. We might need to notify the condition variable to wake threads if they were waiting with nothing to do.
           * The Executor should be able to handle multiple sequential "rounds" of tasks over time. We must ensure any lingering state (like encounteredError flag) is reset at the start of a new build round.
        + Print rebuild-specific info, e.g., "Rebuilding X file...".
      - If watch mode is triggered by a file addition or deletion, handle:
        * Addition: If a new file was added that is imported by something, when that import is resolved next time, Graph will create a node then. We might not get an event unless it was within watched set (maybe not watched if it wasn’t known). But if it’s new and imported, the first build attempt after will catch it.
        * Deletion: If a file is deleted, the watcher will trigger. If that file is in graph (e.g., user deleted a source file), we mark it dirty then attempt rebuild. When Executor tries to load it, it will error file not found. We handle error as usual (report and wait for either user to restore file or terminate watch if they Ctrl-C).
      - In summary, watch mode remains active until user stops. Coordinator loops on file events to trigger new build cycles.
- **Finalization:**
  - After a build (non-watch), Coordinator collects outputs:
    * If bundling for production, writes output files (e.g., to `outdir`). It can iterate `entryNodes`: for each, get its output (which might be a `BuildArtifact` Blob) and write to disk or present to user.
    * If `--outdir` is set, do that; if not and `bun build` by default prints to stdout the result (as docs show, bun build prints bundle to stdout if not directed to file [oai_citation:59‡bun.sh](https://bun.sh/docs/bundler/macros#:~:text=Now%20we%27ll%20bundle%20this%20file,will%20be%20printed%20to%20stdout)), then Coordinator writes the combined output of entry(s) to stdout.
    * If multiple outputs (multiple entry points), either we write multiple files or we print them sequentially separated (but better to require `--outdir` if multiple).
    * Also write sourcemaps if enabled.
    * If building a standalone executable (`--compile` flag in Bun), then coordinator would call Bun’s bundler logic to package the runtime with the code (Bun had that feature). That might require calling an external function with the artifact. We maintain that by still calling whatever function was used, but now feeding it code from our DAG output.
  - In test mode, after all tests complete:
    * Coordinator collates test results from each TestTask’s output. It may sort tests by file or maintain the original order. 
    * It then feeds that to Bun’s test reporter (which could be a JS implementation printing nicely). Possibly Bun’s test runner collects results in JS; if so, maybe we should actually integrate earlier, by running the standard reporter in the worker or main.
    * Simpler: output a summary: e.g., “5 tests passed, 1 failed (in 2 files)”.
    * However, users expect detailed output per test (names of failing tests, etc.). We might need to replicate or leverage Bun’s existing reporter logic. Perhaps we can instantiate the reporter in the main process and feed it results.
    * If time, we just print basic info and rely on the fact that each test file’s worker already printed details to stdout (we could design the worker to do that).
    * Return exit code 0 if all passed, or non-zero if failures (the coordinator will set process exit accordingly).
  - **onEnd Hooks:** If Bun had a plugin onEnd (not documented, but hypothetical), we’d call them here after build success. For example, a plugin might want to do something once bundling is done (like publish artifacts). We ensure any such callback is called. None is in current API except perhaps one can simulate onEnd via onStart of next run, but anyway.
  - Save cache (if incremental): Coordinator invokes `Versioning.saveCache` to persist the build cache (node versions and outputs) to disk for next run. This occurs if build succeeded (we could also save partial cache on failure for unaffected parts, but not necessary).
  - Print overall build result or test result to console (like “Build succeeded in X ms” or test summary).
  - Free or clean up: If not exiting process, dispose of Executor threads and kill worker processes to free resources. If it’s watch mode, we don’t free because it stays running.
- **Cross-Process Workflow Awareness:** 
  - Coordinator ensures that processes like plugin workers and test workers are aware of the project context. For example, share environment variables like `BUN_DEBUG_` flags, working directory, etc. 
  - If we want to share compiled modules across processes, Coordinator might set something up like a shared cache directory which all processes read. (By default, our Versioning writes to disk which both main and workers can access).
  - For instance, if a worker needs to compile a module, it could check the cache from main build saved on disk. If present, load that compiled output instead of re-transpiling. This scenario might occur if main compiled module A and then a worker also needs module A. In our design, workers (like test worker) starting after build could reuse cached compiled code for faster startup. We should mention:
    * After building, Coordinator can signal to test workers the list of compiled modules and their cache locations (maybe via an IPC message or environment variable pointing to the cache file). Workers could then use Bun’s mechanism to load from cache. This is advanced and maybe not needed for initial implementation, but conceptually possible.
  - At minimum, the cross-process coordination is in orchestrating tasks, which we have via ProcessPool.

**Assumptions & Constraints:**
- We assume Bun’s core (Zig) can interface with the JS runtime for running plugin hooks (`onStart`) and possibly for macro execution if not using separate process. We will utilize existing bridging code in Bun.
- The Coordinator must not block the JS event loop indefinitely, especially when awaiting plugin promises. We likely run those hooks through Bun’s JS and yield appropriately.
- BuildOptions should capture all relevant toggles (minify, target, environment variables for defines, macro enable/disable, etc.). We ensure to pass these into Graph/Executor so they know how to handle code generation.
- If building for production (target not "bun"), the bundler might do extra transforms (e.g., polyfills). The Coordinator or Graph should set that mode so that transpilation knows to target a different runtime (like older Node or browsers).
- We ensure compatibility with existing Bun usage: e.g., `Bun.build({...})` programmatic API. If a user calls that in JS, it likely goes into Bun’s native code. We will route that to use our Coordinator under the hood. That means our Coordinator might be triggered not just via CLI but via an API call on the main thread. We need to ensure reentrancy or at least that calling Bun.build from JS runs the same pipeline (maybe synchronously or returns a promise that resolves when build done). This likely requires exposing an async interface (since our implementation in Zig might block). Possibly Bun.build (JS) could spawn a background thread for bundling; but given our engine is multi-threaded, maybe we run it on those threads and return a promise to JS. This is an integration detail: we should support that usage by ensuring Coordinator can be invoked asynchronously and signal completion back.

In conclusion, `Coordinator.zig` orchestrates the entire `bun-meta-dag` engine, bridging between user-level operations and the internal DAG mechanics. It ensures that the new DAG-based system operates within Bun’s existing plugin/macro ecosystem without regression, and that features like watch mode and test runner are enhanced by DAG-awareness. By managing plugin hooks, process pools, incremental cache usage, and final output/reporting, the Coordinator enables engineering teams to use the DAG engine in a way that feels natural and improves performance and capability, all while hiding the complexity under a user-friendly interface.

## src/meta_dag/Versioning.zig

**Purpose & Scope:** Manages version hashes, caching of compiled results, and build cache persistence. The Versioning component enables **incremental builds** by determining when a node’s inputs have changed and by storing outputs from previous builds for reuse. It provides a central place to compute content hashes (e.g., file content hashes), compare versions, and save or load build artifacts to disk. This system ensures that if nothing relevant changed since the last build, Bun can skip recalculating those parts, achieving fast rebuilds.

**Key Responsibilities:**
- **Content Hashing:** Provide fast hashing of file contents and other data to produce VersionId values. Use a consistent hash algorithm across runs and processes (e.g., a 64-bit FNV or a cryptographic hash if collision must be virtually impossible).
- **Version Comparison:** Given stored version info, quickly decide if a node is up-to-date.
- **Persistent Cache Storage:** Define a format and location for storing build cache information (e.g., a JSON or binary file listing each source file’s last built hash and relevant output info). Possibly also store compiled artifacts (like transpiled JS code) to reuse between runs or for use in worker processes.
- **Cache Retrieval:** On a new build, load existing cache data from disk (if available) so we know previous versions of each file. This could pre-mark many nodes as up-to-date before execution.
- **Cache Update:** After a successful build, update the cache file with new hashes and outputs.
- **Immutability & Versioning:** If Bun’s internal file format changes or we want to invalidate caches (like when Bun upgrades or a plugin’s behavior changes), incorporate an internal cache version identifier so old cache can be ignored.
- **Integration with Graph & Executor:** Provide APIs for Graph/Executor to call:
  - E.g., `versioning.checkNodeUpToDate(nodeKey, newHash) -> bool` and if true maybe `getCachedOutput(nodeKey)` to retrieve the compiled output.
  - E.g., `versioning.recordOutput(nodeKey, hash, output)` to store results after execution.

**Data Structures:**
- **`struct CacheEntry`** – Represents stored info for one node (or one source file module):
  - `hash: VersionId` – The content hash of the inputs last time this was built.
  - `outputPath: ?[]const u8` – File path to a cached output artifact (if output is large, e.g., compiled code stored in a separate file). Or it could directly store a short output (like for small macros, maybe store the result inline).
  - (Potentially additional metadata: e.g., for source file modules, could store the list of dependency file hashes to ensure none changed; but our DAG inherently covers that by checking dependency nodes. We might not duplicate that here.)
  - We might differentiate by node type: e.g., no need to cache macro results separately if we always re-run them – but maybe worthwhile to cache expensive macro results if their inputs (including any external data they read) didn’t change. Hard to know external dependencies though, so maybe skip that.)
- **`struct BuildCache`** – The overall cache, containing:
  - `entries: HashMap<[]const u8, CacheEntry>` – map from node key (likely file path for source files) to cache entry.
  - `version: u32` – version number of the cache format/logic to invalidate outdated caches. If mismatch, ignore file.
  - Possibly global metadata like the Bun version or certain plugin versions to force rebuild if environment changed.
- **Cache file location:** Could be in project, e.g., `bun.lockb` or `.bun-dag.json` in the project directory. (A name `bun.lockb` appears in search results [oai_citation:60‡github.com](https://github.com/erikbrinkman/d3-dag/blob/main/bun.lockb#:~:text=bun.lockb%20,dag), perhaps Bun uses that for something in bundler context, maybe bundler lock file or cache.)
  - Alternatively, use system temp or global cache dir. But better keep in project to tie to content.
  - We'll assume a file in project root, like `bun-development-cache.json` or similar, or piggyback on `bun.lockb`. We can pick one.

**API Methods:**
- `fn computeFileHash(path: []const u8) !VersionId` – Reads the file (possibly streaming) and computes its hash. Used for initial version computation in Graph when adding a file node. Use a fast algorithm (like xxHash or murmur) for performance since cryptographic strength is not critical (collisions extremely unlikely to cause wrong build).
- `fn computeDataHash(data: []const u8) VersionId` – Hashes an in-memory buffer (used for e.g., macro results or plugin outputs if needed).
- `fn loadCache(cachePath: []const u8) BuildCache` – Reads the cache file from disk. If file not present or version mismatch, returns an empty cache (or with `versionMismatch=true` maybe). This will parse JSON or binary into the `entries` map.
- `fn saveCache(cache: *BuildCache, cachePath: []const u8) !void` – Writes the cache entries to disk (e.g., as JSON with file paths and hash values, or as binary).
- `fn checkUpToDate(cache: *BuildCache, key: []const u8, newHash: VersionId) bool` – Looks up the cache entry by key, compares stored hash with `newHash`. Returns true if they match, meaning the content has not changed since last build.
  - Optionally, we might also check if outputs referenced by cache exist. E.g., if `outputPath` is set, ensure that file exists. If not, treat as cache miss (because maybe user cleaned output directory).
- `fn fetchCachedOutput(cache: *BuildCache, key: []const u8) ?CachedOutput` – If the node’s output was cached (and checkUpToDate was true), retrieve it. This could either:
  - Return an in-memory copy if we stored it in the cache (some caches might store small outputs directly).
  - Or read the artifact file from disk (if outputPath given). For example, for a transpiled JS module, we could have stored the compiled code in `.bun-dag-cache/<hash>.js` file. We then read it here.
  - We might need to also handle source maps similarly if caching them.
- `fn storeOutput(cache: *BuildCache, key: []const u8, hash: VersionId, output: []const u8) !void` – Stores the output of a node after execution:
  - If output is small (e.g., a few bytes from a macro), we could store directly in the cache entry (in memory and optionally in the JSON).
  - If large (like kilobytes of JS), better to store as a separate file in a cache directory. Possibly name it by hash to avoid duplicates – content-addressable caching. For example, name = `<hash>.js` for compiled code and maybe `<hash>.map` for source map. Or include part of file path to avoid collisions on hash alone if different files coincidentally share hash (extremely unlikely if hash is strong).
  - Write the file to disk.
  - Update cache.entries with new hash and new output path.
- `fn clearCache(cache: *BuildCache, keys: []const u8)` – (Optional) Remove cache entries for given keys. Could be used if we detect a scenario of invalidation (like plugin changed, so drop all entries that were built with old plugin).
- Possibly `fn initCache(projectPath) !BuildCache` – A convenience that decides the path (like using projectPath + "/.bun-build-cache") and calls loadCache or creates new.

**Integration Workflow:**
- **Initial Build:** 
  - Coordinator calls `loadCache` at start (if incremental builds are not disabled and if cache file exists). 
  - Graph, when adding nodes, can use `checkUpToDate`:
    * For each source file node added, compute file hash via `computeFileHash`.
    * Then `if cache.checkUpToDate(filePath, hash)`:
      - Mark node’s version = hash, and possibly set a flag that it’s up-to-date.
      - We might still need to ensure its dependencies are accounted for. Actually, if file content unchanged, and we assume that means its imports and internal code is same, then if all its dependencies are also up-to-date, we can skip it. We rely on DAG traversal for that: Executor might still schedule it if any dep changed. But if none changed, when Executor considers it, we could skip executing it because version matches and all deps have Completed status with same version as before (or also skipped).
      - Possibly Graph or Coordinator can proactively mark it as `Completed` and load its cached output so that dependents see it as available without execution.
      - That is advanced; maybe easier to just store an up-to-date flag and let Executor call `fetchCachedOutput` and skip actual compilation. 
    * We could decide:
      - Approach A: Do not mark Completed upfront. Instead, when Executor picks a node to run, it checks `isUpToDate` (which consults version info). If true, it fetches cached output and immediately calls `markDependencyComplete` without doing heavy work.
      - Approach B: Pre-mark nodes as Completed in Graph before execution. But that complicates scheduling because dependents might be marked ready immediately, which is okay if we treat skip as an instant completion.
      - Approach A is simpler to implement in Executor logic.
  - So Graph will still add all nodes, but maybe not queue them if they have unresolved deps.
  - Executor when pulling from readyQueue, for each node:
    * If `graph.cache.checkUpToDate(node.key, node.version)` and (for safety) all dependency nodes are Completed (which they would be if ready), then:
      - It calls `cache.fetchCachedOutput(node.key)`.
      - Sets `node.output` to that data (or at least ensures dependents can get it).
      - Marks node as Completed (calls markDependencyComplete on it immediately without actually running parse, etc.).
      - Essentially skipping actual execution.
    * If fetch fails (like artifact missing), then it falls back to normal execution.
  - This ensures correctness: if artifact was missing but content same, we rebuild it. That's fine (just a lost caching opportunity).
- **After Build:**
  - Coordinator (or Graph/Executor at end) calls `saveCache` to persist updated cache file.
  - But note: by then, cache.entries in memory has been updated via storeOutput for each executed node.
  - The output artifact files are already written individually during the run.
  - The cache file likely contains just mapping of keys to hashes and artifact file names (not the content itself for big ones).
- **Artifact reuse across processes:**
  - If a worker process needs an already-built artifact, how to use it:
    * For example, test worker needs the compiled JS of a TS module from the bundler. The bundler main would have cached that .js in cache directory with a content hash.
    * When worker goes to load that module (via normal module loader), we could have Bun’s loader check the cache directory first: e.g., if there's a `.bun-dag-cache/<hash>.js` and perhaps some marker, load that instead of recompiling.
    * We might integrate with Bun’s transpiler: maybe provide a `BUN_CACHE` environment pointing to cache; Bun might not support that out of box yet, but we can implement in its Module loader: if file is TS and a compiled .js exists in cache with matching timestamp or hash, use that. This might be an optimization beyond initial scope but aligns with cross-process DAG awareness.
    * At minimum, the workers that are part of our build (like plugin worker, test worker) could have access to the BuildCache object via IPC. But that’s heavy to sync. Simpler: rely on disk.
    * So yes, writing compiled outputs to disk in a known location means any Bun process can use them if programmed to. Perhaps in future, Bun’s require/ESM loader can check a cache of compiled code keyed by content hash.
    * For now, we may mention this but not fully implement.

**Edge Cases & Decisions:**
- If a file's content hash collides with another (extremely unlikely with 64-bit for typical file sizes, basically negligible risk, further reduced if using 128-bit), worst case we treat them as same version erroneously and skip when we shouldn’t. This is a trade-off usually fine for build systems. If concerned, use a strong 160-bit or so but then storing that in cache is bigger. 64 or 128 is okay.
- If a user manually edits the cache file or some artifacts, weird behavior may ensue (we can ignore or override).
- Ensuring cache validity with environment changes: if e.g., a plugin’s code changed (like user updated a custom plugin in between builds) but source files didn’t, our content hashes won’t detect that and we’d erroneously skip. To handle this, we might incorporate some global version: e.g., hash of all plugin source code as part of key for caching or a “cache salt” that changes when plugins change.
  - Simpler: flush cache on major changes. We could require user to do `--no-cache` if they know plugin changed. Or always re-run in such scenario (hard to detect reliably).
  - Perhaps track plugin file mtimes in the cache and if any changed, invalidate all.
  - This is complex to fully solve; we can document that plugin changes may require cleaning cache.
- Large outputs: In caching compiled JS, note that final bundled output might not be cached as it might change if any small part changes. But intermediate compiled files we do cache. The final bundle assembly (concatenation) is so fast anyway that caching it is unnecessary.
- Memory: Keep cache map manageable. We can limit storing outputs in memory – maybe don’t keep large code strings in memory cache entries, just keep them on disk. Use memory only for quick comparisons (hashes).
- Removing stale cache entries: If a file is deleted or renamed, its old cache entry remains in cache file. Over time could accumulate. We could periodically purge:
  - E.g., after each save, remove entries for which no node in current graph had that key. (We can track keys used this build and filter others out before saving.)
  - That cleans up cache for removed files.
- Concurrency: The Versioning methods will mostly be called in the main thread or controlled contexts:
  - Graph building calls computeFileHash serially or in executor but we can allow multiple threads to compute different file hashes concurrently (no shared state except maybe a global seed for hash – not needed).
  - The cache data structure itself (maps) might be accessed by multiple threads if, say, executor threads call checkUpToDate concurrently. We should guard it or better, do checkUpToDate in Graph single-thread and store the flag in node to avoid repeated checks. But simpler: we can have a mutex for cache access if needed. However, checking a hash in a map is very fast and a bit of lock overhead is fine if needed.
  - Alternatively, we could copy needed cache info into each node (like store `lastBuildHash` in SchemaNode on init) to avoid locking when checking in Executor.
  - Let's consider doing that: Graph on node creation, if cache says up-to-date, it marks node with a flag and maybe pointer to cached artifact path. Then Executor can simply read that flag and path from node without locking the global cache.
  - That’s cleaner: basically, integrate cache info into node at init: e.g., `node.lastBuildHash = cacheEntry.hash` and `node.cachedOutputPath = cacheEntry.outputPath`.
  - Then `node.isUpToDate() = (node.version == node.lastBuildHash)` and if true, we use node.cachedOutputPath to load output.
  - This decouples Executor from needing to query cache structure at runtime.
  - We will proceed with that approach in implementation: The Versioning module primarily is used at graph init and end of build, not during execution except for output storage.

In summary, `Versioning.zig` provides the mechanisms for Bun’s DAG engine to avoid redundant work by reliably detecting unchanged inputs and reusing previous outputs. By hashing content and storing compiled artifacts, it underpins the incremental build capability. This allows features like `bun build --watch` to rebuild in milliseconds for small changes, and even across process boundaries, ensures that expensive compilation results can be shared rather than duplicated, moving Bun closer to a truly **DAG-aware runtime** that caches and reuses work intelligently.

## src/meta_dag/Introspection.zig

**Purpose & Scope:** Offers tools for logging, debugging, and introspecting the DAG engine’s behavior. This includes formatting the internal state (nodes, dependencies, etc.) for human-readable output, as well as handling debug flags to produce verbose logs of execution. Introspection does not affect core logic; it runs alongside for transparency. It’s crucial for developers integrating the DAG engine to trace what’s happening under the hood and ensure there are no regressions compared to the old bundler.

**Features:**
- **Verbose Logging:** When enabled (e.g., via `-v` or an env var), log the progression of the build:
  - Node readiness, execution start, completion, and any errors with identifying info.
  - Timing information for tasks.
  - Summary of results (counts of nodes, duration, etc.).
- **Graph Dumping:** Provide a function to dump the entire DAG structure (or a subgraph) in a readable form. Could be text or DOT format for graphviz.
- **Interactive Query Support:** (Optional/future) If Bun had a REPL or debug mode, allow querying the DAG data structure at runtime (perhaps not needed for now, but possible).
- **Minimal Overhead when off:** When introspection is not enabled, its calls should be no-ops or compile out, so they don’t slow the engine.

**Key Components:**
- **Global Flags/Config:** Likely a global boolean or flags bitfield for what to log:
  - `bool verbose` – general verbose logging.
  - `bool logExecution` – log each node exec.
  - `bool logScheduling` – log scheduling events (maybe combined with execution).
  - Possibly separate for test vs build logging, etc. Simpler to have one verbose that does everything.
- **Log Sink:** Could use Zig’s `std.debug.print` to stderr. Bun might also have a logging system (like using `Console.write` or so) but since we are in Zig core, `stderr` is fine for development logs.
- **Functions:**
  - `fn logNodeEvent(event: EventType, node: *SchemaNode, details: ?[]const u8)` – Logs a formatted message about a node. EventType might be START, COMPLETE, SKIP, FAIL, READY, etc. It will include node.id, type, key in the message, and any details (like error message or duration).
  - `fn dumpGraph(graph: *MetaDag)` – Iterates through graph.nodes and prints each node’s basic info and deps. Example output:
    ```
    Node 5: TranspiledModule "src/app.ts" [deps: 2,3] state=Completed
      version=abc123 (up-to-date)
    ```
    Possibly also print dependents or other info.
    - We might indent to show hierarchy, but since it's a DAG not tree, hierarchy is tricky. Could do a topological listing.
  - `fn dumpNodeDetails(node: *SchemaNode)` – prints one node with all details (could be used in above).
  - `fn dumpReadyQueue(graph: *MetaDag)` – If needed, print what’s currently in the ready queue (for debugging scheduling).
  - `fn logSummary(graph: *MetaDag, startTime: f64, endTime: f64)` – Logs summary: total nodes, executed count vs skipped, total time, maybe peak concurrency.
  - If we want to integrate with a profiling or tracing system, we could add hooks to emit events (like to Chrome tracing format). But that's beyond scope.
- **Integration Points:**
  - Executor: Wraps node execution calls with log messages:
    * Right before executing, `Introspection.logNodeEvent(START, node, null)`.
    * After, `Introspection.logNodeEvent(COMPLETE, node, "${duration}ms")`.
    * If skip, `Introspection.logNodeEvent(SKIP, node, "used cache")`.
    * On error, `Introspection.logNodeEvent(FAIL, node, errorMsg)`.
  - Graph/Coordinator: When adding entry nodes or when marking ready, can log:
    * `Introspection.logNodeEvent(READY, node, null)` when a node becomes ready to run.
    * Possibly log when dependencies are added or cycles detected: e.g., `debug.print("Cycle detected: %s -> %s\n", .{nodeKey, depKey})`.
  - Plugin & Macro calls: Could log those actions too, e.g., `debug.print("Plugin resolve '%s' in '%s' -> '%s'\n", spec, importer, result)`.
  - Coordinator: 
    * log when starting bundler, and when finished (“Build finished with X errors in Y ms”).
    * For watch mode, log when rebuild triggered.
    * For tests, log test results in detail if verbose (maybe every test pass/fail).
- **Thread Safety:** Logging is one of the few things that will be done from multiple threads (executor workers). We must ensure log output lines don’t intermix:
  - We can use a mutex around logging (one global for all introspection logs). Simpler: funnel logs from threads to main thread.
  - But implementing a log queue is overkill; just lock around each log print call. The performance cost in verbose mode is acceptable since that's debugging scenario.
  - Use `std.atomic` or `std.sync.Mutex` for a global lock around `debug.print` calls, or possibly use synchronous I/O which might internally block anyway making it effectively serial.
  - To be safe, implement a simple spinlock or use a provided primitive for multi-thread printing.
- **Conditional Compilation or Runtime Check:** 
  - Could compile out all introspection calls if not needed (like in release mode), but it's often useful to enable via a flag at runtime.
  - We'll likely do runtime checks: e.g., `if (verbose) debug.print(...);`.
  - The overhead of an if for each logged event is fine given logging itself is far more overhead.
  - Alternatively, some debug logs might be behind compile-time `if (BuildMode != Release)` but let's keep toggling at runtime for flexibility.

**Example Usage:**
If user runs `bun build -v`, in output they might see:
```
Bundle started (entry: src/index.ts)
[dag] READY Node 1 (FileModule "src/index.ts")
[dag] READY Node 2 (FileModule "src/util.ts")
[dag] START Node 2 (FileModule "src/util.ts")
[dag] COMPLETE Node 2 (FileModule "src/util.ts") in 4ms
[dag] READY Node 3 (FileModule "src/index-dep.ts")
[dag] START Node 3 (... etc)
...
[dag] START Node 1 (TranspiledModule "src/index.ts")
[dag] COMPLETE Node 1 ... in 10ms
Bundle completed in 15ms (3 nodes executed, 0 skipped)
```
(This is illustrative.)

We prefix with `[dag]` or similar tag to distinguish from other logs.

**Edge Cases:**
- If thousands of nodes, printing each might be overwhelming. But user asked for verbose, so that's expected. We might not print absolutely everything unless verbose.
- Maybe define levels: normal verbose logs just start/finish of tasks; a debug level beyond could print entire graph structure.
- For initial implementation, one verbose level is enough.

**No effect on logic:** 
All introspection should avoid modifying any state except maybe capturing timings. It strictly reads and reports.
We ensure not to call introspection in tight inner loops where it would slow down things even if off (but we mostly call it on significant events, so it's fine).

**Integration with Testing** (of our engine):
- Introspection utilities will help ensure our DAG is correct (we can write unit tests that construct small graphs and then call dumpGraph to verify structure, for development).

All told, `Introspection.zig` provides the necessary visibility into the DAG engine, which is critical for adopting such a complex system in a real-world tool like Bun. It will help catch issues early and allow developers (and advanced users) to understand the build process, thereby complementing the reliability and performance improvements with transparency and debuggability.

## src/meta_dag/ProcessPool.zig

**Purpose & Scope:** Manages a pool of background Bun processes for executing tasks that must be isolated or run in parallel outside the main process. This primarily serves plugin callbacks (which run in a separate JS runtime for safety/performance [oai_citation:61‡bun.sh](https://bun.sh/blog/bun-bundler#:~:text=,for%20Bun%27s%20runtime%2C%20improving%20running)) and test file execution (each test file in its own process for isolation). By pooling processes, we amortize startup cost and reuse them for multiple tasks. ProcessPool handles spawning these workers, sending them tasks via IPC, and receiving results.

**Design Overview:** 
- Launch a fixed number of worker processes (for each kind of workload) at startup or on first use.
- Maintain a queue of pending tasks if all workers are busy.
- Communicate with workers using JSON or a simple protocol over IPC (likely stdio or a pipe).
- Each worker runs a minimal event loop waiting for tasks, executes them (within Bun’s JS runtime), and sends back a response.
- The master (Coordinator/Executor) awaits these responses and then continues DAG execution.

**Worker Process Setup:**
- Plugin Worker: 
  - Launch `bun` with a special argument (e.g., `bun --worker-plugin`) that runs a built-in script which sets up the plugin environment.
  - That script would load all user-defined plugins (so they are ready to handle onResolve/onLoad).
  - Then it listens on IPC (which could be its stdin or an IPC channel) for messages like:
    `{ "id": 1, "type": "resolve", "spec": "path", "importer": "importerPath" }`
    or `{ "id": 2, "type": "load", "path": "filePath" }`
  - On receiving, it finds the appropriate plugin callback (matches filter regex and namespace as Bun’s plugin API defines) and calls it with the provided args.
  - It then sends back a result message: e.g., `{ "id": 1, "success": true, "path": "/resolved/path.js", "namespace": "file" }` for resolve, or `{ "id": 2, "success": true, "contents": "<file content>", "loader": "js" }` for load.
  - If plugin threw or couldn’t resolve, `success: false` and maybe an `error` field.
  - The worker might also handle macro calls if we decide to offload those here: e.g., message type "macro" with module & function. It would import the module (if not loaded already), call the function, and return its result.
  - Since macro files are essentially modules, the plugin worker can load them easily (we might unify macro execution with plugin mechanism).
  - The worker can keep modules loaded in memory for efficiency (so calling same macro twice is quick, or multiple resolves for same package keep cache). Bun’s runtime will naturally cache requires/imports in that worker process.
- Test Worker:
  - Launch `bun` with `--worker-test`.
  - The worker script sets up a minimal test environment:
    * It probably uses Bun’s test API to define a custom reporter that sends results back via IPC instead of printing.
    * It listens for messages: `{ "id": X, "type": "runTest", "file": "path/to/test.ts" }`.
    * Upon receiving, it imports that test file (which triggers the definitions of tests and then runs them, since Bun’s test framework automatically runs at end? Actually, in Bun, tests are run via a runner; we may need to explicitly trigger run).
    * We could alternatively make the test worker just run `bun test file.ts` internally, but we want control to capture output.
    * Better: in the worker script, after importing the test file, call Bun’s internal function to run tests (maybe Bun provides an API like `Bun.jest()` but not likely; perhaps the test runner is integrated in Bun’s CLI only).
    * We might have to replicate some test runner logic: for example, gather global `it()` calls, execute them, catch results.
    * However, to keep it simpler, perhaps skip fancy reporting: let the worker run the test file in its own Bun instance normally (which will print results to its stdout). We could just forward that output to the main process or have it captured.
    * But capturing structured results is nicer. Let's assume we can get a summary: maybe rely on exit code and a summary line.
    * Actually, Bun’s `bun test` likely prints detailed info and sets exit code. We can cheat: spawn `bun test file --reporter=json` (if Bun had that option) in a separate process. But that is spawning another process, not using a persistent one.
    * Instead, use persistent: if Bun’s test runner is accessible via calling some internal function or by driving global state, we could code that in the worker script.
    * This might be complex. For now, we might do a simpler approach: let each test file run in its own worker process from start to finish (not reusing the process for multiple test files). That loses pooling benefits but ensures correctness with minimal effort (basically same as current behavior if any).
    * But since the prompt specifically says "pooling behavior so runtime is DAG-aware across worker boundaries", they want pooling for tests too.
    * We'll assume we can manage running multiple tests in one process sequentially. So test worker remains alive after finishing one test file, ready for another.
    * The worker can maintain state (like loaded modules). However, to avoid tests interfering, maybe after running one test file, we should clear require cache or use separate context. Or simpler: perhaps each test file is isolated anyway because Bun likely spawns separate processes in current design, implying tests might not be written to handle a single process running multiple files (global state in one test file could affect the next).
    * We could mitigate by forcing the worker to run each test file in a fresh JS context. Bun’s Worker threads could be used inside the worker process to simulate that, but it’s quite heavy. Or the test worker can `bun.spawn` itself? That defeats pooling.
    * This is tricky. Possibly we do use separate processes for tests but reuse the OS process partially via forking (like pre-fork then run different file on each).
    * Given complexity, maybe it's acceptable that test pool means N processes for N tests concurrently, but after one test done, that process can take another test file (provided we somehow clean up global state).
    * We can explicitly call something to reset state in worker (like delete require cache, global variables).
    * We'll proceed with an approach: run tests in worker processes sequentially, trusting user tests not to pollute global state in a way that affects next (or we implement a partial cleanup).
    * Or could exit the worker and spawn a new one for next test if that is simpler; but that loses pooling advantage.
    * If tests are heavy (starting server etc.), better isolate, but if quick, reuse is fine.
    * We'll lean on reuse and mention the limitation that full isolation per test isn't guaranteed unless user uses separate processes deliberately.
  - Worker sends back at least whether tests passed and count of failures.
  - Possibly it could also send logs of failing tests to main for summary. Or just rely on the worker's own output.
  - Coordinator will aggregate these results.

**ProcessPool Data Structures:**
- **`struct WorkerHandle`** – represents a worker process in the pool:
  - `proc: Subprocess` – a handle to the Bun subprocess (from Zig’s perspective, maybe an object with pid and pipes).
  - `busy: bool` – whether currently executing a task.
  - `taskId: ?u32` – the ID of the current task if busy (so we can match response).
  - Maybe store type of task if needed.
- **`struct TaskRequest`** – as described, with an `id` (to correlate), `type` (string or enum), and fields depending on type.
- **`struct TaskResponse`** – with matching `id`, `success` bool, and result fields (path, contents, etc.) or error.
- **`struct ProcessPool`**:
  - `workers: []WorkerHandle` – array of length = pool size.
  - `freeIndices: Queue<usize>` – indexes of available workers.
  - `pendingTasks: Queue<TaskRequest>` – tasks waiting for a free worker.
  - `nextTaskId: Atomic(u32)` – to generate unique IDs for requests.
  - Perhaps separate pools per type, but we might implement as separate instances of ProcessPool (like one for plugin, one for tests).
  - If combined, tasks would have a field to denote which type of worker they require, but easier: separate pool instances.
- **Communication Mechanism:**
  - We can use blocking I/O in separate threads to listen to each worker’s output. Or use async I/O multiplexing.
  - Simpler: dedicate one thread to each pool (or even to each worker if easier) to read messages.
  - Or use Zig’s async to await on multiple sockets.
  - Could integrate with main thread event loop, but main thread is busy coordinating. 
  - Possibly spawn a Zig thread per worker that reads lines and enqueues responses in a thread-safe queue.
  - Use something like `std.io.BufferedReader` to read from `worker.proc.stdout`.
  - When it gets a full JSON message (terminated by newline), parse it (maybe using a simple JSON parser or manual parsing since format is known).
  - Then find the corresponding TaskRequest by id. We likely have a map `outstanding: HashMap<u32, *TaskRequest or callback>` or simply store it in WorkerHandle (`taskId`).
  - Actually, simpler: When we send a task to worker X with id N, we set `worker.taskId = N`. When reading a response on worker X, assume it’s for that id (since tasks are processed sequentially per worker, we probably won’t send two tasks to same worker concurrently).
  - So we might not need a global map of all tasks, just handle responses per worker in order.
  - Then signal completion: perhaps by storing the response in some result field and marking worker not busy, then notifying whoever is waiting (the Executor thread that submitted the task might be waiting on a condvar for that response).
  - Implementation: use a `std.sync.Channel` or simpler approach: we could in `submitTask` do:
    ```zig
    worker.busy = true;
    worker.taskId = id;
    send message to worker;
    // then wait (maybe spin or use condvar) until worker.busy becomes false or we receive result.
    // But can't busy-wait, better use a condvar or promise.
    ```
    - Or we use an event-driven approach: don't block the Executor thread at all. Instead, integrate with DAG scheduling by marking the DAG node waiting on external result and allow other tasks to run. Then when result arrives, mark node ready to continue (if a macro, then resume, but how?).
    - Given our earlier design, we actually block the Executor thread that called `submitTask`, which is acceptable since we have multiple threads. It's simpler logically.
    - So do a condvar: `worker.cv.wait()` until signaled by the reader thread when response comes.
  - So, we need a condvar per worker or per task. Could have one per worker: worker thread signals its condvar when it's done.
  - Or simpler, use one condvar in submitTask: Actually, each call to submitTask is on a separate Executor thread likely, so it can wait on its own stack until signaled.
  - We can utilize a `std.sync.WaitGroup` or an `std.sync.Event` for each task.
  - Possibly easier: do a busy-wait with small sleep, but let's do proper sync.
  - Approach: In `submitTask`, create a new `std.sync.ThreadCondition` and store it in WorkerHandle as `waiter`. Reader thread, upon message, sets response fields and calls `waiter.signal()`.
  - That unblocks the Executor thread.
- **Task Routing:**
  - If a task is submitted and an idle worker is available, assign it immediately and send message.
  - If none free, push into pendingTasks queue.
  - When a worker finishes, if pendingTasks not empty, take next and assign to that freed worker.
  - This is a typical worker pool logic.

**Concurrency considerations:**
- Access to `pendingTasks` and `freeIndices` needs locking (likely a Mutex). But could simplify using atomic counters since pattern is simple: we can just lock around the code that assigns tasks.
- However, since tasks can complete out-of-order, better to lock.
- We can use one mutex to guard both workers list (free/busy state) and task queue. Or separate locks but one is enough.
- The condvar approach covers the waiting on response, not worker availability.
- We also ensure to handle multiple tasks concurrently across different workers - this is fine with multiple Executor threads.

**Error handling:**
- If a worker process crashes or exits unexpectedly:
  - Our read thread will likely get EOF or error. We detect that, mark the worker as down. Possibly respawn a new process to replace it (if tasks remain).
  - If a task was in progress on it, we mark that task failed (notify waiting thread with error "Worker crashed").
  - Also potentially output an error log for debugging.
- If a worker process hangs (e.g., plugin code deadlocked or test stuck in infinite loop):
  - We might implement a timeout for tasks. For example, if no response in X seconds:
    * Mark task failed with timeout error.
    * Kill the worker process (SIGKILL).
    * Start a new one to replace.
  - We can measure by storing a timestamp when task sent; spawn a separate monitor or check in reader thread every so often.
  - This adds complexity; maybe not initial but it's a good safety for tests especially.
  - We can set a generous default timeout for plugin tasks (maybe 30s) and for tests (maybe a minute or user configurable).
  - We'll mention timeouts as a planned feature.

**Integration:**
- Coordinator will create e.g. `pluginPool = ProcessPool.create(pluginWorkerCount)` and `testPool = ProcessPool.create(testWorkerCount)`.
- Executor’s `processMacroInvoke` and `processPluginTask` will call `pluginPool.submitTask` and block for result.
- Executor’s `processTest` will call `testPool.submitTask` and block for result.
- On shutting down (Coordinator), call `pool.shutdown()` which:
  - Sends an "exit" message to each worker or just kills the processes.
  - Joins any reader threads.
- In watch mode, we might keep pluginPool alive across builds (makes sense; plugin processes can handle multiple builds).
- For test pool, each test run might re-use processes if running tests multiple times (like watch tests on file changes).
- If we are done with tests entirely, kill them.

**Assumptions:**
- The Bun runtime in worker mode will share some caches with main:
  - e.g., if main downloaded packages or resolved package paths, the workers could reuse global module cache (they share ~/.bun cache for npm maybe). That’s fine.
  - They won't share in-memory compiled code unless we specifically hook it up (as discussed).
- The overhead of sending data (e.g., entire file contents for onLoad) is acceptable given most files are not huge (if they are, reading them in main and then sending over pipe might be slower than worker reading directly from disk).
  - We could optimize by having the plugin worker read file from disk itself if needed. Actually, our design suggests onLoad plugin likely will do FS reads in plugin worker anyway (like if plugin is to transform a file, it might do `fs.readFile` itself).
  - If main already has content (like for MacroInvoke we have content in AST), sending that to worker might not be beneficial because worker could just import the module file from disk as well.
  - But for macro we may want to avoid reading file twice. Perhaps for macro, main could share the AST or source, but we won't go that far.
- Size of JSON messages: typically small (file paths, etc.). Should be fine on a pipe.
- We ensure to JSON-escape any strings to avoid parse issues (or use a simpler delimiter if certain data is not arbitrary).
- Alternatively, we could use a binary struct protocol, but JSON is easier to debug and fine given overhead negligible compared to the work done.

**Summary of steps in plugin process for an example:**
Main needs to resolve `"./foo.scss"`:
- Executor thread calls `pluginPool.submitTask` with `{id:5, type:"resolve", spec:"./foo.scss", importer:"/path/app.ts"}`.
- ProcessPool picks an idle plugin worker, sends `{"id":5,"type":"resolve","spec":"./foo.scss","importer":"/path/app.ts"}\n`.
- Worker receives, its JS plugin code (registered by a Sass plugin) sees `build.onResolve({filter:/\.scss$/}, ... )` matches, runs callback returning e.g. `path:"/path/foo.scss", namespace:"file"`.
- Worker sends back `{"id":5,"success":true,"path":"/path/foo.scss","namespace":"file"}\n`.
- Reader thread on master for that worker reads message, parses JSON, finds id 5.
- It sets worker.busy=false, stores result (in a temp place like ProcessPool.lastResponse or directly constructs a TaskResponse object).
- It signals the worker’s condvar.
- Executor thread wakes, finds success true with path, returns NodeId for that resolved path.
- Graph continues building using that.

This design, while detailed, covers the integration needed to ensure Bun’s new DAG engine can coordinate multiple processes and fully leverage parallelism across CPU cores and processes. The result is a system where even tasks that traditionally required separate processes (plugins, tests) are smoothly integrated into the DAG execution, making Bun’s runtime truly DAG-aware across worker boundaries as requested.

### Modifications to Existing Bun Source Code

To integrate `bun-meta-dag` into Bun, we must adjust several existing components of Bun’s codebase. The goal is to replace or augment existing mechanisms with our new DAG engine without regressing functionality. Below we list the key modifications:

#### src/bundler/Bundle.zig (hypothetical main bundler entry)
**Purpose:** This would be the main bundler routine that handles `bun build`. We modify it to use the DAG engine:
- **Initialize DAG Coordinator:** Instead of the previous bundler logic (which likely created an `OutputBundle` and did recursive module resolution and transformation), call into `Coordinator.runBuild()` with appropriate options.
- **Plugin Setup:** Remove direct calls to plugin callbacks in Zig. The Coordinator will internally handle `onStart` and Graph handles `onResolve/onLoad` via ProcessPool. Ensure that any global plugin registration (e.g., populating plugin filters) still happens via user code calling `plugin()` in JS (which likely populates a global list we now pass to plugin worker).
- **Macro Handling:** Remove any inline macro evaluation. For instance, if previously bundler had code like `if importAttr.type == "macro": executeMacro(...)`, that is gone. Now the DAG covers it.
- **Output Writing:** If previously after building modules, Bun concatenated them, we now gather artifacts from Graph:
  - The Coordinator should produce final `BuildArtifact` objects (with code and sourcemap). If necessary, adapt any code that writes these to disk or wraps them (for standalone executables).
  - If bundler previously computed some metadata (like a manifest of modules), ensure the DAG provides equivalent info if needed. Possibly the DAG can output a list of included files, etc., if required for some plugin.
- **Remove Legacy Code:** Deprecate or delete functions that built a module graph or transpiled files outside the DAG. They might include:
  - Module resolution functions (if not reused by Graph).
  - The old parallelization (if any) — e.g., Bun’s bundler might have used threads to parse files concurrently similarly to what we do, but we now unify it under DAG.
  - Old cache usage: If Bun had some caching (maybe it didn’t beyond package caching), replace with our Versioning. If `bun.lockb` was used to store resolved module paths, verify if still needed or if our caching supersedes it.
- **Error handling:** If bundler previously caught errors to format them nicely, integrate that with DAG:
  - For example, bundler might catch syntax error from parser and output an error with file path and code snippet. Our DAG Executor will catch and create an error string; we should ensure Coordinator prints it in a user-friendly way. Possibly reuse Bun’s error formatting utilities (like a function that given an error and source context prints a snippet). The Graph/Executor can store error info (like file path and error location) in a standardized way for Coordinator to print.
- **Watch Mode:** The CLI `--watch` flag likely triggered a separate code path (maybe restarting the process). We replace that with Coordinator’s incremental loop. So adjust `bun build --watch` handling:
  - Instead of simply re-running the bundler process on file changes, call Coordinator.watch() (which uses our file watcher integration).
  - Possibly disable the old restart mechanism. The old mechanism might have been implemented in JS (there's a `bun --watch` in docs doing hard restart [oai_citation:62‡bun.sh](https://bun.sh/docs/runtime/hot#:~:text=Watch%20mode%20%E2%80%93%20Runtime%20,process%20when%20imported%20files%20change)). We now intercept that: when `--watch` is present, our Coordinator will not exit after build but go into a loop.
  - If the old watch was implemented at a higher level (like restarting process externally), we might remove that and implement watchers in Zig (using OS notify APIs or polling).
  - Bun’s `fs` module or Node compatibility might have file watch functionality we can use internally.
- **CLI Output Differences:** Ensure any logs or prints that the old bundler did (like “Bundling...” or progress indicators) are either replicated or consciously changed. Our Introspection covers detailed logs in verbose mode, but normal mode should likely remain mostly silent except on errors or final summary.
  - Possibly output a summary like old bundler might not have done explicitly; we can decide if that's wanted (maybe not to avoid breaking scripts).
  - For now, probably keep it similar: Bun’s bundler might not print anything on success by default, just writes files. We'll do the same (maybe print something if standalone executable built).

#### src/bundler/PluginManager.zig (if exists, handling plugin registration)
**Purpose:** Manage plugin definitions from calls to `Bun.plugin()`. We integrate it as:
- Keep storing the plugin callbacks (onResolve, onLoad) as before, but now rather than calling them in-process, we pass them to plugin worker.
- Actually, easier: we don’t need to pass callbacks themselves; the plugin worker runs the same plugin setup code, so it has its own PluginManager in that process. That means our main process’s PluginManager is not used for execution, just for possibly knowing that there are plugins (maybe for not launching worker at all if none).
- So when Coordinator sees plugins registered (perhaps via a global in Bun), it will spawn pluginPool. The actual filter logic is executed in worker.
- Remove any direct invocation of these plugin callbacks in the main process bundler code. Instead, Graph/Executor uses ProcessPool for resolution/loading.
- Possibly adapt the filter/namespace logic: The main process Graph doesn’t need to know filter specifics, because it offloads resolution entirely. It just blindly sends every unresolved import to worker and gets an answer (worker will return either resolved or indicate not handled).
- That simplifies main code: we don’t have to iterate multiple plugins or match regex in Zig; plugin worker does it in JS with the actual plugin code.
- Ensure that features like Virtual modules and namespace usage remain supported:
  - E.g., a plugin could resolve an import to a virtual module (namespace "foo"). In old bundler, plugin onResolve could provide a namespace and onLoad would provide contents for that virtual module.
  - In our setup, plugin worker onResolve returns namespace "foo" and path likely same as spec or some unique key. Graph gets that and will treat it as key "foo:whatever".
  - Next, when Graph tries to load that, it sees a namespace != "file", so it should know to use plugin onLoad. How? Possibly by deferring to worker again:
    * Graph.resolveImport returns a Node with key "foo:whatever". Mark that node as needing plugin load (maybe by NodeType or storing namespace).
    * Executor when executing a FileModule with namespace "foo" will realize it’s not a disk file; it will call pluginPool for onLoad.
    * Or Graph itself could have immediately called worker onLoad after onResolve. But doing in Executor keeps asynchronous consistency.
  - We'll do it in Executor: If node.namespace != "file", Executor’s loadContent for that node will do `pluginPool.submitTask` with type "load".
  - This ensures plugins can create arbitrary virtual modules and we’ll get their content to compile as normal.
- Remove old macro system integration from plugin manager if any (maybe not, macro was separate concept).
- Macro imports might have been partially handled by plugin in old bundler? Not likely, macro was separate.
- Possibly remove or adjust how import attributes are parsed – ensure the Zig loader still passes import attributes to Graph (like it needs to know if `type:"macro"` attribute is present to mark macro module nodes).
  - That probably happens in parsing stage which we handle in Executor now. We must ensure our parser is aware of import attributes (Bun likely extended esbuild parser to capture them). We use the same parser, so it should handle and expose them to us.
  - We then create MacroInvoke nodes accordingly.

#### src/test/TestRunner.zig (or wherever `bun test` is implemented)
**Purpose:** Manage test execution and results.
- Currently, Bun’s test runner might find test files, spawn them (maybe directly via Bun’s multi-threading or processes).
- We intercept that to use our DAG:
  - When user runs `bun test`, instead of directly iterating test files and running them, we initialize the DAG system in test mode.
  - Possibly reuse some code to discover test files (if not, we implement a simple directory scan per config).
  - Create a `TestTask` node for each test file (or perhaps group by directory or something, but likely one per file).
  - Use Executor with testPool to run them in parallel.
  - Collect results and output via Coordinator as described.
- Remove the old logic that spawned processes per file:
  - e.g., maybe it was using `Bun.spawn` for each test or using threads to run multiple tests in same process.
  - Ensure that features like test filters (`bun test someNamePattern`) are respected. Possibly in old runner, it filtered tests at runtime. In our design, we can filter at discovery (only include matching test files and or pass a filter to the worker to only run tests matching a pattern).
  - If filter applies to test names, not just file names, we might need to pass that to the worker so it only runs some tests. Bun’s test API might support an env or global filter (like a regex stored in a global that test definitions check).
  - We'll see if Bun had that; but to be safe, allow a mechanism: e.g., environment variable `BUN_TEST_NAME_FILTER` passed to worker processes. The worker’s test harness can then skip tests not matching.
- Lifecycles:
  - Possibly `bun test` has `beforeAll`/`afterAll` hooks per file or globally. Running each file in separate process naturally scope those to file. In our pooling, if we reuse a process for multiple files, global hooks might persist incorrectly. 
  - So, to maintain same semantics, we might choose to use one worker per test file at a time (like do not reuse the same process for a different file without restarting it).
  - But that throws away pooling advantage. Perhaps accept slight difference or find a workaround:
    * A compromise: use one process per file concurrently (so pool size equals concurrency but each process handles exactly one test then terminates).
    * That is effectively what a typical test runner does, but we can pre-spawn them (pool just avoids spawn overhead but they still exit after one test).
    * We could implement that: spawn N workers, assign each a test file and then kill it and spawn a new one if more tests left.
    * This still saves some overhead by parallelizing spawns and running in parallel, but not reusing processes for multiple files. That might actually be fine and simpler.
    * For now, let's do that: treat testPool in a slightly different mode where each worker handles one test then is killed or not reused for another test in same run. But we can reuse in watch mode for new runs by restarting anyway.
    * Actually, easier: do not pool test workers at all, just spawn as many as tests in parallel (or up to concurrency limit). But that could be thousands of processes which is not good for large test suites. So a pool with size limit is needed to throttle concurrency, but each process in pool will be used exactly once then replaced.
    * That is an unusual use of "pool" but workable: we maintain e.g. 4 processes at a time running tests, as one finishes we spawn another to handle next test, etc.
    * Our ProcessPool can support that: when a worker finishes a test, instead of marking it idle for reuse, we can decide to kill and respawn it (fresh state).
    * Could add a flag for testPool to always refresh workers.
    * That ensures isolation at cost of respawn per task, but concurrency maintained. This might be the safest approach to preserve semantics and avoid flaky interactions.
  - We will adopt that to avoid complex context reset logic in test processes.
- Therefore, modifications:
  - Remove any direct calls to run tests internally. Use DAG as above.
  - Ensure reporter output: We might reuse Bun’s existing output formatting by capturing worker output or replicating summary logic.
  - Possibly just rely on each test process printing to stdout; then main collates or prints as they come. If processes print concurrently, output could intermix; maybe we buffer per test file and print sequentially as they complete to avoid garble. That is something we can do: our ProcessPool reader can capture the whole stdout of a test worker, store it, and only print it once the test is done (or print line by line with prefix).
  - Simpler: let test processes do their thing (their output presumably ends with a summary). Many test frameworks print live progress though (like dots or names as they run).
  - Bun's test likely prints each test name and result as it executes. Intermixing them from multiple files running concurrently might be fine if they add some prefix or at least won't break lines. But there is potential confusion.
  - Many test runners handle this by grouping output by test file.
  - Perhaps we do that: intercept all stdout from workers and prefix each line with `[fileName] ` or similar if multiple parallel.
  - Or hold output until file done, then print.
  - We'll lean on simpler: sequentially print after each file done to not scramble.
  - So, ProcessPool for tests collects stdout into a buffer (maybe large but okay if tests output not huge, if huge we could stream with prefix).
  - When test done, Coordinator prints buffer.
  - If a test fails, we ensure the failing details (stack trace) are included in that buffer by Bun.
  - This approach sacrifices seeing live progress, but with parallel tests progress is non-deterministic ordering anyway.
- Set exit code: If any test failed, exit with code 1 (or whatever Bun does). If all passed, exit 0. Coordinator can determine that by aggregating results.

#### src/cli/CLI.ts (or whichever code triggers build/test)
**Purpose:** The user interface between commands and internal logic.
- Ensure that `bun build` and `bun test` commands now invoke the new engine.
- Possibly minimal changes if they already call into `Bundle.zig` or `TestRunner.zig` functions which we modified.
- If any flags or options are passed into those functions, ensure they propagate to our BuildOptions or Coordinator:
  - E.g., `--minify`, `--target bun` etc., we must set in BuildOptions so transpiler knows.
  - `--no-macros` toggles a flag we honor (Graph should check and throw error if macro import used).
  - `--external:<pkg>` or other bundler options: Bun’s bundler may have options for treating certain imports as external (not bundling them). We should support that by instructing Graph to mark those modules as external nodes (which perhaps means skip resolution or treat them as existing provided modules).
    * If external flag exists, implement by onResolve returning a dummy namespace "external" which tells bundler not to include it. Or simpler, don't even create a node for it beyond a placeholder that won’t produce output. We'll need to mimic how esbuild handles externals.
    * Possibly add NodeType ExternalModule where execution is a no-op (just yields nothing but marks as completed so dependents compile with a require call or import left intact).
    * If out of scope, at least ensure not to break by maybe still bundling it.
    * But user expectation is to exclude externals, so we should honor it. We can add a check in Graph.resolveImport: if import path matches an external from options (maybe by exact match or package name prefix), then instead of adding normal node, create an `ExternalModule` node that when executed does nothing (or yields some marker in output if needed).
    * When bundling, how to handle external? Usually bundler leaves `require("pkg")` in output. In our code generation, if a module’s dependency is external, we need to not replace it with bundled code. Possibly easiest: mark that import as not resolved (or resolved to an ExternalModule) and in the final output for the parent module, when printing import, we omit bundling the content and leave an import or require.
    * Since Bun’s bundler might already support it, maybe esbuild’s transform does so given an externals list.
    * If using esbuild’s engine, there might be an option in code generation to treat certain imports as external.
    * If not, maybe simpler: do not actually remove the import in codegen so it remains a runtime import.
    * We'll assume if external, we simply skip bundling that dependency (no output for it).
    * That should be fine; the runtime will then attempt to load it via Node’s resolution at runtime if needed.
  - We ensure any other bundler options are handled (source map, define replacements, etc.):
    * Source map: our Engine can produce external source map because esbuild’s printer can generate it. We have to capture it and write to file if needed. 
    * Defines: Bun might allow replacing global constants. If using esbuild under hood, we can pass define mapping in parser or a post-step. We might need to implement that in AST transform ourselves if esbuild interface not exposed. Possibly Bun had a simple mechanism for a few known values (like replacing `import.meta`).
    * Time limited, but mention that no feature is lost: If Bun had defines support, implement as a macro or plugin in new engine, or a preprocessor in parse (e.g., check AST for those identifiers and replace).
    * Could also be done by injecting a special macro or hooking into parse as needed.

#### src/runtime/ModuleLoader.zig (if the runtime uses a separate loader for ES modules/CommonJS)
**Purpose:** Load modules at runtime (`bun run` or dynamic imports).
- We might not strictly need to change this, as the DAG engine is mostly for bundler/test.
- But mention cross-process caching: We could modify ModuleLoader to check the build cache:
  - For example, if `require('foo.ts')` is called in Bun’s runtime, Bun currently transpiles it on the fly. We could optimize: check if `foo.ts` was compiled in a previous `bun build` and cache artifact exists. If yes, load that JS directly instead of transpiling now.
  - This coordination could be done via the cache file we generate. E.g., we store artifacts in `bun-dag-cache/`.
  - The loader could hash the file content quickly, see if there's a compiled output with that hash. If yes and cache version matches runtime compatibility, evaluate that instead.
  - This is speculative but aligns with "runtime DAG-aware". It means even if user didn’t run bun build, bun run could use cached compilations from a prior run or share with test runner.
  - Another scenario: the test worker processes could use this mechanism to avoid recompiling code that the build process already compiled.
  - We propose adding this:
    * When Bun’s ModuleLoader goes to compile a TS/JS file (especially TS or JSX), compute hash, look in `~/.bun/<projecthash>/cache/<hash>.js` or similar. (We need a project identifier to not mix caches from different projects).
    * If found, load that code via evaluate.
    * If not, do normal JIT compilation (and optionally store it to cache for future).
  - This could drastically speed up starting a server in dev after doing a build.
  - Implementation details might be heavy but concept is straightforward.
- Not mandated by user request explicitly, but “runtime DAG-aware across worker boundaries” suggests something like this.
- If time, mention that we implement a hook in ModuleLoader to use Versioning data:
  - Possibly reuse the same cache file we wrote (so unify location).
  - We must ensure the compiled code is compatible with runtime environment (it should, since Bun uses same JS engine).
  - We also should ensure if user edits file after build, running it uses updated code (so maybe check timestamps or content match).
  - The simplest: only use cache if content hash matches exactly the one in cache (so we recompute content hash at runtime which is fine for a warm start).
  - This is safe and ensures no stale code runs.

#### src/cli/OutputFormatting.zig (or wherever error messages and such are formatted)
- Ensure that any error messages produced by DAG (especially plugin errors, macro errors) are formatted consistently:
  - Bun might have standardized error prints with colors, source snippet and pointer to error location.
  - If our macro error just gives "Error: something in macro X", we should try to include file name and line. Possibly our MacroInvoke can capture stack or at least catch a `Error` object with stack property via JSC, and then our Coordinator prints it.
  - For plugin errors, plugin might throw an error string; we should present it prefixed with plugin name maybe (like `[PluginName] Error: message`).
  - For test failures, output likely comes from Bun’s test framework already formatted; we will just relay it (maybe adding file name context).
- If any integration needed with source maps for error stack traces (like if bundler errors, though bundler errors are at compile time so mostly source positions already known).
- Possibly update CLI help text to reflect any new behavior (though ideally user-facing behavior remains same except performance improvements).

Overall, while the introduction of `bun-meta-dag` significantly changes internal architecture, the above modifications ensure that externally Bun behaves the same or better (faster builds, parallel tests, incremental watch) with no lost features. Existing plugin and macro APIs continue to work, test outcomes remain the same but run concurrently, and runtime usage optionally benefits from cached compilation. The integration plan avoids phase-by-phase toggles; it would replace the old system in one coherent update, delivering a state-of-the-art DAG-based build and runtime engine for Bun.