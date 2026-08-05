# Source manifest inputs

`schema_sanitizer.sources.discover` returns a `SourceManifest` that freezes the
exact remote objects selected by one discovery step. It can be supplied
directly to `iter_batches` and every analytical or file-output converter.

Version one supports GCS manifests. Every entry must:

- belong to the declared `source_uri` bucket and prefix;
- use a supported GCS URI;
- carry a non-empty immutable `generation`;
- have a file extension compatible with the explicit `input_format`;
- have a unique `(uri, generation)` content identity.

The converter never lists `source_uri`. It stages only the frozen entries and
passes each `RemoteFile` generation to the GCS download path, which requests the
same generation and applies `ifGenerationMatch`. Local staging and cleanup use
the same bounded lifecycle as remote directory inputs.

The generated `source_file` column stores the original object URI. Public
statistics additionally expose:

- `source_manifest_uri`;
- `source_object_count`;
- `source_objects`, ordered dictionaries containing `uri` and `generation`.

A manifest may be reused across schema inference and final materialization. Its
immutable identities ensure that both operations target the same object
versions even when the current object at a URI is later replaced.
