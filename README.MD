Docker compose file to launch emulators is in library folder.

Breaking change: listing APIs now return ordered `List` results instead of `Set`.
UBSA preserves provider/native listing order, does not deduplicate results, and keeps duplicate entries if the underlying SDK returns them.
