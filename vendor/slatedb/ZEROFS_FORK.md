# ZeroFS SlateDB fork

This directory vendors the exact SlateDB fork previously pinned by ZeroFS at
`Barre/slatedb@230ade681f1fdf0f85fc1c2cb6c118575a83ceb1`.

ZeroFS keeps this source in-tree because its production branch protocol needs
an opt-in shared-source-pin clone mode that is not available in the upstream
crate. Ordinary SlateDB clones retain their original private-pin semantics.
The ZeroFS mode may only be used with permanent, catalog-authenticated source
checkpoints; clone detach and database cleanup must never delete those borrowed
pins.

Keep local changes narrowly marked with `ZeroFS` comments and retain the
upstream Apache-2.0 license in this directory.
