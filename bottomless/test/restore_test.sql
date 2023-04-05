.bail on
.echo on
.load ../../target/debug/bottomless
.open file:test.db?wal=bottomless&immutable=1
pragma journal_mode;
.mode column
SELECT v, length(v) FROM test;
