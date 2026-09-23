#!/usr/bin/env bash
# pg_rewind --restore-target-wal against a wal-g file archive, with the first WAL fetch killed part way, for the unpatched `wal-g wal-fetch %f %p` and for the plugin's atomic fetch (the integration-tagged test binary). Runs as the postgres user inside the postgres image, with wal-g and restore-helper on PATH. Usage: rewind-integration.sh <plain|atomic> <kill|none> <prefetch: inside|outside>
#
# wal-g prefetches the next segments into <dir of the destination>/.wal-g/prefetch unless WALG_PREFETCH_DIR says otherwise, and the plugin does not set it, so in the cluster that directory sits inside pg_wal while pg_rewind walks and rewrites pg_wal. "inside" reproduces that; "outside" moves it away so a run isolates what an interrupted fetch leaves at its destination.
set -euo pipefail
mode=$1 kill_after=$2 prefetch=$3
[ "$kill_after" = none ] && kill_after=
base=$(mktemp -d)
P=$base/p S=$base/s
export WALG_FILE_PREFIX=$base/archive PGHOST=$base PGUSER=postgres
mkdir -p "$WALG_FILE_PREFIX"
[ "$prefetch" = outside ] && export WALG_PREFETCH_DIR=$base/prefetch

initdb -D "$P" -A trust >/dev/null
cat >> "$P/postgresql.conf" <<CONF
port = 5432
unix_socket_directories = '$base'
wal_log_hints = on
archive_mode = on
archive_command = 'PGPORT=5432 wal-g wal-push %p'
wal_keep_size = 0
max_wal_size = 64MB
min_wal_size = 32MB
CONF
pg_ctl -D "$P" -l "$base/p.log" -w start >/dev/null
psql -p 5432 -qc "create table t (v text)"
pg_basebackup -p 5432 -D "$S" -R -X stream
sed -i 's/^port = 5432/port = 5433/' "$S/postgresql.conf"
pg_ctl -D "$S" -l "$base/s.log" -w start >/dev/null
psql -p 5432 -qc "insert into t select repeat('a', 500) from generate_series(1, 20000)"
until [ "$(psql -p 5433 -Atc 'select count(*) from t')" = 20000 ]; do sleep 0.2; done
pg_ctl -D "$S" -w promote >/dev/null

# The old primary carries on alone for several segments and checkpoints, so the WAL pg_rewind has to read back to the divergence point is no longer in its pg_wal and has to come from the archive.
for i in $(seq 1 6); do
  psql -p 5432 -qAtc "insert into t select repeat('b', 500) from generate_series(1, 20000)" -c "select pg_switch_wal()" >/dev/null
done
psql -p 5432 -qc checkpoint
last=$(psql -p 5432 -Atc "select pg_walfile_name(pg_switch_wal())")
until [ "$(psql -p 5432 -Atc "select coalesce(last_archived_wal, '') >= '$last' from pg_stat_archiver")" = t ]; do sleep 0.5; done
pg_ctl -D "$P" -w -m fast stop >/dev/null

# One restore_command for both runs. The first fetch pg_rewind makes is killed the moment its output file appears, at the destination for plain wal-fetch and at the sibling for the atomic fetch, which is the window an interrupted fetch leaves behind; every later fetch runs to completion. A timed kill cannot hit that window reliably against a local archive, where a whole segment arrives in a few milliseconds.
cat > "$base/restore.sh" <<SH
#!/usr/bin/env bash
if [ "$mode" = plain ]; then fetch=(wal-g wal-fetch "\$1" "\$2"); else fetch=(env RESTORE_SOURCE="\$1" RESTORE_DEST="\$2" restore-helper -test.run=TestRestoreCommand); fi
if [ -n "$kill_after" ] && [ ! -e "$base/killed" ]; then
  touch "$base/killed"
  "\${fetch[@]}" >/dev/null &
  pid=\$!
  while kill -0 \$pid 2>/dev/null; do
    if [ -e "\$2" ] || [ -e "\$2.walg-partial" ]; then for q in /proc/[0-9]*; do tr '\\0' ' ' < \$q/cmdline 2>/dev/null | grep -q "^wal-g wal-fetch \$1 " && kill -9 \${q#/proc/}; done; echo "killed wal-g for \$1 at \$(stat -c %s "\$2" "\$2.walg-partial" 2>/dev/null | tr '\\n' ' ')bytes" >> "$base/kills"; break; fi
  done
  wait \$pid
  exit \$?
fi
exec "\${fetch[@]}" >/dev/null
SH
chmod +x "$base/restore.sh"
rewind() {
  grep -q "^restore_command = '$base/restore.sh" "$P/postgresql.conf" || echo "restore_command = '$base/restore.sh %f %p'" >> "$P/postgresql.conf"
  pg_rewind -D "$P" --source-server="port=5433 host=$base user=postgres" --restore-target-wal > "$base/rewind$1.log" 2>&1
}
first=ok; rewind 1 || first=failed
left=$(find "$P/pg_wal" -maxdepth 1 -type f -regextype posix-extended -regex '.*/[0-9A-F]{24}' -size -16777216c -printf '%f=%s ' | tr -d '\n')
partials=$(find "$P/pg_wal" -maxdepth 1 -name '*.walg-partial' -printf '%f ' | tr -d '\n')
second=ok; rewind 2 || second=failed
started=no
if [ $second = ok ]; then
  # The rewound primary can ask to stream from the next segment boundary, which an idle new primary has not written yet; give it WAL past that point.
  psql -p 5433 -qAtc "insert into t values ('after rewind')" -c "select pg_switch_wal()" >/dev/null
  touch "$P/standby.signal"
  printf "port = 5432\nprimary_conninfo = 'port=5433 host=$base user=postgres'\n" >> "$P/postgresql.auto.conf"
  if pg_ctl -D "$P" -l "$base/p2.log" -w -t 60 start >/dev/null; then
    for _ in $(seq 1 240); do
      [ "$(psql -p 5433 -Atc "select count(*) from pg_stat_replication where state = 'streaming'")" = 1 ] && { started=streaming; break; }
      sleep 0.5
    done
  fi
fi
echo "$mode kill=${kill_after:+on-create}${kill_after:-none} prefetch=$prefetch: $(cat "$base/kills" 2>/dev/null || echo 'no kill'); first rewind $first ($(grep -m1 -E 'error|fatal' "$base/rewind1.log" || tail -1 "$base/rewind1.log")); short segments left in pg_wal: ${left:-none}; partial files: ${partials:-none}; second rewind $second ($(tail -1 "$base/rewind2.log")); old primary $started"
[ "$started" = streaming ] || { echo "--- old primary's log after the rewind:"; tail -15 "$base/p2.log" 2>/dev/null || true; }
pg_ctl -D "$P" -w -m immediate stop >/dev/null 2>&1 || true
pg_ctl -D "$S" -w -m immediate stop >/dev/null 2>&1 || true
if [ "$mode" = atomic ] && { [ $second != ok ] || [ "$started" != streaming ]; }; then exit 1; fi
