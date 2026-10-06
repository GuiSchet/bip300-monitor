#!/usr/bin/env bash
# Independent operator-verifier regression test in the dedicated local test DB.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
container="${BIP300_TEST_CONTAINER:-observer-v7-tests}"
[[ "$container" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]] || die 'invalid test container identifier'
database="bip300_test_verifier_$$"
docker exec "$container" createdb -U postgres "$database"
trap 'docker exec "$container" dropdb -U postgres "$database" >/dev/null' EXIT
postgres_query() {
    local statement="$1"
    shift
    docker exec -i "$container" psql -XAtq -U postgres -d "$database" -v ON_ERROR_STOP=1 "$@" <<<"$statement"
}
NETWORK_ID="test"
ECASH_ACTIVATION_HEIGHT=1
ECASH_ACTIVATION_BLOCK_HASH="$(printf '%064d' 1)"
MONITOR_EVENT_CONTRACT_VERSION=8
node_cli() { [[ "$1" == getblockhash ]] && printf '%064d\n' "$2"; }
postgres_query "
CREATE TABLE dataset_manifest(dataset_id uuid,network_id text,activation_height integer,activation_block_hash text,initial_event_contract_version integer);
CREATE TABLE extractor_run(run_id uuid,event_contract_version integer,source text,status text);
CREATE TABLE extractor_status(dataset_id uuid,run_id uuid,source text);
CREATE TABLE extractor_worker_status(run_id uuid,worker text,last_success_at timestamptz,last_error text);
CREATE TABLE event(id bigint,dataset_id uuid,event_contract_version integer,source text,kind text,sidechain smallint,sidechain_instance_id text,block_hash bytea,previous_hash bytea,height integer);
CREATE TABLE current_sidechain_instance(dataset_id uuid,sidechain smallint,sidechain_instance_id text);
CREATE TABLE event_conflict(first_event_id bigint,conflicting_event_id bigint);
CREATE TABLE history_coverage(dataset_id uuid,event_contract_version integer,source text,stream text,sidechain smallint,sidechain_instance_id text,status text,next_hash bytea,covered_tip_hash bytea,covered_tip_height integer,target_tip_hash bytea,target_tip_height integer,coverage_start_height integer);
INSERT INTO dataset_manifest VALUES('11111111-1111-4111-8111-111111111111','test',1,lpad('1',64,'0'),8);
INSERT INTO extractor_run VALUES('22222222-2222-4222-8222-222222222222',8,'enforcer','running');
INSERT INTO extractor_status VALUES('11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222','enforcer');
INSERT INTO extractor_run VALUES('44444444-4444-4444-8444-444444444444',8,'node','running');
INSERT INTO extractor_status VALUES('11111111-1111-4111-8111-111111111111','44444444-4444-4444-8444-444444444444','node');
INSERT INTO extractor_worker_status SELECT '22222222-2222-4222-8222-222222222222',unnest(ARRAY['mainchain_tip','bmm_requests','mainchain_events']),now(),NULL;
INSERT INTO event SELECT n,'11111111-1111-4111-8111-111111111111',8,'node','mainchain_block',NULL,NULL,decode(lpad(n::text,64,'0'),'hex'),decode(lpad((n-1)::text,64,'0'),'hex'),n FROM generate_series(1,3) n;
INSERT INTO history_coverage VALUES('11111111-1111-4111-8111-111111111111',8,'node','mainchain_block',NULL,NULL,'complete',NULL,decode(lpad('3',64,'0'),'hex'),3,decode(lpad('3',64,'0'),'hex'),3,1);
"
record_current_workers_are_healthy || die 'three independent healthy workers rejected'
record_node_history_is_complete 1 3 || die 'canonical chain rejected'
record_has_global_block mainchain_block "$(printf '%064d' 2)" || die 'scoped block rejected'
postgres_query "UPDATE event SET block_hash=decode(lpad('9',64,'0'),'hex') WHERE height=2;"
if record_node_history_is_complete 1 3; then die 'orphan at missing canonical height accepted'; fi
postgres_query "UPDATE event SET block_hash=decode(lpad('2',64,'0'),'hex'),event_contract_version=6 WHERE height=2;"
if record_has_global_block mainchain_block "$(printf '%064d' 2)"; then die 'wrong contract accepted'; fi
if record_node_history_is_complete 1 3; then die 'wrong-contract prefix certified'; fi
postgres_query "UPDATE event SET event_contract_version=8,dataset_id='33333333-3333-4333-8333-333333333333' WHERE height=2;"
if record_has_global_block mainchain_block "$(printf '%064d' 2)"; then die 'wrong dataset accepted'; fi
postgres_query "UPDATE event SET dataset_id='11111111-1111-4111-8111-111111111111' WHERE height=2; INSERT INTO event_conflict VALUES(2,4);"
if record_node_history_is_complete 1 3; then die 'conflicted prefix certified'; fi
postgres_query "DELETE FROM event_conflict; UPDATE history_coverage SET covered_tip_hash=decode(lpad('9',64,'0'),'hex'),target_tip_hash=decode(lpad('9',64,'0'),'hex'); UPDATE event SET block_hash=decode(lpad('9',64,'0'),'hex') WHERE height=3;"
if record_node_history_is_complete 1 3; then die 'tip differing from node accepted'; fi
postgres_query "ALTER TABLE event ADD COLUMN payload jsonb;
CREATE TABLE event_observation(event_id bigint,dataset_id uuid,run_id uuid,capture_method text,observed_at timestamptz);
INSERT INTO event(id,dataset_id,event_contract_version,source,kind,block_hash,payload) VALUES
(10,'11111111-1111-4111-8111-111111111111',8,'enforcer','mainchain_transition',decode(lpad('4',64,'0'),'hex'),'{\"monitor_event\":{\"Enforcer\":{\"event\":{\"MainchainTransition\":{\"action\":1}}}}}');
INSERT INTO event_observation VALUES(10,'11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222','live',now());"
record_has_live_mainchain_connect "$(printf '%064d' 4)" '2000-01-01T00:00:00Z' || die 'global live connect rejected without slots'
postgres_query "UPDATE event_observation SET capture_method='backfill';"
if record_has_live_mainchain_connect "$(printf '%064d' 4)" '2000-01-01T00:00:00Z'; then die 'backfill accepted as live'; fi
postgres_query "UPDATE event_observation SET capture_method='live',run_id='44444444-4444-4444-8444-444444444444';"
if record_has_live_mainchain_connect "$(printf '%064d' 4)" '2000-01-01T00:00:00Z'; then die 'different run accepted as current live'; fi
postgres_query "ALTER TABLE event_observation ADD COLUMN capture_seq bigint;
ALTER TABLE event_observation ADD COLUMN snapshot_group_id uuid;
CREATE TABLE snapshot_group(snapshot_group_id uuid,dataset_id uuid,run_id uuid,consistency text,revision_before bigint,revision_after bigint,tip_before_hash bytea,tip_after_hash bytea,tip_before_height integer,tip_after_height integer);
INSERT INTO current_sidechain_instance VALUES('11111111-1111-4111-8111-111111111111',9,'instance9');
INSERT INTO event(id,dataset_id,event_contract_version,source,kind,sidechain,sidechain_instance_id) VALUES(20,'11111111-1111-4111-8111-111111111111',8,'enforcer','ctip',9,'instance9');
INSERT INTO snapshot_group VALUES('55555555-5555-4555-8555-555555555555','11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222','tip_matched',NULL,NULL,decode(lpad('4',64,'0'),'hex'),decode(lpad('4',64,'0'),'hex'),4,4);
INSERT INTO event_observation VALUES(20,'11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222','poll',now(),1,'55555555-5555-4555-8555-555555555555');"
record_current_state_is_usable ctip 9 || die 'independent CTIP observation rejected'
postgres_query "INSERT INTO snapshot_group SELECT '66666666-6666-4666-8666-666666666666',dataset_id,run_id,'changed',NULL,NULL,tip_before_hash,tip_after_hash,tip_before_height,tip_after_height FROM snapshot_group;
INSERT INTO event_observation VALUES(20,'11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222','poll',now(),2,'66666666-6666-4666-8666-666666666666');"
if record_current_state_is_usable ctip 9; then die 'latest changed occurrence borrowed old quality'; fi
postgres_query "INSERT INTO event_observation VALUES(20,'11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222','poll',now(),3,'55555555-5555-4555-8555-555555555555');"
record_current_state_is_usable ctip 9 || die 'recovery to valid occurrence rejected'
info 'scoped canonical-hash SQL verification passed'
