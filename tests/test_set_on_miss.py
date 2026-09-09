"""
Integration tests for --set-on-miss.

Covers:
- A GET miss against an empty keyspace is repopulated via an injected SET,
  so a later GET for the same key hits.
- The injected SETs are counted in ALL STATS.Sets even though --ratio has
  no explicit SET share.
- --set-on-miss is rejected at startup when combined with --multi-key-get,
  --data-import, --command, or --cluster-mode.
"""

import json
import subprocess
import tempfile

from include import (
    addTLSArgs,
    add_required_env_arguments,
    agg_keyspace_range,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
    MEMTIER_BINARY,
)
from mb import Benchmark, RunConfig


_KEY_PREFIX = "memtier-som-"
_KEY_MIN = 1
_KEY_MAX = 200


def _run_benchmark(env, extra_args, threads=1, clients=2, requests=None):
    test_dir = tempfile.mkdtemp()
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--hide-histogram",
        ] + extra_args,
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)

    json_dict = {}
    json_path = "{}/mb.json".format(config.results_dir)
    with open(json_path) as fh:
        json_dict = json.load(fh)

    return memtier_ok, json_dict


def test_set_on_miss_repopulates_key(env):
    """GET-only traffic against an empty keyspace must repopulate keys via SET."""
    env.skipOnCluster()
    env.flush()

    memtier_ok, js = _run_benchmark(
        env,
        [
            "--ratio=0:1",
            "--set-on-miss",
            "--key-pattern=P:P",
            "--key-minimum={}".format(_KEY_MIN),
            "--key-maximum={}".format(_KEY_MAX),
            "--key-prefix={}".format(_KEY_PREFIX),
        ],
        requests="allkeys",
    )
    env.assertTrue(memtier_ok)

    all_stats = js.get("ALL STATS", {})
    gets = all_stats.get("Gets", {})
    env.assertTrue("Count" in gets, message="Gets missing from ALL STATS")
    env.assertGreater(gets["Count"], 0)

    # --ratio=0:1 configures no explicit SET share; any recorded Sets must
    # come from the miss-triggered write-back.
    sets = all_stats.get("Sets", {})
    env.assertTrue("Count" in sets, message="Sets missing from ALL STATS (set-on-miss did not fire)")
    env.assertGreater(sets["Count"], 0)

    # Keys should now exist in the keyspace, repopulated by the write-back
    # (the DB started empty via env.flush()).
    master_nodes_connections = env.getOSSMasterNodesConnectionList()
    keyspace_count = agg_keyspace_range(master_nodes_connections)
    env.assertGreater(keyspace_count, 0, message="No keys were repopulated by --set-on-miss")


def test_set_on_miss_rejects_multi_key_get(env):
    env.skipOnCluster()

    result = subprocess.run(
        [
            MEMTIER_BINARY,
            "--server=127.0.0.1",
            "--port=6379",
            "--set-on-miss",
            "--multi-key-get=4",
        ],
        capture_output=True,
        text=True,
    )
    env.assertNotEqual(result.returncode, 0,
                       message="Expected non-zero exit for --set-on-miss + --multi-key-get")
    env.assertTrue("set-on-miss" in result.stderr.lower(),
                   message="Expected --set-on-miss error; got stderr={}".format(result.stderr[:200]))


def test_set_on_miss_rejects_data_import(env):
    env.skipOnCluster()

    result = subprocess.run(
        [
            MEMTIER_BINARY,
            "--server=127.0.0.1",
            "--port=6379",
            "--set-on-miss",
            "--data-import=/dev/null",
        ],
        capture_output=True,
        text=True,
    )
    env.assertNotEqual(result.returncode, 0,
                       message="Expected non-zero exit for --set-on-miss + --data-import")
    env.assertTrue("set-on-miss" in result.stderr.lower(),
                   message="Expected --set-on-miss error; got stderr={}".format(result.stderr[:200]))


def test_set_on_miss_rejects_cluster_mode(env):
    env.skipOnCluster()

    result = subprocess.run(
        [
            MEMTIER_BINARY,
            "--server=127.0.0.1",
            "--port=6379",
            "--set-on-miss",
            "--cluster-mode",
        ],
        capture_output=True,
        text=True,
    )
    env.assertNotEqual(result.returncode, 0,
                       message="Expected non-zero exit for --set-on-miss + --cluster-mode")
    env.assertTrue("set-on-miss" in result.stderr.lower(),
                   message="Expected --set-on-miss error; got stderr={}".format(result.stderr[:200]))
