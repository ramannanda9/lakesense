window.BENCHMARK_DATA = {
  "lastUpdate": 1774915628912,
  "repoUrl": "https://github.com/ramannanda9/lakesense",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "ramannanda9@gmail.com",
            "name": "Ramandeep Singh",
            "username": "ramannanda9"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5a1a45e520ceef5ec1ef82242c02c6ab79882f30",
          "message": "ci: add non-blocking benchmark job (#6)\n\n* ci: add non-blocking benchmark job on main pushes\n\nRuns pytest-benchmark on push to main only (not PRs) to avoid noise\nduring development. Results are uploaded as artifacts (90-day retention)\nand tracked as a trend via github-action-benchmark on the gh-pages branch.\nNo hard failure gate — purely observational at this stage.\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>\n\n* ci: run benchmarks on PRs too with 200% regression alert\n\nRemoves the main-only gate so regressions are visible during code review.\nOn PRs: runs benchmarks and posts a comment if any benchmark is >2x slower\nthan the stored baseline. On main: same, plus pushes updated trend data to\ngh-pages. Never fails the build.\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>\n\n* ci: rename bench_core.py to test_bench_core.py for pytest discovery\n\nAvoids needing python_files config workaround. testpaths still points to\ntests/ only so benchmarks don't run on a plain pytest invocation.\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>\n\n---------\n\nCo-authored-by: Claude Sonnet 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-03-30T17:01:16-07:00",
          "tree_id": "49b3823eb9af7cb3606e161785a83fff318b71d5",
          "url": "https://github.com/ramannanda9/lakesense/commit/5a1a45e520ceef5ec1ef82242c02c6ab79882f30"
        },
        "date": 1774915628531,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[1000]",
            "value": 2878.922217867268,
            "unit": "iter/sec",
            "range": "stddev: 0.000018335645703378766",
            "extra": "mean: 347.3522118082124 usec\nrounds: 2710"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[10000]",
            "value": 279.07740447141157,
            "unit": "iter/sec",
            "range": "stddev: 0.00013936891451417916",
            "extra": "mean: 3.5832352744359826 msec\nrounds: 266"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[100000]",
            "value": 30.755818973170477,
            "unit": "iter/sec",
            "range": "stddev: 0.0015436343560690186",
            "extra": "mean: 32.51417238709656 msec\nrounds: 31"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[1000]",
            "value": 6239.409612281835,
            "unit": "iter/sec",
            "range": "stddev: 0.00000940275605517573",
            "extra": "mean: 160.27157409758306 usec\nrounds: 5513"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[10000]",
            "value": 606.5173505413541,
            "unit": "iter/sec",
            "range": "stddev: 0.00006006182779403309",
            "extra": "mean: 1.648757449572446 msec\nrounds: 585"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[100000]",
            "value": 67.27667059283688,
            "unit": "iter/sec",
            "range": "stddev: 0.0003362333573378057",
            "extra": "mean: 14.86399358333396 msec\nrounds: 60"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[1000]",
            "value": 4310.583954905658,
            "unit": "iter/sec",
            "range": "stddev: 0.000011279295333013163",
            "extra": "mean: 231.98712992515792 usec\nrounds: 3071"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[10000]",
            "value": 376.9043408833862,
            "unit": "iter/sec",
            "range": "stddev: 0.000021332917727446735",
            "extra": "mean: 2.6531931090424847 msec\nrounds: 376"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[100000]",
            "value": 39.36586954078117,
            "unit": "iter/sec",
            "range": "stddev: 0.00081597512988885",
            "extra": "mean: 25.40271589743617 msec\nrounds: 39"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[7]",
            "value": 3547.673039588484,
            "unit": "iter/sec",
            "range": "stddev: 0.000013818206437619702",
            "extra": "mean: 281.87490471669736 usec\nrounds: 3159"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[30]",
            "value": 1435.959749567757,
            "unit": "iter/sec",
            "range": "stddev: 0.00002307168214993588",
            "extra": "mean: 696.3983498151764 usec\nrounds: 1355"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[90]",
            "value": 1028.4973276096423,
            "unit": "iter/sec",
            "range": "stddev: 0.00001942231883874354",
            "extra": "mean: 972.2922686869068 usec\nrounds: 990"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[7]",
            "value": 17524.92491272509,
            "unit": "iter/sec",
            "range": "stddev: 0.000002806860380016089",
            "extra": "mean: 57.061585426473705 usec\nrounds: 11267"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[30]",
            "value": 5939.3728173687405,
            "unit": "iter/sec",
            "range": "stddev: 0.00002483540035951019",
            "extra": "mean: 168.36794569885575 usec\nrounds: 5359"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[90]",
            "value": 2640.8787974425277,
            "unit": "iter/sec",
            "range": "stddev: 0.00002390835173339917",
            "extra": "mean: 378.66183066349623 usec\nrounds: 2622"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[7]",
            "value": 3656.9084145171646,
            "unit": "iter/sec",
            "range": "stddev: 0.000012438505389530805",
            "extra": "mean: 273.4550299455705 usec\nrounds: 2204"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[30]",
            "value": 1390.8873220139421,
            "unit": "iter/sec",
            "range": "stddev: 0.00002068640222558219",
            "extra": "mean: 718.9655007797793 usec\nrounds: 1282"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[90]",
            "value": 1043.4515830612409,
            "unit": "iter/sec",
            "range": "stddev: 0.000035102203580497604",
            "extra": "mean: 958.357834933017 usec\nrounds: 1042"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[1000]",
            "value": 169.36650327430394,
            "unit": "iter/sec",
            "range": "stddev: 0.0007728291757317921",
            "extra": "mean: 5.904355233575391 msec\nrounds: 137"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[10000]",
            "value": 83.13153370016536,
            "unit": "iter/sec",
            "range": "stddev: 0.0008463550131993939",
            "extra": "mean: 12.029129687499207 msec\nrounds: 80"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[100000]",
            "value": 11.809206050406512,
            "unit": "iter/sec",
            "range": "stddev: 0.0009643637474663422",
            "extra": "mean: 84.67969783333373 msec\nrounds: 12"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[1000]",
            "value": 102.1702380606979,
            "unit": "iter/sec",
            "range": "stddev: 0.0020926644446288716",
            "extra": "mean: 9.787586081632835 msec\nrounds: 98"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[10000]",
            "value": 25.216781909184505,
            "unit": "iter/sec",
            "range": "stddev: 0.0009623035158290453",
            "extra": "mean: 39.65613073077252 msec\nrounds: 26"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[10]",
            "value": 994.7648674041108,
            "unit": "iter/sec",
            "range": "stddev: 0.000017381632035948482",
            "extra": "mean: 1.005262683441516 msec\nrounds: 616"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[100]",
            "value": 515.283203327745,
            "unit": "iter/sec",
            "range": "stddev: 0.00031969059585774484",
            "extra": "mean: 1.9406803744851582 msec\nrounds: 486"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[500]",
            "value": 122.141851971071,
            "unit": "iter/sec",
            "range": "stddev: 0.007526702631510975",
            "extra": "mean: 8.187201879310358 msec\nrounds: 174"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[10]",
            "value": 433.2120702777373,
            "unit": "iter/sec",
            "range": "stddev: 0.00006148567365940567",
            "extra": "mean: 2.3083382680424585 msec\nrounds: 97"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[100]",
            "value": 77.63951019518747,
            "unit": "iter/sec",
            "range": "stddev: 0.00011419296611439238",
            "extra": "mean: 12.880040039999963 msec\nrounds: 75"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[500]",
            "value": 16.55082486346599,
            "unit": "iter/sec",
            "range": "stddev: 0.00039583958841118066",
            "extra": "mean: 60.419949352940286 msec\nrounds: 17"
          }
        ]
      }
    ]
  }
}