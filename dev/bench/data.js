window.BENCHMARK_DATA = {
  "lastUpdate": 1784929146241,
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
      },
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
          "id": "894c53e4be5c3798a8144ee4a17f9ff768c2463e",
          "message": "fix(sketches): word n-gram tokenization for compute_minhash (#7)\n\n* fix(sketches): replace whitespace tokenization with word n-grams in compute_minhash\n\nNaive whitespace splitting produced unreliable Jaccard signals — identical\nbag-of-words with different word order scored as identical, and short strings\ngenerated too few tokens. Replace with word bigrams (unigrams + bigrams) as\nthe default, add char shingles for structured/ID strings, and keep whitespace\nas a legacy opt-in.\n\nAlso adds tokenizer consistency guards: build_baseline raises if minhash\nrecords with mixed tokenizers are merged, and compute_signals raises if\ncurrent and baseline were built with different tokenizers.\n\nNote: existing baselines stored under whitespace tokenization are invalidated\nby this change and must be rebuilt.\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>\n\n* chore: bump version to 0.2.1\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>\n\n* chore: single-source version from lakesense/__init__.py via hatchling dynamic\n\nRemoves duplicate version field from pyproject.toml. Hatchling now reads\n__version__ from lakesense/__init__.py — one place to bump going forward.\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>\n\n* docs: document MinHash tokenizers and v0.2.1 in README\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>\n\n* perf(sketches): eliminate per-iteration branch and list alloc in compute_minhash\n\nBranch was resolved inside the hot loop, paying if/elif/else + function call\n+ list copy on every value. Move branch outside the loop and inline word_ngram\nlogic directly — uses string concat instead of join for bigrams which avoids\na slice allocation per pair.\n\nRecovers the benchmark regression introduced by the n-gram patch.\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>\n\n---------\n\nCo-authored-by: Claude Sonnet 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-03-31T12:38:13-07:00",
          "tree_id": "28b3b2bd23670a1504444d44bc74666a18964c43",
          "url": "https://github.com/ramannanda9/lakesense/commit/894c53e4be5c3798a8144ee4a17f9ff768c2463e"
        },
        "date": 1774985994878,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[1000]",
            "value": 2012.4405654217896,
            "unit": "iter/sec",
            "range": "stddev: 0.000010518503554750962",
            "extra": "mean: 496.90908500962803 usec\nrounds: 1541"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[10000]",
            "value": 195.78754276396413,
            "unit": "iter/sec",
            "range": "stddev: 0.00003965397128808187",
            "extra": "mean: 5.107577253807059 msec\nrounds: 197"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[100000]",
            "value": 21.428652488711105,
            "unit": "iter/sec",
            "range": "stddev: 0.002710751614984372",
            "extra": "mean: 46.66649013636359 msec\nrounds: 22"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[1000]",
            "value": 5930.892158142636,
            "unit": "iter/sec",
            "range": "stddev: 0.000020950371795343347",
            "extra": "mean: 168.60869719694378 usec\nrounds: 5601"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[10000]",
            "value": 623.4684105795924,
            "unit": "iter/sec",
            "range": "stddev: 0.000020309027276582314",
            "extra": "mean: 1.6039305007776963 msec\nrounds: 643"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[100000]",
            "value": 67.73545158424564,
            "unit": "iter/sec",
            "range": "stddev: 0.00007065255230956797",
            "extra": "mean: 14.76331782857098 msec\nrounds: 70"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[1000]",
            "value": 4176.207204050201,
            "unit": "iter/sec",
            "range": "stddev: 0.000012886418418576947",
            "extra": "mean: 239.45172045825993 usec\nrounds: 3055"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[10000]",
            "value": 380.06916384622804,
            "unit": "iter/sec",
            "range": "stddev: 0.00005318578362630601",
            "extra": "mean: 2.631100060526324 msec\nrounds: 380"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[100000]",
            "value": 38.60549481094727,
            "unit": "iter/sec",
            "range": "stddev: 0.00010342816857295594",
            "extra": "mean: 25.903048384615765 msec\nrounds: 39"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[7]",
            "value": 3519.640895606407,
            "unit": "iter/sec",
            "range": "stddev: 0.0000065564074293254734",
            "extra": "mean: 284.11989451773536 usec\nrounds: 2882"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[30]",
            "value": 1407.3694602309827,
            "unit": "iter/sec",
            "range": "stddev: 0.000007324763284125109",
            "extra": "mean: 710.545473848691 usec\nrounds: 1281"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[90]",
            "value": 1023.2070187295831,
            "unit": "iter/sec",
            "range": "stddev: 0.000007721330604303146",
            "extra": "mean: 977.3193319584566 usec\nrounds: 970"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[7]",
            "value": 17257.364526213856,
            "unit": "iter/sec",
            "range": "stddev: 0.000004409452077583821",
            "extra": "mean: 57.94627554404409 usec\nrounds: 10844"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[30]",
            "value": 6077.975917649066,
            "unit": "iter/sec",
            "range": "stddev: 0.0000047305705532103465",
            "extra": "mean: 164.52845709642028 usec\nrounds: 4918"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[90]",
            "value": 2554.3804727179986,
            "unit": "iter/sec",
            "range": "stddev: 0.000007428548322814888",
            "extra": "mean: 391.48435821541733 usec\nrounds: 2331"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[7]",
            "value": 3416.8308981470595,
            "unit": "iter/sec",
            "range": "stddev: 0.000008451282277040415",
            "extra": "mean: 292.6688588956211 usec\nrounds: 2771"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[30]",
            "value": 1369.472095400612,
            "unit": "iter/sec",
            "range": "stddev: 0.000008295196068084812",
            "extra": "mean: 730.2083798264395 usec\nrounds: 1269"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[90]",
            "value": 1010.5867490806456,
            "unit": "iter/sec",
            "range": "stddev: 0.000009089863812345367",
            "extra": "mean: 989.524156050654 usec\nrounds: 942"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[1000]",
            "value": 174.4956024256475,
            "unit": "iter/sec",
            "range": "stddev: 0.00012744878745901906",
            "extra": "mean: 5.730803447760809 msec\nrounds: 134"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[10000]",
            "value": 83.32099076866663,
            "unit": "iter/sec",
            "range": "stddev: 0.00016182455527600085",
            "extra": "mean: 12.001777592592623 msec\nrounds: 81"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[100000]",
            "value": 11.859064681399456,
            "unit": "iter/sec",
            "range": "stddev: 0.0005038366981747575",
            "extra": "mean: 84.32368208333212 msec\nrounds: 12"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[1000]",
            "value": 79.63317881482352,
            "unit": "iter/sec",
            "range": "stddev: 0.00007884812015808842",
            "extra": "mean: 12.557579828947533 msec\nrounds: 76"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[10000]",
            "value": 15.427744808241604,
            "unit": "iter/sec",
            "range": "stddev: 0.0009744020140965186",
            "extra": "mean: 64.81828759999928 msec\nrounds: 15"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[10]",
            "value": 992.1982366419348,
            "unit": "iter/sec",
            "range": "stddev: 0.0003401112326870059",
            "extra": "mean: 1.0078631094774668 msec\nrounds: 612"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[100]",
            "value": 516.7537994839516,
            "unit": "iter/sec",
            "range": "stddev: 0.0001528335068151666",
            "extra": "mean: 1.935157517948847 msec\nrounds: 390"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[500]",
            "value": 127.22960782055982,
            "unit": "iter/sec",
            "range": "stddev: 0.006639779821924205",
            "extra": "mean: 7.859805725490917 msec\nrounds: 153"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[10]",
            "value": 438.68950494764414,
            "unit": "iter/sec",
            "range": "stddev: 0.000050430952474312586",
            "extra": "mean: 2.2795165799996653 msec\nrounds: 100"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[100]",
            "value": 77.18159778279816,
            "unit": "iter/sec",
            "range": "stddev: 0.0007380228255135146",
            "extra": "mean: 12.95645631506834 msec\nrounds: 73"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[500]",
            "value": 16.304184805088386,
            "unit": "iter/sec",
            "range": "stddev: 0.0009834495947594877",
            "extra": "mean: 61.33394658823477 msec\nrounds: 17"
          }
        ]
      },
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
          "id": "fa0261805139838d2c1b20a4f5a2f243a56e7094",
          "message": "fix: __version__ bump and cleanup missed in #7 (#8)\n\n* chore: fix __version__ to 0.2.1 after squash merge dropped version bump\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>\n\n* chore: untrack .claude/settings.local.json\n\nAlready in .gitignore but was previously tracked. Removes it from the index\nwithout deleting the local file.\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>\n\n* chore: untrack .python-version and stray - file\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>\n\n---------\n\nCo-authored-by: Claude Sonnet 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-03-31T12:49:57-07:00",
          "tree_id": "b89fce2d4a1c99cef70b24458fc48c44fc20c585",
          "url": "https://github.com/ramannanda9/lakesense/commit/fa0261805139838d2c1b20a4f5a2f243a56e7094"
        },
        "date": 1774986698122,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[1000]",
            "value": 2053.4188944259718,
            "unit": "iter/sec",
            "range": "stddev: 0.000010733564776524375",
            "extra": "mean: 486.99269433748316 usec\nrounds: 1819"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[10000]",
            "value": 193.54340567168128,
            "unit": "iter/sec",
            "range": "stddev: 0.00004972034932777559",
            "extra": "mean: 5.166799646464613 msec\nrounds: 198"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[100000]",
            "value": 21.343060739829117,
            "unit": "iter/sec",
            "range": "stddev: 0.004525789568377162",
            "extra": "mean: 46.85363604545533 msec\nrounds: 22"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[1000]",
            "value": 6019.665963875911,
            "unit": "iter/sec",
            "range": "stddev: 0.000006399789827842147",
            "extra": "mean: 166.12217455270977 usec\nrounds: 5030"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[10000]",
            "value": 629.8736835841108,
            "unit": "iter/sec",
            "range": "stddev: 0.00006096733726628431",
            "extra": "mean: 1.5876199086613592 msec\nrounds: 635"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[100000]",
            "value": 67.4118041181671,
            "unit": "iter/sec",
            "range": "stddev: 0.00013132670812551552",
            "extra": "mean: 14.834197260869713 msec\nrounds: 69"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[1000]",
            "value": 4231.342221961598,
            "unit": "iter/sec",
            "range": "stddev: 0.00002687537371715996",
            "extra": "mean: 236.33162895919406 usec\nrounds: 1768"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[10000]",
            "value": 369.64866991533785,
            "unit": "iter/sec",
            "range": "stddev: 0.00032016457831730047",
            "extra": "mean: 2.705271468253989 msec\nrounds: 378"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[100000]",
            "value": 38.30488240975155,
            "unit": "iter/sec",
            "range": "stddev: 0.00017355380996237718",
            "extra": "mean: 26.10633258974378 msec\nrounds: 39"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[7]",
            "value": 3644.5047447987854,
            "unit": "iter/sec",
            "range": "stddev: 0.00000655456245290952",
            "extra": "mean: 274.385703963519 usec\nrounds: 2851"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[30]",
            "value": 1382.3850610815734,
            "unit": "iter/sec",
            "range": "stddev: 0.000008576902596924951",
            "extra": "mean: 723.3874469227868 usec\nrounds: 1300"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[90]",
            "value": 1027.7653768301777,
            "unit": "iter/sec",
            "range": "stddev: 0.000010815854698811336",
            "extra": "mean: 972.98471279913 usec\nrounds: 961"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[7]",
            "value": 17585.065624073468,
            "unit": "iter/sec",
            "range": "stddev: 0.000003468103782034876",
            "extra": "mean: 56.866435495755425 usec\nrounds: 10914"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[30]",
            "value": 5984.244236433478,
            "unit": "iter/sec",
            "range": "stddev: 0.000013280167091664301",
            "extra": "mean: 167.10547906981571 usec\nrounds: 4945"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[90]",
            "value": 2540.772886529257,
            "unit": "iter/sec",
            "range": "stddev: 0.000010482113954799597",
            "extra": "mean: 393.58102619160843 usec\nrounds: 2329"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[7]",
            "value": 3340.6258657167673,
            "unit": "iter/sec",
            "range": "stddev: 0.000008228532261525109",
            "extra": "mean: 299.3451048387423 usec\nrounds: 2480"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[30]",
            "value": 1379.3070020628722,
            "unit": "iter/sec",
            "range": "stddev: 0.000012172126044415266",
            "extra": "mean: 725.001757044961 usec\nrounds: 1313"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[90]",
            "value": 982.798025936934,
            "unit": "iter/sec",
            "range": "stddev: 0.000017409071583743143",
            "extra": "mean: 1.0175030612690403 msec\nrounds: 914"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[1000]",
            "value": 168.52916610753542,
            "unit": "iter/sec",
            "range": "stddev: 0.00016176677014056773",
            "extra": "mean: 5.933691022727295 msec\nrounds: 132"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[10000]",
            "value": 80.76143417050612,
            "unit": "iter/sec",
            "range": "stddev: 0.00014774324886080608",
            "extra": "mean: 12.382147621211976 msec\nrounds: 66"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[100000]",
            "value": 11.4758476045916,
            "unit": "iter/sec",
            "range": "stddev: 0.0006314638141001691",
            "extra": "mean: 87.13953290908901 msec\nrounds: 11"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[1000]",
            "value": 78.53352965234198,
            "unit": "iter/sec",
            "range": "stddev: 0.00012077039759327175",
            "extra": "mean: 12.733414688310505 msec\nrounds: 77"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[10000]",
            "value": 15.124646049755672,
            "unit": "iter/sec",
            "range": "stddev: 0.0005739114019415924",
            "extra": "mean: 66.11724973333537 msec\nrounds: 15"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[10]",
            "value": 989.9482116067554,
            "unit": "iter/sec",
            "range": "stddev: 0.00010308080543723658",
            "extra": "mean: 1.0101538527726919 msec\nrounds: 523"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[100]",
            "value": 502.5898067369515,
            "unit": "iter/sec",
            "range": "stddev: 0.00024259548927044444",
            "extra": "mean: 1.9896941533543397 msec\nrounds: 313"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[500]",
            "value": 124.93969835837436,
            "unit": "iter/sec",
            "range": "stddev: 0.006879631314248713",
            "extra": "mean: 8.003861167742068 msec\nrounds: 155"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[10]",
            "value": 431.7553086529181,
            "unit": "iter/sec",
            "range": "stddev: 0.00007856078869455294",
            "extra": "mean: 2.3161267040815607 msec\nrounds: 98"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[100]",
            "value": 75.83637353191737,
            "unit": "iter/sec",
            "range": "stddev: 0.0004433050316251553",
            "extra": "mean: 13.186284541666915 msec\nrounds: 72"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[500]",
            "value": 16.19827059773839,
            "unit": "iter/sec",
            "range": "stddev: 0.0007350704743639765",
            "extra": "mean: 61.734985470586004 msec\nrounds: 17"
          }
        ]
      },
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
          "id": "582a86cf11dd5db60df53fd8171e4410747dca33",
          "message": "refactor: DatasetDriftSummary with per-column signal attribution (#9)\n\nSeparates per-column signals (DriftSignals) from the dataset-level\naggregate (DatasetDriftSummary). Each metric now tracks which column\nproduced the worst value (jaccard_worst_column, null_rate_worst_column,\netc.), making alerts actionable without digging into raw data.\n\n- New DatasetDriftSummary type with per-metric column attribution\n- aggregate_signals takes dict[str, DriftSignals] keyed by column\n- compute_profile_signals returns dict[str, DriftSignals] (per-column)\n- Schema drift and row_count_delta computed inline in base_interpret\n- InterpretationResult.drift_signals renamed to dataset_drift_summary\n- to_dict/from_dict round-trip all DatasetDriftSummary fields\n- Version bump to 0.2.2",
          "timestamp": "2026-04-14T22:50:55-07:00",
          "tree_id": "7d426dd3709361b495b5a1a9e12bbf28f4f0589d",
          "url": "https://github.com/ramannanda9/lakesense/commit/582a86cf11dd5db60df53fd8171e4410747dca33"
        },
        "date": 1776232371979,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[1000]",
            "value": 1986.8543301758757,
            "unit": "iter/sec",
            "range": "stddev: 0.00001121951253074306",
            "extra": "mean: 503.30816145513813 usec\nrounds: 1759"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[10000]",
            "value": 199.28033105228982,
            "unit": "iter/sec",
            "range": "stddev: 0.00007233036937183717",
            "extra": "mean: 5.018056697916699 msec\nrounds: 192"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[100000]",
            "value": 21.711108794842946,
            "unit": "iter/sec",
            "range": "stddev: 0.00033799071036717197",
            "extra": "mean: 46.059370318181564 msec\nrounds: 22"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[1000]",
            "value": 6119.4121459725975,
            "unit": "iter/sec",
            "range": "stddev: 0.000006862088057193102",
            "extra": "mean: 163.4143895109493 usec\nrounds: 5358"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[10000]",
            "value": 629.9393157364474,
            "unit": "iter/sec",
            "range": "stddev: 0.000018719276173576678",
            "extra": "mean: 1.5874544976303366 msec\nrounds: 633"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[100000]",
            "value": 67.88105551744444,
            "unit": "iter/sec",
            "range": "stddev: 0.00011704304406846705",
            "extra": "mean: 14.73165071428529 msec\nrounds: 70"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[1000]",
            "value": 4099.106839212989,
            "unit": "iter/sec",
            "range": "stddev: 0.000012383730910663651",
            "extra": "mean: 243.95558330750794 usec\nrounds: 3235"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[10000]",
            "value": 372.30013380625945,
            "unit": "iter/sec",
            "range": "stddev: 0.00004075567664193814",
            "extra": "mean: 2.686004943850995 msec\nrounds: 374"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[100000]",
            "value": 37.739947501700335,
            "unit": "iter/sec",
            "range": "stddev: 0.0003128148752066497",
            "extra": "mean: 26.497122179487558 msec\nrounds: 39"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[7]",
            "value": 3618.1547219841023,
            "unit": "iter/sec",
            "range": "stddev: 0.000008033464777200002",
            "extra": "mean: 276.38397935940833 usec\nrounds: 2810"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[30]",
            "value": 1349.8201754552106,
            "unit": "iter/sec",
            "range": "stddev: 0.000007993627803114909",
            "extra": "mean: 740.8394230459342 usec\nrounds: 1241"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[90]",
            "value": 992.6391258774478,
            "unit": "iter/sec",
            "range": "stddev: 0.000010315114379729538",
            "extra": "mean: 1.0074154583782353 msec\nrounds: 925"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[7]",
            "value": 17712.85363100462,
            "unit": "iter/sec",
            "range": "stddev: 0.0000028767911740197076",
            "extra": "mean: 56.45617701314924 usec\nrounds: 10519"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[30]",
            "value": 6084.751843508413,
            "unit": "iter/sec",
            "range": "stddev: 0.000006776656008746547",
            "extra": "mean: 164.34523966114762 usec\nrounds: 4957"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[90]",
            "value": 2525.7999804038864,
            "unit": "iter/sec",
            "range": "stddev: 0.000012486305895715378",
            "extra": "mean: 395.9141688805048 usec\nrounds: 2108"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[7]",
            "value": 3447.0883116108835,
            "unit": "iter/sec",
            "range": "stddev: 0.000008216494923925038",
            "extra": "mean: 290.09990740059766 usec\nrounds: 2743"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[30]",
            "value": 1343.80012195073,
            "unit": "iter/sec",
            "range": "stddev: 0.000010327380339014528",
            "extra": "mean: 744.1582893654959 usec\nrounds: 1213"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[90]",
            "value": 990.3949825867248,
            "unit": "iter/sec",
            "range": "stddev: 0.000011929238679773203",
            "extra": "mean: 1.0096981684905033 msec\nrounds: 914"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[1000]",
            "value": 168.4061408272141,
            "unit": "iter/sec",
            "range": "stddev: 0.00017171896336599175",
            "extra": "mean: 5.938025745902028 msec\nrounds: 122"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[10000]",
            "value": 77.67028989856885,
            "unit": "iter/sec",
            "range": "stddev: 0.0003562811353125137",
            "extra": "mean: 12.874935851352165 msec\nrounds: 74"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[100000]",
            "value": 11.247534502093735,
            "unit": "iter/sec",
            "range": "stddev: 0.000560167467356919",
            "extra": "mean: 88.90837363635998 msec\nrounds: 11"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[1000]",
            "value": 75.40376989074987,
            "unit": "iter/sec",
            "range": "stddev: 0.0003083165292185197",
            "extra": "mean: 13.261936391892185 msec\nrounds: 74"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[10000]",
            "value": 14.738443215266042,
            "unit": "iter/sec",
            "range": "stddev: 0.0005427658754629938",
            "extra": "mean: 67.84977120000045 msec\nrounds: 15"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[10]",
            "value": 956.3587630347158,
            "unit": "iter/sec",
            "range": "stddev: 0.00033877175141033426",
            "extra": "mean: 1.0456327046419296 msec\nrounds: 474"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[100]",
            "value": 493.31846935262314,
            "unit": "iter/sec",
            "range": "stddev: 0.00006146711024905813",
            "extra": "mean: 2.0270881025644347 msec\nrounds: 351"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[500]",
            "value": 122.66240737430492,
            "unit": "iter/sec",
            "range": "stddev: 0.0061951542347916805",
            "extra": "mean: 8.152456986666627 msec\nrounds: 150"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[10]",
            "value": 424.5830611279908,
            "unit": "iter/sec",
            "range": "stddev: 0.0000666149753438733",
            "extra": "mean: 2.3552517553180237 msec\nrounds: 94"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[100]",
            "value": 73.51676514833024,
            "unit": "iter/sec",
            "range": "stddev: 0.00026349555060448186",
            "extra": "mean: 13.602339520548295 msec\nrounds: 73"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[500]",
            "value": 16.034412792639305,
            "unit": "iter/sec",
            "range": "stddev: 0.0004137974587094606",
            "extra": "mean: 62.36586352941194 msec\nrounds: 17"
          }
        ]
      },
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
          "id": "f95b666b7d8c59877e80c8b789845946a941ce90",
          "message": "feat: OpenLineage DataQualityAssertions facet for WAP gating (#10)\n\n* feat: OpenLineage DataQualityAssertions facet for WAP gating\n\nAdds lakesense.lineage module that converts InterpretationResult into\nOpenLineage DataQualityAssertionsDatasetFacet dicts. Callers attach\nthese to their own OL RunEvents to gate Iceberg WAP publish steps.\n\n- to_openlineage_facets / to_openlineage_assertions standalone functions\n- Dict-first output; typed OL object if openlineage-python is installed\n- Top-level lakesense_quality_check assertion (pass unless ALERT)\n- Per-signal assertions with column attribution and configurable thresholds\n- AssertionThresholds defaults match _heuristic_severity warn level\n- openlineage-python optional dependency\n\n* feat(openlineage): expected/actual diagnostic fields on all assertions; fix numeric_range success flag",
          "timestamp": "2026-05-05T14:10:37-07:00",
          "tree_id": "c32756922622905c870cb4cd8e0e89962d556b34",
          "url": "https://github.com/ramannanda9/lakesense/commit/f95b666b7d8c59877e80c8b789845946a941ce90"
        },
        "date": 1778015560389,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[1000]",
            "value": 1959.8711577956465,
            "unit": "iter/sec",
            "range": "stddev: 0.000013576198629615248",
            "extra": "mean: 510.2376225204233 usec\nrounds: 1714"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[10000]",
            "value": 187.24698326888935,
            "unit": "iter/sec",
            "range": "stddev: 0.00004526726232144433",
            "extra": "mean: 5.34053997849453 msec\nrounds: 186"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[100000]",
            "value": 20.557622877427438,
            "unit": "iter/sec",
            "range": "stddev: 0.0006302441662570405",
            "extra": "mean: 48.643756428571045 msec\nrounds: 21"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[1000]",
            "value": 6043.800882042566,
            "unit": "iter/sec",
            "range": "stddev: 0.0000060834571751693766",
            "extra": "mean: 165.4587931530331 usec\nrounds: 5550"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[10000]",
            "value": 628.8453597240909,
            "unit": "iter/sec",
            "range": "stddev: 0.000017297658520511437",
            "extra": "mean: 1.590216075441433 msec\nrounds: 623"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[100000]",
            "value": 67.70569895697554,
            "unit": "iter/sec",
            "range": "stddev: 0.00013659326043524348",
            "extra": "mean: 14.769805428572015 msec\nrounds: 70"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[1000]",
            "value": 4196.416203114173,
            "unit": "iter/sec",
            "range": "stddev: 0.000012348540416167594",
            "extra": "mean: 238.2985746880629 usec\nrounds: 3287"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[10000]",
            "value": 380.97887755365684,
            "unit": "iter/sec",
            "range": "stddev: 0.00003202478372953405",
            "extra": "mean: 2.6248174345549136 msec\nrounds: 382"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[100000]",
            "value": 38.43452725192423,
            "unit": "iter/sec",
            "range": "stddev: 0.00012623760403479754",
            "extra": "mean: 26.018272410256717 msec\nrounds: 39"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[7]",
            "value": 3639.00251008853,
            "unit": "iter/sec",
            "range": "stddev: 0.000009728446485212045",
            "extra": "mean: 274.80057989178795 usec\nrounds: 2954"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[30]",
            "value": 1427.64321161268,
            "unit": "iter/sec",
            "range": "stddev: 0.000010759526077694546",
            "extra": "mean: 700.4551220261749 usec\nrounds: 1303"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[90]",
            "value": 1025.6119693634002,
            "unit": "iter/sec",
            "range": "stddev: 0.000009252157560264734",
            "extra": "mean: 975.0276224064569 usec\nrounds: 964"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[7]",
            "value": 17627.438179006178,
            "unit": "iter/sec",
            "range": "stddev: 0.000004058078215624481",
            "extra": "mean: 56.72974086449919 usec\nrounds: 9578"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[30]",
            "value": 6036.719890029418,
            "unit": "iter/sec",
            "range": "stddev: 0.000005618499262481985",
            "extra": "mean: 165.6528741132507 usec\nrounds: 4933"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[90]",
            "value": 2546.9388059681833,
            "unit": "iter/sec",
            "range": "stddev: 0.000008862653998198215",
            "extra": "mean: 392.6282004328973 usec\nrounds: 2310"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[7]",
            "value": 3405.0281783303953,
            "unit": "iter/sec",
            "range": "stddev: 0.000019051200322931804",
            "extra": "mean: 293.6833258426469 usec\nrounds: 2136"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[30]",
            "value": 1372.894081909885,
            "unit": "iter/sec",
            "range": "stddev: 0.000034829794920549905",
            "extra": "mean: 728.3883099043314 usec\nrounds: 1252"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[90]",
            "value": 1015.9438531007529,
            "unit": "iter/sec",
            "range": "stddev: 0.000011695057730089163",
            "extra": "mean: 984.3063639274053 usec\nrounds: 937"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[1000]",
            "value": 162.74533945332823,
            "unit": "iter/sec",
            "range": "stddev: 0.0006434650758219038",
            "extra": "mean: 6.144569198473286 msec\nrounds: 131"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[10000]",
            "value": 80.29695758512665,
            "unit": "iter/sec",
            "range": "stddev: 0.0004559457891055743",
            "extra": "mean: 12.453771974359704 msec\nrounds: 78"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[100000]",
            "value": 11.156264148234621,
            "unit": "iter/sec",
            "range": "stddev: 0.003285605226323656",
            "extra": "mean: 89.63574066666762 msec\nrounds: 12"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[1000]",
            "value": 77.14419858766784,
            "unit": "iter/sec",
            "range": "stddev: 0.0001245727670786645",
            "extra": "mean: 12.962737552631191 msec\nrounds: 76"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[10000]",
            "value": 14.957818294265689,
            "unit": "iter/sec",
            "range": "stddev: 0.0004816673002622627",
            "extra": "mean: 66.85466960000213 msec\nrounds: 15"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[10]",
            "value": 889.7934499306606,
            "unit": "iter/sec",
            "range": "stddev: 0.00044425858180557795",
            "extra": "mean: 1.123856328767005 msec\nrounds: 511"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[100]",
            "value": 479.8478614874557,
            "unit": "iter/sec",
            "range": "stddev: 0.00028924217246878745",
            "extra": "mean: 2.083993866097791 msec\nrounds: 351"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[500]",
            "value": 126.88853920726956,
            "unit": "iter/sec",
            "range": "stddev: 0.005031273851511675",
            "extra": "mean: 7.880932401361502 msec\nrounds: 147"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[10]",
            "value": 402.73525660936997,
            "unit": "iter/sec",
            "range": "stddev: 0.00006547931482153143",
            "extra": "mean: 2.4830207526875214 msec\nrounds: 93"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[100]",
            "value": 72.69542971883307,
            "unit": "iter/sec",
            "range": "stddev: 0.0002601280644064883",
            "extra": "mean: 13.756022955882356 msec\nrounds: 68"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[500]",
            "value": 15.789974751010567,
            "unit": "iter/sec",
            "range": "stddev: 0.0009289058663300977",
            "extra": "mean: 63.33132356250282 msec\nrounds: 16"
          }
        ]
      },
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
          "id": "fcf41f63913a0c97452699e20ef84bec151685b3",
          "message": "fix: remove unused openlineage-python dependency and dead defensive code (#12)\n\n* docs: remove stale typed-facet references from openlineage docstrings\n\n* fix: remove unused openlineage-python dependency and dead defensive code\n\n- Drop `openlineage-python` optional extra — we emit plain dicts matching\n  the OL JSON schema directly; no runtime dependency needed\n- Remove `or DatasetDriftSummary()` fallback in _build_assertions —\n  dataset_drift_summary has a default_factory and is never None\n- Remove test_none_drift_summary_does_not_raise which tested an impossible\n  None state\n- Update README and CHANGELOG to reflect dict-only output (no typed upgrade)\n\n* fix: remove stale pip install lakesense[openlineage] from lineage __init__ docstring",
          "timestamp": "2026-05-05T18:11:57-07:00",
          "tree_id": "699cbaf23c6d6426f60d6c76b3c228e5eef4f3c8",
          "url": "https://github.com/ramannanda9/lakesense/commit/fcf41f63913a0c97452699e20ef84bec151685b3"
        },
        "date": 1778030031392,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[1000]",
            "value": 2005.5570564174927,
            "unit": "iter/sec",
            "range": "stddev: 0.00003635143840309058",
            "extra": "mean: 498.61458530942537 usec\nrounds: 1729"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[10000]",
            "value": 194.80243092140384,
            "unit": "iter/sec",
            "range": "stddev: 0.000055232809463410874",
            "extra": "mean: 5.13340616577555 msec\nrounds: 187"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[100000]",
            "value": 21.05420913548378,
            "unit": "iter/sec",
            "range": "stddev: 0.0001232219023495932",
            "extra": "mean: 47.496440904761734 msec\nrounds: 21"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[1000]",
            "value": 5987.319840372559,
            "unit": "iter/sec",
            "range": "stddev: 0.000006783384090781059",
            "extra": "mean: 167.01963928116714 usec\nrounds: 5453"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[10000]",
            "value": 628.5776820443353,
            "unit": "iter/sec",
            "range": "stddev: 0.000021131024125579665",
            "extra": "mean: 1.5908932635783068 msec\nrounds: 626"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[100000]",
            "value": 67.93191798353442,
            "unit": "iter/sec",
            "range": "stddev: 0.00018269173847142392",
            "extra": "mean: 14.720620728570971 msec\nrounds: 70"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[1000]",
            "value": 4147.74401697733,
            "unit": "iter/sec",
            "range": "stddev: 0.000014010890113539875",
            "extra": "mean: 241.09491711804105 usec\nrounds: 3137"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[10000]",
            "value": 364.77194854447964,
            "unit": "iter/sec",
            "range": "stddev: 0.000029925551623073963",
            "extra": "mean: 2.74143887431646 msec\nrounds: 366"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[100000]",
            "value": 37.59464532343563,
            "unit": "iter/sec",
            "range": "stddev: 0.00047906135124465475",
            "extra": "mean: 26.599532763157185 msec\nrounds: 38"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[7]",
            "value": 3638.7241058212826,
            "unit": "iter/sec",
            "range": "stddev: 0.00000685729835271482",
            "extra": "mean: 274.8216052984577 usec\nrounds: 2982"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[30]",
            "value": 1389.7679300128216,
            "unit": "iter/sec",
            "range": "stddev: 0.000011501668560210211",
            "extra": "mean: 719.5445933126218 usec\nrounds: 1286"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[90]",
            "value": 1004.8928512661591,
            "unit": "iter/sec",
            "range": "stddev: 0.00001530717036083104",
            "extra": "mean: 995.1309721628588 usec\nrounds: 934"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[7]",
            "value": 17836.46032044068,
            "unit": "iter/sec",
            "range": "stddev: 0.0000034040210098150293",
            "extra": "mean: 56.06493564499423 usec\nrounds: 10411"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[30]",
            "value": 6003.824443237084,
            "unit": "iter/sec",
            "range": "stddev: 0.000014455305450207421",
            "extra": "mean: 166.56049980382667 usec\nrounds: 5098"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[90]",
            "value": 2526.7601237074555,
            "unit": "iter/sec",
            "range": "stddev: 0.000010166894392467962",
            "extra": "mean: 395.7637254986926 usec\nrounds: 2306"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[7]",
            "value": 3389.459858937827,
            "unit": "iter/sec",
            "range": "stddev: 0.000009750988877877604",
            "extra": "mean: 295.03225930321986 usec\nrounds: 2526"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[30]",
            "value": 1399.3387264330636,
            "unit": "iter/sec",
            "range": "stddev: 0.00001160035606807993",
            "extra": "mean: 714.6232581935438 usec\nrounds: 1251"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[90]",
            "value": 1026.6337720977692,
            "unit": "iter/sec",
            "range": "stddev: 0.00000973748308196776",
            "extra": "mean: 974.0571829783592 usec\nrounds: 940"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[1000]",
            "value": 172.1009449039174,
            "unit": "iter/sec",
            "range": "stddev: 0.0001700786579046003",
            "extra": "mean: 5.810543344537081 msec\nrounds: 119"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[10000]",
            "value": 82.34743320621622,
            "unit": "iter/sec",
            "range": "stddev: 0.00011722390821005711",
            "extra": "mean: 12.14366934177266 msec\nrounds: 79"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[100000]",
            "value": 11.4923274904056,
            "unit": "iter/sec",
            "range": "stddev: 0.0026243402482238475",
            "extra": "mean: 87.01457566666566 msec\nrounds: 12"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[1000]",
            "value": 78.81501092341438,
            "unit": "iter/sec",
            "range": "stddev: 0.00012727570145415105",
            "extra": "mean: 12.68793835442989 msec\nrounds: 79"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[10000]",
            "value": 15.049085078785849,
            "unit": "iter/sec",
            "range": "stddev: 0.005820267433514902",
            "extra": "mean: 66.44922231250217 msec\nrounds: 16"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[10]",
            "value": 1005.0593466573541,
            "unit": "iter/sec",
            "range": "stddev: 0.0000906478275751238",
            "extra": "mean: 994.966121479114 usec\nrounds: 568"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[100]",
            "value": 503.25807914118946,
            "unit": "iter/sec",
            "range": "stddev: 0.00024116123658723132",
            "extra": "mean: 1.9870520542988628 msec\nrounds: 442"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[500]",
            "value": 124.91057637067081,
            "unit": "iter/sec",
            "range": "stddev: 0.00572083526953766",
            "extra": "mean: 8.005727209459915 msec\nrounds: 148"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[10]",
            "value": 372.95098240409084,
            "unit": "iter/sec",
            "range": "stddev: 0.0004207357054334231",
            "extra": "mean: 2.6813175113626704 msec\nrounds: 88"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[100]",
            "value": 71.24832602800119,
            "unit": "iter/sec",
            "range": "stddev: 0.00022340446353279252",
            "extra": "mean: 14.035417472222319 msec\nrounds: 72"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[500]",
            "value": 15.832021946341385,
            "unit": "iter/sec",
            "range": "stddev: 0.0007566735140500579",
            "extra": "mean: 63.16312618749809 msec\nrounds: 16"
          }
        ]
      },
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
          "id": "4cc86c0b7e868750a90bcabf7fbdfea7b02f2d67",
          "message": "perf(sketches): vectorize KLL ingest and dedup Theta tokenization (#13)\n\n* perf(sketches): vectorize KLL ingest and dedup Theta tokenization\n\ncompute_kll looped in Python and hand-rolled Welford for mean/std, ignoring\nthe native ndarray overload on kll_doubles_sketch.update. Feeding the whole\ncolumn to the C path and letting numpy do mean/std is ~10x faster on 1M rows\n(0.238s -> 0.023s).\n\ncompute_minhash tokenized every row even when values repeat. Theta is a set\nsketch, so updating with an already-seen value is a no-op — tokenizing each\ndistinct value once produces the same sketch for a fraction of the work.\nMeasured at Spark's default 10k-row Arrow batch size: 1.45x when all values\nare distinct, 4x at 2k distinct/batch, 20x at 100 distinct/batch.\n\nDedup uses dict.fromkeys rather than a set: Theta's retained sample is\ninsertion-order dependent, and set iteration order for strings varies with\nPYTHONHASHSEED, which would make blobs irreproducible across Spark workers.\ndict.fromkeys preserves first-occurrence order, verified to give byte-identical\nblobs across PYTHONHASHSEED=1,2,3. It also benchmarks faster than pd.unique and\nkeeps compute.py free of pandas, which is an optional extra.\n\ncompute_hll is deliberately left alone. The same dedup trick regresses 21% on\nhigh-cardinality columns at 10k-row batches — exactly the id_columns case it\nwould target — and a sampling heuristic to gate it proved unreliable.\n\nPandasProvider now passes numpy arrays instead of materializing Python lists.\n\n* fix(sketches): keep vectorized paths O(1) memory for streaming inputs\n\nThe previous commit materialized inputs into an array (compute_kll) and an\nunbounded dedup dict (compute_minhash). Both are fine for PandasProvider and\nSparkProvider, where the batch is already resident, but they broke\nStreamingProvider's documented \"strictly O(1) memory\" contract over\nfile-backed generators. Measured over a 2M-row generator:\n\n    compute_kll               0.0 MB -> 34.0 MB\n    compute_minhash (distinct) 0.1 MB -> 210.5 MB\n\nand both grew with row count. StreamingProvider has no tests, so nothing caught it.\n\ncompute_kll now consumes one-shot iterables in 64Ki-element blocks, feeding each\nblock to the same native ndarray overload and combining per-block moments with\nChan et al.'s parallel variance. Already-materialized inputs (ndarray, Series,\nlist) still take a single-block fast path, so nothing is copied that wasn't\nalready in memory and the ~10x stands.\n\ncompute_minhash caps its dedup memo and clears it on overflow. Theta is\nidempotent, so re-tokenizing an evicted value cannot change the result — it only\ncosts redundant work on columns where dedup was never going to pay.\n\nPeak memory is now flat at 1.76 MB (kll) and ~6.8 MB (minhash) across 1M/4M/16M\nrows. Theta output stays bit-identical to main across all 9 tokenizer/data\ncombinations; KLL mean/std match to 1e-9. End-to-end provider throughput is\nunchanged at 2.1x.\n\nAdds the first tests for StreamingProvider and for generator inputs, including a\npeak-memory regression guard verified to fail against the previous commit.",
          "timestamp": "2026-07-24T14:27:37-07:00",
          "tree_id": "cdd7229fba3755ceaebae6496273179860bd3db1",
          "url": "https://github.com/ramannanda9/lakesense/commit/4cc86c0b7e868750a90bcabf7fbdfea7b02f2d67"
        },
        "date": 1784928595788,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[1000]",
            "value": 2418.7580563259808,
            "unit": "iter/sec",
            "range": "stddev: 0.00000924644814212265",
            "extra": "mean: 413.4353154440628 usec\nrounds: 1826"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[10000]",
            "value": 223.56175117854718,
            "unit": "iter/sec",
            "range": "stddev: 0.00004843083081623526",
            "extra": "mean: 4.4730370679613785 msec\nrounds: 206"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[100000]",
            "value": 22.647497129200083,
            "unit": "iter/sec",
            "range": "stddev: 0.00028034192387916694",
            "extra": "mean: 44.15498959091027 msec\nrounds: 22"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[1000]",
            "value": 11112.822353358682,
            "unit": "iter/sec",
            "range": "stddev: 0.000002922474657824682",
            "extra": "mean: 89.98614107223312 usec\nrounds: 9123"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[10000]",
            "value": 1174.8269393292128,
            "unit": "iter/sec",
            "range": "stddev: 0.000020997739470617117",
            "extra": "mean: 851.189197764708 usec\nrounds: 1163"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[100000]",
            "value": 129.83730930707458,
            "unit": "iter/sec",
            "range": "stddev: 0.00004577131445556927",
            "extra": "mean: 7.701946423080349 msec\nrounds: 130"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[1000]",
            "value": 31394.922634837894,
            "unit": "iter/sec",
            "range": "stddev: 0.0000022578663463769717",
            "extra": "mean: 31.852284257274565 usec\nrounds: 4904"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[10000]",
            "value": 2353.4816185226623,
            "unit": "iter/sec",
            "range": "stddev: 0.000009974675645947174",
            "extra": "mean: 424.9024050707158 usec\nrounds: 1854"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[100000]",
            "value": 219.0348743799554,
            "unit": "iter/sec",
            "range": "stddev: 0.00004844490108774311",
            "extra": "mean: 4.565483021052255 msec\nrounds: 190"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[7]",
            "value": 5578.421265715326,
            "unit": "iter/sec",
            "range": "stddev: 0.000008396896599556839",
            "extra": "mean: 179.2621876992234 usec\nrounds: 3756"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[30]",
            "value": 1982.025795899318,
            "unit": "iter/sec",
            "range": "stddev: 0.000010446044428233046",
            "extra": "mean: 504.5343012532606 usec\nrounds: 1756"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[90]",
            "value": 1478.674526264842,
            "unit": "iter/sec",
            "range": "stddev: 0.00002957704328776821",
            "extra": "mean: 676.2813467315337 usec\nrounds: 1269"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[7]",
            "value": 36821.80149682035,
            "unit": "iter/sec",
            "range": "stddev: 0.0000013171017138827371",
            "extra": "mean: 27.157823880136675 usec\nrounds: 14104"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[30]",
            "value": 13998.783049875541,
            "unit": "iter/sec",
            "range": "stddev: 0.000002287784129141292",
            "extra": "mean: 71.43478089753599 usec\nrounds: 7193"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[90]",
            "value": 5342.5753723715015,
            "unit": "iter/sec",
            "range": "stddev: 0.0000072710506643766615",
            "extra": "mean: 187.17564663128238 usec\nrounds: 3577"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[7]",
            "value": 5696.962868580737,
            "unit": "iter/sec",
            "range": "stddev: 0.00000832582489159013",
            "extra": "mean: 175.53212528645568 usec\nrounds: 3488"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[30]",
            "value": 1925.3933842185156,
            "unit": "iter/sec",
            "range": "stddev: 0.000011618882575979081",
            "extra": "mean: 519.3743825009989 usec\nrounds: 1600"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[90]",
            "value": 1455.6189859900305,
            "unit": "iter/sec",
            "range": "stddev: 0.000010333157625241954",
            "extra": "mean: 686.9929628733553 usec\nrounds: 1239"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[1000]",
            "value": 388.6801692990137,
            "unit": "iter/sec",
            "range": "stddev: 0.00017157478720533038",
            "extra": "mean: 2.5728094175823384 msec\nrounds: 182"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[10000]",
            "value": 170.1275256793189,
            "unit": "iter/sec",
            "range": "stddev: 0.00009070006854832005",
            "extra": "mean: 5.877943595588085 msec\nrounds: 136"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[100000]",
            "value": 20.635029283260497,
            "unit": "iter/sec",
            "range": "stddev: 0.0009381549580360989",
            "extra": "mean: 48.461283300005675 msec\nrounds: 20"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[1000]",
            "value": 150.85352284575464,
            "unit": "iter/sec",
            "range": "stddev: 0.0026165879920803953",
            "extra": "mean: 6.628946948905424 msec\nrounds: 137"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[10000]",
            "value": 28.354388372180427,
            "unit": "iter/sec",
            "range": "stddev: 0.001147637829279291",
            "extra": "mean: 35.26790939285921 msec\nrounds: 28"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[10]",
            "value": 1012.6310448603006,
            "unit": "iter/sec",
            "range": "stddev: 0.0026934513545033385",
            "extra": "mean: 987.5265083720171 usec\nrounds: 657"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[100]",
            "value": 218.5380233535978,
            "unit": "iter/sec",
            "range": "stddev: 0.0038875317004062986",
            "extra": "mean: 4.575862747609761 msec\nrounds: 523"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[500]",
            "value": 32.74542837427303,
            "unit": "iter/sec",
            "range": "stddev: 0.03345987974409558",
            "extra": "mean: 30.53861407981048 msec\nrounds: 213"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[10]",
            "value": 1270.339877064971,
            "unit": "iter/sec",
            "range": "stddev: 0.00004278261944110255",
            "extra": "mean: 787.1909069802863 usec\nrounds: 129"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[100]",
            "value": 645.4268608125534,
            "unit": "iter/sec",
            "range": "stddev: 0.00005722679439861151",
            "extra": "mean: 1.5493622294260583 msec\nrounds: 401"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[500]",
            "value": 138.7579857081359,
            "unit": "iter/sec",
            "range": "stddev: 0.0003772535768440952",
            "extra": "mean: 7.206792422768403 msec\nrounds: 123"
          }
        ]
      },
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
          "id": "53e5971d167edc70f8cc19aa124bd84a5c3926c6",
          "message": "ci: make the release gate real and stop benchmark alerts failing the build (#14)\n\nThree gaps surfaced while shipping #13.\n\npublish.yml ran only tests/unit/, so tests/providers/ never gated a PyPI\npublish. #13 was entirely a change to sketch computation, and the test that\nverifies SparkProvider's map-reduce agrees with PandasProvider would not have\nblocked its release. Mirrors ci.yml's test-providers matrix into publish.yml and\nmakes publish depend on it.\n\nThe Spark fixture turned any JVM failure into pytest.skip, including in CI. That\nis how the equivalence test came to be silently skipping — a green run meant\nnothing. It now raises when CI is set, and still skips locally where a missing\nJVM is a normal state.\n\nThe benchmark job declared only contents: write, but comment-on-alert posts via\nthe issues API. Every fired alert failed the job with \"Resource not accessible\nby integration\" despite fail-on-alert: false. Adds pull-requests: write and\nissues: write.\n\nThe alert that triggered this was noise — test_write_sketches times only the\nwrite, while sketch computation happens in setup, and local medians were faster\non all three sizes. Leaving alert-threshold alone: with permissions fixed an\nalert now comments instead of failing, which is the intended behavior.",
          "timestamp": "2026-07-24T14:36:50-07:00",
          "tree_id": "badb6b646447ea8cff0f4d2df369933c1ca0dadb",
          "url": "https://github.com/ramannanda9/lakesense/commit/53e5971d167edc70f8cc19aa124bd84a5c3926c6"
        },
        "date": 1784929145283,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[1000]",
            "value": 1282.5244467323278,
            "unit": "iter/sec",
            "range": "stddev: 0.000016779567551308765",
            "extra": "mean: 779.7122328138415 usec\nrounds: 1091"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[10000]",
            "value": 121.63644217494719,
            "unit": "iter/sec",
            "range": "stddev: 0.0000582721633462425",
            "extra": "mean: 8.221220401709223 msec\nrounds: 117"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_minhash[100000]",
            "value": 12.333755405063675,
            "unit": "iter/sec",
            "range": "stddev: 0.00048558938090300643",
            "extra": "mean: 81.07830641666898 msec\nrounds: 12"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[1000]",
            "value": 5773.97142171137,
            "unit": "iter/sec",
            "range": "stddev: 0.000005141811763016214",
            "extra": "mean: 173.1910200039761 usec\nrounds: 5249"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[10000]",
            "value": 600.2350585970828,
            "unit": "iter/sec",
            "range": "stddev: 0.000026635238929707496",
            "extra": "mean: 1.666013981817856 msec\nrounds: 605"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_hll[100000]",
            "value": 64.82839602332959,
            "unit": "iter/sec",
            "range": "stddev: 0.00011717814568214062",
            "extra": "mean: 15.425339223881663 msec\nrounds: 67"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[1000]",
            "value": 15626.900534609851,
            "unit": "iter/sec",
            "range": "stddev: 0.000005967493266303658",
            "extra": "mean: 63.99221635699536 usec\nrounds: 4414"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[10000]",
            "value": 1358.7628733558488,
            "unit": "iter/sec",
            "range": "stddev: 0.00002484246076076918",
            "extra": "mean: 735.9635883560885 usec\nrounds: 979"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchCompute::test_compute_kll[100000]",
            "value": 143.81681994108806,
            "unit": "iter/sec",
            "range": "stddev: 0.000024960151016251406",
            "extra": "mean: 6.953289611115249 msec\nrounds: 18"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[7]",
            "value": 3516.7248108651693,
            "unit": "iter/sec",
            "range": "stddev: 0.000006620764977024361",
            "extra": "mean: 284.3554880695895 usec\nrounds: 2766"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[30]",
            "value": 1402.4765726531086,
            "unit": "iter/sec",
            "range": "stddev: 0.000007856621137263237",
            "extra": "mean: 713.0243880710741 usec\nrounds: 1291"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_minhash[90]",
            "value": 1013.1983069746539,
            "unit": "iter/sec",
            "range": "stddev: 0.000007987519132475283",
            "extra": "mean: 986.9736191979405 usec\nrounds: 948"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[7]",
            "value": 17447.01160451232,
            "unit": "iter/sec",
            "range": "stddev: 0.0000029842891833581016",
            "extra": "mean: 57.31640596498314 usec\nrounds: 8885"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[30]",
            "value": 6038.791503439448,
            "unit": "iter/sec",
            "range": "stddev: 0.00001271401020547597",
            "extra": "mean: 165.59604673061506 usec\nrounds: 5307"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_merge_hll[90]",
            "value": 2529.9468598299927,
            "unit": "iter/sec",
            "range": "stddev: 0.00000935571910265282",
            "extra": "mean: 395.26521915452327 usec\nrounds: 2318"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[7]",
            "value": 3654.93235954981,
            "unit": "iter/sec",
            "range": "stddev: 0.000007743632766063983",
            "extra": "mean: 273.6028745886759 usec\nrounds: 2735"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[30]",
            "value": 1379.4478475666951,
            "unit": "iter/sec",
            "range": "stddev: 0.000010626729915142255",
            "extra": "mean: 724.9277323270829 usec\nrounds: 1259"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestSketchMerge::test_build_baseline_rolling[90]",
            "value": 1003.7622915313957,
            "unit": "iter/sec",
            "range": "stddev: 0.000008893372913735234",
            "extra": "mean: 996.2518102511544 usec\nrounds: 917"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[1000]",
            "value": 166.6229031611962,
            "unit": "iter/sec",
            "range": "stddev: 0.0003460154076256655",
            "extra": "mean: 6.001575899998386 msec\nrounds: 100"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[10000]",
            "value": 82.54824528226914,
            "unit": "iter/sec",
            "range": "stddev: 0.00013513785840958695",
            "extra": "mean: 12.1141278846153 msec\nrounds: 78"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProfiling::test_profile_dataframe[100000]",
            "value": 11.93304976747581,
            "unit": "iter/sec",
            "range": "stddev: 0.00048628990644800497",
            "extra": "mean: 83.80087400000254 msec\nrounds: 12"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[1000]",
            "value": 77.64583325977654,
            "unit": "iter/sec",
            "range": "stddev: 0.00014735269009155146",
            "extra": "mean: 12.878991157894335 msec\nrounds: 76"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestProviderE2E::test_pandas_provider_sketch[10000]",
            "value": 15.237876872062465,
            "unit": "iter/sec",
            "range": "stddev: 0.0005048299230682867",
            "extra": "mean: 65.62594043750458 msec\nrounds: 16"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[10]",
            "value": 920.3783289815101,
            "unit": "iter/sec",
            "range": "stddev: 0.000253542654587946",
            "extra": "mean: 1.0865097194396125 msec\nrounds: 499"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[100]",
            "value": 468.195294791737,
            "unit": "iter/sec",
            "range": "stddev: 0.00024055612226489486",
            "extra": "mean: 2.135860849359498 msec\nrounds: 312"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_write_sketches[500]",
            "value": 131.79545554949325,
            "unit": "iter/sec",
            "range": "stddev: 0.005573098567400239",
            "extra": "mean: 7.58751503100552 msec\nrounds: 129"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[10]",
            "value": 764.7471308085858,
            "unit": "iter/sec",
            "range": "stddev: 0.00007075759591594432",
            "extra": "mean: 1.3076217741970155 msec\nrounds: 31"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[100]",
            "value": 356.782497831759,
            "unit": "iter/sec",
            "range": "stddev: 0.00015040434213942015",
            "extra": "mean: 2.8028280705393533 msec\nrounds: 241"
          },
          {
            "name": "benchmarks/test_bench_core.py::TestStorageIO::test_read_sketches[500]",
            "value": 82.72905255135316,
            "unit": "iter/sec",
            "range": "stddev: 0.0014268415006309767",
            "extra": "mean: 12.08765202985083 msec\nrounds: 67"
          }
        ]
      }
    ]
  }
}