window.BENCHMARK_DATA = {
  "lastUpdate": 1776232372796,
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
      }
    ]
  }
}