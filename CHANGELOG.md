# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-02-09

### Features
- Rewrite `BadTaxiApp` to simulate ultra high IO and shuffle stress (10x data inflation, 100 wide columns, heavy self-joins).
- Increase Spark executor memory to 1024m in `start-spark.sh` to handle the increased load.
- Add multi-stage job grouping and persistence to improve Spark UI visibility for performance analysis.
