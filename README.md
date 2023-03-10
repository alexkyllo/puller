
<!-- README.md is generated from README.Rmd. Please edit that file -->

# puller

<!-- badges: start -->

[![Lifecycle
Status](https://img.shields.io/badge/lifecycle-experimental-yellow.svg)](https://lifecycle.r-lib.org/articles/stages.html)
<!-- badges: end -->

(WIP) puller is a package to provide a high-level interface for data
scientists to get data from Azure for reproducible data analysis work.
It implements an opinionated workflow to minimize the amount of time you
need to spend configuring cloud resources, so that you can focus on your
analysis.

The basic workflow is: write a query on a database, run it and export
the results to Blob Storage, download a copy to your machine and run the
rest of your R script against it, while using Azure Key Vault to
authenticate to Azure without having to store secret keys.

With this workflow you can run your script on multiple machines without
having to rerun the query on the source database because the results you
want are cached in Blob storage. If the source data upstream changes or
gets deleted, you can still reproduce your analysis because you have the
dataset.

## Features

  - [x] Log into Azure via the CLI
  - [x] Export data to Azure Blob Storage from a Kusto query
  - [x] Collect export function to pipe an AzureKusto dbplyr query
  - [ ] Export data to Azure Blob Storage from a SQL query via Kusto
    using `sql_request()`
  - [ ] Export data to Azure Blob Storage from an Azure SQL database
    (will require C/C++ code to put access token into connection
    arguments because rodbc/nanodbc doesn’t support this)
  - [ ] Helper to download data from blob to local
  - [ ] {targets} integration

## Installation

You can install the development version of puller from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("alexkyllo/puller")
```
