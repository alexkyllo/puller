% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/puller.R
\name{kusto_export_cmd}
\alias{kusto_export_cmd}
\title{Run a Kusto query and export results to Azure Storage in Parquet or CSV
format.}
\usage{
kusto_export_cmd(query, name_prefix, storage_uri, key = "impersonate")
}
\arguments{
\item{query}{The text of the Kusto query to run}

\item{name_prefix}{The filename prefix for each exported file}

\item{storage_uri}{The URI of the blob storage container to export to}

\item{key}{The account key for the storage container. Default "impersonate"
uses the identity that is signed into Kusto to authenticate to Azure Storage.}
}
\description{
Run a Kusto query and export results to Azure Storage in Parquet or CSV
format.
}
