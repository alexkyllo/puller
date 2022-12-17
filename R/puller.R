#' Log into the Azure CLI by running the `az login` command.
#' @export
az_login <- function() {
    message("az login\n")
    system2("az", "login", stdout = TRUE)
}

#' Get an access token using the Azure CLI
#'
#' @param resource The URI of the Azure resource to which to authenticate.
#'
#' @details Runs the `az account get-access-token` command to retrieve an access
#' token from Azure via the Azure CLI. Presumes that you are signed in. If not,
#' call `az_login()` first.
#'
#' @export
get_cli_token <- function(resource) {
    resource_name <- jsonlite::read_json(
        path = paste0(resource, "/v1/rest/auth/metadata")
    )
    scope_command <- paste0(
        "az account get-access-token --scope ",
        resource_name$AzureAD$KustoServiceResourceId,
        "/.default"
    )
    message(scope_command)
    access_token <- jsonlite::parse_json(system(scope_command, intern = TRUE))
    return(access_token)
}

#' Run a Kusto query and export results to Azure Storage in Parquet or CSV
#' format.
#' 
#' @param query The text of the Kusto query to run
#' @param name_prefix The filename prefix for each exported file
#' @param storage_uri The URI of the blob storage container to export to
#' @param key The account key for the storage container
#' @export
export_kusto_query <- function(query, name_prefix, storage_uri, key) {
    template <- ".export
compressed
to parquet (h@'{{ storage_uri }}/{{ name_prefix }};{{ key }}')
with (
sizeLimit=1073741824,
namePrefix='{{ name_prefix }}',
fileExtension=parquet,
compressionType=snappy,
includeHeaders=firstFile,
encoding=UTF8NoBOM,
distributed=false
)
<|
{{ query }}
"
    args <- list(query = query, name_prefix = name_prefix,
        storage_uri = storage_uri, key = key)
    whisker::whisker.render(template, args)
}