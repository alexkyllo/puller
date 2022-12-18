#' Log into the Azure CLI by running the `az login` command.
#' @export
az_login <- function(tenant, use_device_code = FALSE) {
    args <- c("login")
    if (!missing(tenant)) args <- c(args, paste("--tenant", tenant))
    if (use_device_code) args <- c(args, "--use-device-code")
    execute_cmd(list(command = "az", args = args))
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
# TODO: fix
# ERROR: User '' does not exist in MSAL token cache. Run `az login`.
# Error: lexical error: invalid char in json text.
#                                        NA
#                      (right here) ------^

#' Build the Azure CLI command to get an access token
#' @param command default "az"
#' @param resource The URI of the Azure resource to which to authenticate.
#' @param tenant The Azure tenant ID to authenticate to.
build_az_token_cmd <- function(command = "az", resource, tenant) {
    args <- c("account", "get-access-token", "--output json")
    if (!missing(resource)) args <- c(args, paste("--resource", resource))
    if (!missing(tenant)) args <- c(args, paste("--tenant", tenant))
    list(command = command, args = args)
}

#' Handling for error messages returned by the Azure CLI
#' @param cond the error message string
handle_az_cmd_errors <- function(cond) {
    not_loggedin <- grepl("az login", cond, fixed = TRUE) |
        grepl("az account set", cond, fixed = TRUE)
    not_found <- grepl("not found", cond, fixed = TRUE)
    error_in <- grepl("error in running", cond, fixed = TRUE)

    if (not_found | error_in) {
        msg <- paste(
            "az is not installed or not in PATH.\n",
            "Please see: ",
            "https://learn.microsoft.com/en-us/cli/azure/install-azure-cli\n",
            "for installation instructions."
        )
        stop(msg)
    } else if (not_loggedin) {
        stop("You are not logged into the Azure CLI.
        Please call AzureAuth::az_login()
        or run 'az login' from your shell and try again.")
    } else {
        # Other misc errors, pass through the CLI error message
        message("Failed to invoke the Azure CLI.")
        stop(cond)
    }
}

#' Execute a command to the Azure CLI with arguments and handle error cases.
#' @param cmd A list with the command in $command and a vector of arguments in
#' $args.
execute_cmd <- function(cmd) {
    tryCatch(
        {
            message(cmd$command, " ", paste(cmd$args, collapse = " "), "\n")
            result <- do.call(system2, append(cmd, list(stdout = TRUE)))
            # result is a multi-line JSON string, concatenate together
            paste0(result)
        },
        warning = function() {
            # if an error case, catch it, pass the error string and handle it
            handle_az_cmd_errors(result)
        },
        error = function(cond) {
            handle_az_cmd_errors(cond$message)
        }
    )
}

#' Run a Kusto query and export results to Azure Storage in Parquet or CSV
#' format.
#'
#' @param query The text of the Kusto query to run
#' @param name_prefix The filename prefix for each exported file
#' @param storage_uri The URI of the blob storage container to export to
#' @param key The account key for the storage container. Default "impersonate"
#' uses the identity that is signed into Kusto to authenticate to Azure Storage.
#' @export
kusto_export_cmd <- function(query, name_prefix, storage_uri,
                             key = "impersonate") {
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
    args <- list(
        query = query, name_prefix = name_prefix,
        storage_uri = storage_uri, key = key
    )
    whisker::whisker.render(template, args)
}

#' Wrap a SQL query in a Kusto request
wrap_sql_request <- function(query, sql_server, sql_database) {
    conn_str <- glue::glue('Server={sql_server},1433;Authentication="Active Directory Integrated";Initial Catalog={sql_database};')
    query <- gsub("[\r\n]", " ", query)
    glue::glue("set notruncation; evaluate sql_request('{conn_str}', \"{query}\")")
}

query_kusto <- function(server_url, database, query) {
    token <- get_cli_token(server_url)
    endpoint <- AzureKusto::kusto_database_endpoint(
        server = server_url,
        database = database,
        .query_token = token
    )
    AzureKusto::run_query(endpoint, query)
}

query_kusto_sql <- function(sql_server, sql_database, server, database, query) {
    wrapped_query <- wrap_sql_request(query, sql_server, sql_database)
    query_kusto(server, database, wrapped_query)
}