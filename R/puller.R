#' Log into the Azure CLI by running the `az login` command.
#' @param tenant The Azure Active Directory tenant name or GUID.
#' @param use_device_code default FALSe. Set to TRUE if you wish to use device
#' code flow instead of interactive web sign-in flow.
#' @export
az_login <- function(tenant, use_device_code = FALSE) {
    args <- c("login")
    if (!missing(tenant)) args <- c(args, paste("--tenant", tenant))
    if (use_device_code) args <- c(args, "--use-device-code")
    execute_az_cmd(list(command = "az", args = args))
}

#' Get an access token using the Azure CLI
#' @param resource The URI of the Azure resource to which to authenticate.
#' @param tenant The Azure Active Directory tenant name or GUID.
#' @param use_cache default TRUE. If TRUE, tokens will be cached for reuse.
#' @details Runs the `az account get-access-token` command to retrieve an access
#' token from Azure via the Azure CLI. Presumes that you are signed in. If not,
#' call `az_login()` first.
#'
#' @export
get_az_cli_token <- function(resource, tenant, use_cache = TRUE) {
    if (missing(tenant))
        AzureTokenCLI$new(resource = resource, use_cache = use_cache)
    else
        AzureTokenCLI$new(resource = resource, tenant = tenant,
            use_cache = use_cache)
}

#' Get an access token for Kusto using authorization code flow.
#' @param cluster The cluster name or URI. e.g.: "mycluster.westus"
#' @param tenant The Azure Active Directory tenant name or GUID.
#' @param use_cache default TRUE. If TRUE, tokens will be cached for reuse.
#' @export
get_kusto_auth_token <- function(cluster, tenant = "common", use_cache = TRUE) {
    if (startsWith(cluster, "https://")) {
        resource <- cluster
    } else {
        resource <- glue::glue("https://{cluster}.kusto.windows.net")
    }
    AzureAuth::get_azure_token(resource = resource, tenant = tenant,
        use_cache = use_cache, app = "db662dc1-0cfe-4e1c-a843-19a68e65be58")
}

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
execute_az_cmd <- function(cmd) {
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

process_cli_response <- function(res, resource) {
    # Parse the JSON from the CLI and fix the names to snake_case
    ret <- jsonlite::parse_json(res)
    tok_data <- list(
        token_type = ret$tokenType,
        access_token = ret$accessToken,
        tenant = ret$tenant,
        expires_on = as.numeric(as.POSIXct(ret$expiresOn))
    )
    # CLI doesn't return resource identifier so we need to pass it through
    if (!missing(resource)) tok_data$resource <- resource
    return(tok_data)
}

#' Run a Kusto query and export results to Azure Storage in Parquet or CSV
#' format.
#'
#' @param query The text of the Kusto query to run
#' @param name_prefix The filename prefix for each exported file
#' @param storage_uri The URI of the blob storage container to export to
#' @param key The account key for the storage container. Default "impersonate"
#' uses the identity that is signed into Kusto to authenticate to Azure Storage.
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

#' Execute the Kusto query and export the result to Azure Storage.
#' @param tbl An instance of AzureKusto::tbl_kusto
#' @param name_prefix The filename prefix to use for exported files.
#' @param storage_uri The Azure Storage URI to export files to.
#' @param key default "impersonate" which uses the account signed into Kusto to
#' authenticate to Azure Storage. An Azure Storage account key.
#' @param ... pass-through parameters to AzureKusto::run_query
#' @export
export.tbl_kusto <- function(tbl, name_prefix, storage_uri,
    key = "impersonate", ...) {
    # TODO: upstream this to the AzureKusto package
    q <- AzureKusto::kql_build(tbl)
    q_str <- kusto_export_cmd(
        AzureKusto::kql_render(q), name_prefix,
        storage_uri, key
    )
    params <- c(tbl$params, list(...))
    params$database <- tbl$src
    params$qry_cmd <- q_str
    res <- do.call(AzureKusto::run_query, params)
    tibble::as_tibble(res)
}

#' @export
export <- function(object, ...) {
    UseMethod("export")
}
.S3method("export", "tbl_kusto", export.tbl_kusto)

#' Wrap a SQL query in a Kusto request
wrap_sql_request <- function(query, sql_server, sql_database) {
    conn_str <- glue::glue('Server={sql_server},1433;Authentication="Active Directory Integrated";Initial Catalog={sql_database};')
    query <- gsub("[\r\n]", " ", query)
    glue::glue("set notruncation; evaluate sql_request('{conn_str}', \"{query}\")")
}

#' Query a Kusto server/database using an Azure CLI token by default.
#' 
query_kusto <- function(server_url, database, query, token) {
    if (missing(token)) token <- get_az_cli_token(server_url)
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

#' A subclass of AzureAuth::AzureToken R6 class for tokens from Azure CLI.
#' @docType class
#' @export
AzureTokenCLI <- R6::R6Class("AzureTokenCLI", inherit = AzureAuth::AzureToken,
    # TODO: upstream this to the AzureAuth package.
    public = list(
        #' @param resource The URI of the Azure resource to which to authenticate.
        #' @param tenant The Azure Active Directory tenant name or GUID.
        #' @param aad_host The Azure Active Directory authority.
        #' @param version The Azure Active Directory API version (1 or 2)
        #' @param use_cache default TRUE. If TRUE, tokens will be cached for reuse.
        #' @param token_args pass through args to AzureAuth token constructor
        initialize = function(resource = NULL, tenant = NULL,
            aad_host = "https://login.microsoftonline.com/", version = 1,
            use_cache = NULL, token_args = list())
        {
            self$auth_type <- "cli"
            self$version <- version
            if (!is.null(resource)) {
                if (self$version == 1) self$resource <- resource
                else self$scope <- sapply(resource, AzureAuth::verify_v2_scope,
                    USE.NAMES = FALSE)
            }

            self$aad_host <- aad_host
            if (!is.null(tenant))
                self$tenant <- AzureAuth::normalize_tenant(tenant)
            self$token_args <- token_args
            private$use_cache <- use_cache

            # use_cache = NA means return dummy object:
            # initialize fields, but don't contact AAD
            if (is.na(use_cache)) return()

            if (use_cache) private$load_cached_credentials()

            # time of initial request for token: in case we need to set expiry
            # time manually
            request_time <- Sys.time()
            if (is.null(self$credentials)) {
                res <- private$initfunc(auth_info)
                self$credentials <- private$process_response(res)
            }
            private$set_expiry_time(request_time)

            if (private$use_cache)
                self$cache()
        }
    ),
    private = list(
        initfunc = function(init_args) {
            tryCatch(
                {
                    if (is.null(self$tenant)) {
                        cmd <- build_az_token_cmd(resource = self$resource)
                    } else {
                        cmd <- build_az_token_cmd(resource = self$resource,
                            tenant = self$tenant)
                    }
                    result <- execute_az_cmd(cmd)
                    # result is a multi-line JSON string, concatenate together
                    paste0(result)
                },
                warning = function(cond) {
                    not_found <- grepl("not found", cond, fixed = TRUE)
                    not_loggedin <- grepl("az login", cond, fixed = TRUE) |
                        grepl("az account set", cond, fixed = TRUE)
                    bad_resource <- grepl(
                        "was not found in the tenant",
                        cond,
                        fixed = TRUE
                    )
                    if (not_found)
                        message("Azure CLI not found on path.")
                    else if (not_loggedin)
                        message("Please run 'az login' to set up account.")
                    else
                        message("Failed to invoke the Azure CLI.")
                }
            )
        },
        process_response = function(res) {
            processed <- process_cli_response(res, self$resource)
            self$tenant <- processed$tenant
            processed
        }
    )
)
