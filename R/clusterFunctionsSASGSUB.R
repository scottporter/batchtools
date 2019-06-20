#' @title ClusterFunctions for SASGSUB
#'
#' @description
#' Cluster functions for SASGSUB (\url{http://support.sas.com/documentation/cdl/en/gridref/67371/HTML/default/viewer.htm}).
#'
#' Job files are created based on the brew template \code{template.file}. This
#' file is processed with brew and then submitted to the queue using the
#' \code{sasgsub} command. Jobs are killed using the \code{sasgsub -KILLGRIDJOB} command and the
#' list of running jobs is retrieved using \code{sasgsub -GRIDGETRESULTS}. The user
#' must have the appropriate privileges to submit, delete and list jobs on the
#' cluster (this is usually the case).
#'
#' The template file can access all resources passed to \code{\link{submitJobs}}
#' as well as all variables stored in the \code{\link{JobCollection}}.
#' It is the template file's job to choose a queue for the job and handle the desired resource
#' allocations.
#'
#' @note
#' Array jobs are currently not supported.
#'
#' @template template
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsSASGSUB = function(template = "sasgsub", scheduler.latency = 1, fs.latency = 65) { # nocov start
  template = findTemplateFile(template)
  if (testScalarNA(template))
    stopf("Argument 'template' (=\"%s\") must point to a readable template file or contain the template itself as string (containing at least one newline)", template)
  template = cfReadBrewTemplate(template)

  # When LSB_BJOBS_CONSISTENT_EXIT_CODE = Y, the bjobs command exits with 0 only
  # when unfinished jobs are found, and 255 when no jobs are found,
  # or a non-existent job ID is entered.
  Sys.setenv(LSB_BJOBS_CONSISTENT_EXIT_CODE = "Y")

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")
    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("sasgsub", stdin = shQuote(outfile))

    if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("sasgsub", res$exit.code, res$output)
    } else {
      batch.id = stri_extract_first_regex(stri_flatten(res$output, " "), "\\d+")
      makeSubmitJobResult(status = 0L, batch.id = batch.id)
    }
  }

  listJobs = function(reg, args) {
    assertRegistry(reg, writeable = FALSE)
    res = runOSCommand("sasgsub -GRIDGETRESULTS", args)
    if (res$exit.code > 0L) {
      if (res$exit.code == 255L || any(stri_detect_regex(res$output, "No (unfinished|pending|running) job found")))
        return(character(0L))
      OSError("Listing of jobs failed", res)
    }
    stri_extract_first_regex(tail(res$output, -1L), "\\d+")
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "sasgsub -KILLGRIDJOB", batch.id)
  }

  makeClusterFunctions(name = "SASGSUB", submitJob = submitJob, killJob = killJob, store.job.collection = TRUE, scheduler.latency = scheduler.latency, fs.latency = fs.latency)
} # nocov end
