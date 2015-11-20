.findAll = function(reg) {
  reg$status[, "job.id", with = FALSE]
}

.findSubmitted = function(reg, ids = NULL) {
  submitted = NULL
  if (is.null(ids))
    reg$status[!is.na(submitted), "job.id", with = FALSE]
  else
    reg$status[ids][!is.na(submitted), "job.id", with = FALSE]
}

.findNotSubmitted = function(reg, ids = NULL) {
  submitted = NULL
  if (is.null(ids))
    reg$status[is.na(submitted), "job.id", with = FALSE]
  else
    reg$status[ids][is.na(submitted), "job.id", with = FALSE]
}

.findStarted = function(reg, ids = NULL) {
  started = NULL
  if (is.null(ids))
    reg$status[!is.na(started), "job.id", with = FALSE]
  else
    reg$status[ids][!is.na(started), "job.id", with = FALSE]
}

.findNotStarted = function(reg, ids = NULL) {
  started = NULL
  if (is.null(ids))
    reg$status[is.na(started), "job.id", with = FALSE]
  else
    reg$status[ids][is.na(started), "job.id", with = FALSE]
}

.findDone = function(reg, ids = NULL) {
  done = error = NULL
  if (is.null(ids))
    reg$status[!is.na(done) & is.na(error), "job.id", with = FALSE]
  else
    reg$status[ids][!is.na(done) & is.na(error), "job.id", with = FALSE]
}

.findNotDone = function(reg, ids = NULL) {
  done = error = NULL
  if (is.null(ids))
    reg$status[is.na(done) | !is.na(error), "job.id", with = FALSE]
  else
    reg$status[ids][is.na(done) | !is.na(error), "job.id", with = FALSE]
}

.findError = function(reg, ids = NULL) {
  error = NULL
  if (is.null(ids))
    reg$status[!is.na(error), "job.id", with = FALSE]
  else
    reg$status[ids][!is.na(error), "job.id", with = FALSE]
}

.findTerminated = function(reg, ids = NULL) {
  done = NULL
  if (is.null(ids))
    reg$status[!is.na(done), "job.id", with = FALSE]
  else
    reg$status[ids][!is.na(done), "job.id", with = FALSE]
}

.findNotTerminated = function(reg, ids = NULL) {
  done = NULL
  if (is.null(ids))
    reg$status[is.na(done), "job.id", with = FALSE]
  else
    reg$status[ids][is.na(done), "job.id", with = FALSE]
}

.findOnSystem = function(reg, ids = NULL) {
  if (is.null(reg$cluster.functions$listJobs))
    return(data.table(job.id = integer(0L), key = "job.id"))
  batch.ids = reg$cluster.functions$listJobs(reg)
  submitted = done = batch.id = NULL
  if (is.null(ids))
    reg$status[!is.na(submitted) & is.na(done) & batch.id %in% batch.ids, "job.id", with = FALSE]
  else
    reg$status[ids][!is.na(submitted) & is.na(done) & batch.id %in% batch.ids, "job.id", with = FALSE]
}

#' @title Find and filter jobs
#'
#' @description
#' Use \code{findJobs} to query jobs for which a predicate expression, evaluated on the parameters, yields \code{TRUE}.
#' The other functions can be used to query the computational status.
#' Note that they do not synchronize the registry, thus you are advised to run \code{\link{syncRegistry}} yourself
#' or, if you are really just interested in the status, use \code{\link{getStatus}}.
#'
#' @param expr [\code{expression}]\cr
#'   Predicate expression evaluated in the job parameters.
#' @templateVar ids.default all
#' @template ids
#' @template reg
#' @return [\code{data.table}]. Matching job ids are stored in the column \dQuote{job.id}.
#' @export
#' @examples
#' reg = makeTempRegistry(make.default = FALSE)
#' batchMap(identity, i = 1:3, reg = reg)
#' ids = findNotSubmitted(reg = reg)
#'
#' # get all jobs:
#' findJobs(reg = reg)
#'
#' # filter for jobs with parameter i >= 2
#' findJobs(i >= 2, reg = reg)
#'
#' # filter on the computational status
#' findSubmitted(reg = reg)
#' findNotDone(reg = reg)
findJobs = function(expr, ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, strict = TRUE)
  syncRegistry(reg)
  if (missing(expr))
    return(asIds(reg, ids, default = .findAll(reg)))

  ids = asIds(reg, ids, default = .findAll(reg))
  expr = substitute(expr)
  ee = parent.frame()
  fun = function(pars) eval(expr, pars, enclos = ee)
  pars = NULL
  reg$status[ids][reg$defs, on = "def.id", nomatch = 0L][vlapply(pars, fun), "job.id", with = FALSE]
}

#' @export
#' @rdname findJobs
#' @param prob.name [\code{character(1)}]\cr
#'   Whitelist of problem names.
#' @param algo.name [\code{character(1)}]\cr
#'   Whitelist of algorithm names.
#' @param prob.pars [\code{expression}]\cr
#'   Predicate expression evaluated in the problem parameters.
#' @param algo.pars [\code{expression}]\cr
#'   Predicate expression evaluated in the algorithm parameters.
#' @param repls [\code{integer}]\cr
#'   Whitelist of replication numbers.
findExperiments = function(prob.name, algo.name, prob.pars, algo.pars, repls = NULL, ids = NULL, reg = getDefaultRegistry()) {
  assertExperimentRegistry(reg)
  syncRegistry(reg)
  ee = parent.frame()
  pars = repl = NULL

  ids = asIds(reg, ids, default = .findAll(reg))
  tab = reg$status[ids][reg$defs, c("job.id", "pars", "repl"), on = "def.id", nomatch = 0L, with = FALSE]

  if (!missing(prob.name)) {
    assertCharacter(prob.name, any.missing = FALSE)
    tab = tab[vcapply(pars, "[[", "prob.name") %in% prob.name]
  }

  if (!missing(algo.name)) {
    assertCharacter(algo.name, any.missing = FALSE)
    tab = tab[vcapply(pars, "[[", "algo.name") %in% algo.name]
  }

  if (!missing(prob.pars)) {
    expr = substitute(prob.pars)
    fun = function(pars) eval(expr, pars$prob.pars, enclos = ee)
    tab = tab[vlapply(pars, fun)]
  }

  if (!missing(algo.pars)) {
    expr = substitute(algo.pars)
    fun = function(pars) eval(expr, pars$algo.pars, enclos = ee)
    tab = tab[vlapply(pars, fun)]
  }

  if (!is.null(repls)) {
    repls = asInteger(repls, any.missing = FALSE)
    tab = tab[repl %in% repls]
  }

  return(tab[, "job.id", with = FALSE])
}

#' @export
#' @rdname findJobs
findSubmitted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findSubmitted(reg, asIds(reg, ids))
}

#' @export
#' @rdname findJobs
findNotSubmitted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findNotSubmitted(reg, asIds(reg, ids))
}

#' @export
#' @rdname findJobs
findStarted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findStarted(reg, asIds(reg, ids))
}

#' @export
#' @rdname findJobs
findNotStarted = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findNotStarted(reg, asIds(reg, ids))
}

#' @export
#' @rdname findJobs
findDone = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findDone(reg, asIds(reg, ids))
}

#' @export
#' @rdname findJobs
findNotDone = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findNotDone(reg, asIds(reg, ids))
}

#' @export
#' @rdname findJobs
findError = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findError(reg, asIds(reg, ids))
}

#' @export
#' @rdname findJobs
findOnSystem = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  syncRegistry(reg)
  .findOnSystem(reg, asIds(reg, ids))
}
