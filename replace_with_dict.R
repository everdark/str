### Kyle Chung <alienatio@pixnet.net>
### string operation exercise



### Scenario:
### [raw data layout] key_int1:score_numeric1, key_int2:score_numeric2...
### with an key_int-to-val_string mapping table,
### need to strip all scores string and translate key_int to val_string
### join is not allowed for performance concern
### so this requires string replacement with dictionary lookup

## small scale experiment
len <- 40000 # do no more than 40000 for the gregexpr solution
rscores <- runif(len)
rkeys <- sample(1:2000000, len, replace=TRUE)
rraw <- paste(rkeys, rscores, sep=':', collapse=',')
rvals <- as.character(sample(2000000:4000000, length(rkeys)))
map <- data.frame(key=as.character(rkeys), val=rvals, stringsAsFactors=FALSE)

# use
# substring replacement: thh straight forward approach (and indeed fast)
system.time(
    score_stripped <- gsub(':[0-9\\.]+', '', rraw)
    )
# or
# regular expression extraction (regmatches NOT able to extract captures only)
# compare to the gsub solution, regmatch leads to result vectorized
regmatches(rraw, gregexpr('([0-9]+):', rraw, perl=TRUE))      # problematic
regmatches(rraw, gregexpr(':[0-9\\.]+,?', rraw), invert=TRUE) # not good enough
# work-around on capture-only extraction
capture_extract <- function(dat, re) {
    start.pos <- attr(re[[1]], "capture.start")
    end.pos <- start.pos + attr(re[[1]], "capture.length") - 1L
    item_extracted <- substring(dat, start.pos, end.pos)
    item_extracted
}
capture_extract(rraw, gregexpr('([0-9]+):', rraw, perl=TRUE))
# but performance issue of gregexpr on long character
system.time(
    tt <- gregexpr('([0-9]+):', rraw, perl=TRUE)
    )

# create low-level hash table
hash <- new.env()
for( i in seq(nrow(map)) )
    hash[[ map$key[i] ]]<- map$val[i]
dict <- as.list(hash) # better than directly create a list, if not already exist

# use gsubfn for dictionary string replacement
result <- gsubfn::gsubfn('[[:digit:]]+', dict, score_stripped)
str(result) # check


## large scale implementation
# discard hash table solution due to possible explicit looping bottleneck
nrows <- 400000 # take minutes to produce sythetic data for nrows = 400000
nitems <- 40
rscores <- matrix(runif(nrows*40), nrow=nrows, ncol=nitems)
ritems <- matrix(sample(1:1000000, nrows*nitems, replace=TRUE), 
                 nrow=nrows, ncol=nitems)
system.time(
    rdata <- matrix(paste(ritems, rscores, sep=':'), nrow=nrows)
    )
rdata <- apply(rdata, 1, function(r) paste(r, collapse=','))
rkeys <- as.character(unique(as.vector(ritems)))
rvals <- as.character(sample(2000000:4000000, length(rkeys)))
map <- data.frame(key=as.character(rkeys), val=rvals, stringsAsFactors=FALSE)

dict_translate <- function(x, dict) {
    newx <- x
    matched <- match(x, dict$key)
    mindex <- !is.na(matched)
    newx[mindex] <- map$val[matched[mindex]]
    newx
}
dict_translate <- compiler::cmpfun(dict_translate)

# unlist-relist solution, scalable!
stripsplit <- function(x) {
    stripped <- gsub(':[0-9\\.]+', '', x)
    splitted <- strsplit(stripped, split=',')
    splitted <- as.relistable(splitted)
    splitted_unlist <- unlist(splitted)
    splitted_unlist
}
system.time(
    splitted_unlist <- stripsplit(rdata)
    )
system.time(
    translated <- dict_translate(splitted_unlist, dict=map)
)
system.time(
    translated <- relist(translated)
    )
system.time(
    result <- lapply(translated, paste, collapse=',')
    )

# lapply solution, not scalable (dont run for nrows > 4000)
system.time(
    translated <- lapply(splitted, dict_translate, dict=map)
)    
# parallel lapply won't fix the scalability (of course)
library(parallel)
cl <- makeCluster(type="PSOCK", rep(c("localhost"), detectCores()))
system.time(
    clusterExport(cl, 'map')
    )
system.time(
    tt <- parLapply(cl, splitted, dict_translate, dict=map)
    )
stopCluster(cl)