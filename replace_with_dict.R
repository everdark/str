### Kyle Chung <alienatio@pixnet.net>
### string operation exercise

## string replace
nrows <- 4000
nitems <- 40
rscores <- matrix(runif(nrows*40), nrow=nrows, ncol=nitems)
ritems <- matrix(sample(1:2000000, 1000000, replace=TRUE), 
                 nrow=nrows, ncol=nitems)
rdata <- matrix(paste(ritems, rscores, sep=':'), nrow=nrows)
rdata <- apply(rdata, 1, function(r) paste(r, collapse=','))

system.time(
    tt <- gsub(':[0-9\\.]+', '', rdata)
)

item_list <- as.character(unique(as.vector(ritems)))
val='13-132-142[eMH;723:123:324;563:433:41]'

hash <- new.env()
for(i in seq(nrow(hash.df))) 
    hash[[ item_list[i] ]]<- val
dict <- as.list(hash)
library(gsubfn)
gsubfn(".", dict, kk)



kk <- tt[1]
kk <- unlist(strsplit(kk, split=','))



kk
gsubfn(item_list[1], val, kk)
chartr(item_list[1], val, kk)



regmatches(rrow, gregexpr('[[:digit:]]+:', rrow))
