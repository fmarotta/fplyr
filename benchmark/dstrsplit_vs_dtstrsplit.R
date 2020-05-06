library(fplyr)
library(ggplot2)
library(iotools)
library(microbenchmark)

file <- "flights14.csv"
times <- 500
sep = ","
col.classes <- c("integer", "integer", "integer", "integer", "integer",
				 "character", "character", "character",
				 "integer", "integer", "integer")
header = FALSE
col.names = paste0("V", 1:11)
stringsAsFactors <- FALSE
select <- NULL
drop <- NULL

# Initialise dtstrsplit
dtstrsplit <- fplyr:::DefineFormatter(sep,
									  col.classes,
									  stringsAsFactors,
									  col.names,
									  select,
									  drop)

# Run the benchmark
mb <- microbenchmark(
	fread = fread(file,
		  sep = sep,
		  stringsAsFactors = stringsAsFactors,
		  header = header,
		  col.names = col.names,
		  colClasses = col.classes,
		  select = select,
		  drop = drop),
	dtstrsplit = dtstrsplit(readAsRaw(file)),
	dstrsplit = dstrsplit(readAsRaw(file),
			  col_types = col.classes,
			  sep = sep),
	times = times
)
mb$time <- mb$time / 1e6 # Convert to milliseconds

# Plot the results
theme_set(theme_bw())
g <- qplot(x = expr, y = time, data = mb, fill = expr, geom = "boxplot",
	  xlab = "Method", ylab = "Time (milliseconds)") +
					 theme(legend.position = "none")
ggsave("fread_vs_dtstrsplit.pdf", g)
