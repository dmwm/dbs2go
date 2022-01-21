#!/usr/bin/env Rscript
# clean-up session parametersRead qrm(list=ls())

# load data
my.path <- paste0(getwd(), "/")
# example
# load.data <- read.csv(paste0(my.path, file.name), header=TRUE)

# set seed
set.seed(12345)

# load libraries
libs <- c("ggplot2")
for(i in 1:length(libs)) {
    pkg <- sprintf("%s", libs[i])
    print(sprintf("load %s", pkg))
    suppressMessages(library(pkg, character.only = TRUE))
}


df=read.csv("data.csv", header=T)
xdf <- df
xdf$Task <- factor(df$Task,  # Change ordering manually
                   levels=c("# vulnerabilities/image", "Image size", "Memory allocation", "Read query time", "Lines of code"))
pdf("dbs-perf.pdf")
ggplot(xdf,aes(x=Task,y=Percentage,fill=Implementation))+geom_bar(stat="identity",width=0.3)+coord_flip()+ggtitle("DBS performance using Python/Go implementations")+scale_fill_manual(values=c("#7A81FF", "#76D6FF"))
