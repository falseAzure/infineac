library(data.table)
library(readxl)
library(stringi)

# Earnings Calls
data.earnings_calls <- fread("../output/data/overview.csv")
data.earnings_calls[,comp:=stri_replace_all(tolower(company), replacement =  '', regex = '\\.')]
data.earnings_calls[,end_ticker:=stri_locate(data.earnings_calls$ticker, regex="\\.")[,1]-1]
data.earnings_calls[is.na(end_ticker), end_ticker:=nchar(end_ticker)]
data.earnings_calls[, tic:=stri_sub(str = ticker, 1, end_ticker)]

# Company Info Orbis
data.orbis <- data.table(read_xlsx("../data/orbis/Company Info OrbisCrossBorder.xlsx"))
data.orbis[,comp:=stri_replace_all(tolower(companyname), replacement =  '', regex = '\\.')]
# to many duplicate tickers
data.orbis[, duplicate_tic:=duplicated(tic)]
data.orbis[, duplicate_company:=duplicated(comp)]

# Test
data.orbis[comp %in% data.orbis[duplicate_company==T]$comp & !is.na(ISINnumber)]

data.orbis <- data.orbis[duplicate_company==FALSE]

data.merged.orbis <- unique(
  rbindlist(list(
    merge(unique(data.earnings_calls[, .(comp, company, tic)]),
          data.orbis[,.(comp, companyname, OrbisIDnumber, BvDIDnumber, ISINnumber)],
          by = "comp")
    # merge(unique(data.earnings_calls[, .(comp, company, tic)]),
    #       unique(data.orbis[,.(companyname, tic)]),
    #       by = "tic")
    ),
    use.names = T
  )
)

data.merged.orbis[,tic_duplicate:=duplicated(tic)]

length(unique(data.orbis$companyname))
# Anteil der Earnings Calls, die in Orbis vorhanden sind
nrow(data.merged.orbis)/nrow(data.orbis)

fwrite(data.merged.orbis, "orbis_merged.csv")



data.merged.ec <- 
  unique(rbindlist(list(
    merge(data.earnings_calls,
          data.orbis[,.(comp, companyname)],
          by = "comp")
    # merge(data.earnings_calls,
    #       unique(data.orbis[,.(companyname, tic)]),
    #       by = "tic")
    ),
    use.names = T
  )
)
nrow(data.merged.ec)/nrow(data.earnings_calls)*100

# Orbis: Abnormal Returns
orbis.ar <- data.table(readRDS("../data/orbis/data_ar.rds"))
orbis.guo <- data.table(read_xlsx("../data/orbis/guo_listing.xlsx", sheet = "Ergebnisse"))[,-1]

orbis.ar <- merge(orbis.ar, orbis.guo, by.x = "bvd_id", by.y="BvD ID Nummer")



###
data.sanctions <- fread("../output/data/overview.csv", na.strings = c(""))
data.sanctions <- data.sanctions[,.(company, ticker, ticker_new, date, russia, russia_count, sanction_count)]
data.sanctions <- merge(data.sanctions,
                        unique(data.merged.orbis[,.(company, ISINnumber, BvDIDnumber, OrbisIDnumber)]),
                        by="company", all.x = T)

fwrite(x = data.sanctions, file = "../output/data/overview_sanctions_combined.csv")

data.sanctions[!is.na(OrbisIDnumber)]
