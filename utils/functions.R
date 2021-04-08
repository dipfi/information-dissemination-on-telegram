library(docstring)

read_telegram_messages <- function(in_path = "input/", ignore = "", out_path = "output/"){

  #'This function reads in all the .json files containing telegram chat histories from a specified location
  #'
  #'@description Saves an RDS-file "all_messages.rds" containing all the messages from all the chats to the output-location.
  #'
  #'@param in_path character. path of the folder containing the .json telegram files
  #'@param ignore character. files or folders in input location which should be ignored
  #'@param out_path character. path of the output-folder for the RDS file
  #
  library(data.table)
  library(jsonlite)
  
  all_msg <- data.table()
  
  filenames <- list.files(in_path)
  filenames <- filenames[!(filenames %in% ignore)]
  
  start_time_outer <- Sys.time()
  
  for (i in filenames){
    start_time_inner <- Sys.time()
    temp0 <- fromJSON(txt=paste0("input/",i))
    
    temp <- as.data.table(temp0$messages)
    temp$channel <- temp0$name
    temp$channel_type <- temp0$type
    
    all_msg <- rbind(all_msg, temp,fill=T)
    print(paste("time to read ", i, ": ", round(Sys.time() - start_time_inner,2)))
  }
  
  print(paste("time to read all input files: ", round(Sys.time() - start_time_outer,2)))
  
  rm(temp,temp0,i,filenames)
  
  saveRDS(all_msg, paste(out_path, "all_messages.rds", sep = ""))
  
  rm(all_msg)
}


prepare_telegram_messages <- function(in_file = "output/all_messages.rds", out_path = "output/"){
  
  #'This function reduces the large rds files with telegram messages to the relevant components and prepares the data for analysis
  #'
  #'@description Saves an RDS-file "all_messages_prepared.rds" with the prepared telegram messages, reduced to relevant features to the output location
  #'
  #'@param in_file character. path of the RDS file containing the telegram chat messages from all chats
  #'@param out_path character. path of the output folder for the new RDS file
  
  library(data.table)
  library(lubridate)
  
  all_msg <- readRDS(in_file)
  
  #IF action is empty, then the action was a message
  all_msg$action <- ifelse(is.na(all_msg$action), all_msg$type, all_msg$action)
  
  
  #Convert time to datetime and day
  all_msg$date <- ymd_hms(all_msg$date)
  
  
  # Services have the username in ?actor? and text have  it in a 'from'
  # join actor and from and do the same for id
  
  all_msg$user <- ifelse(is.na(all_msg$actor), all_msg$from, all_msg$actor)
  all_msg$user_id <- ifelse(is.na(all_msg$actor_id), all_msg$from_id, all_msg$actor_id)
  
  
  #Delete unnecessary rows
  all_msg <- all_msg[,c("date",
                       "action",
                       "title",
                       "text",
                       "forwarded_from",
                       "message_id",
                       "reply_to_message_id",
                       "channel",
                       "channel_type",
                       "user",
                       "user_id")]
  # all_msg[,c("type","id","actor","actor_id", "from","from_id","file","thumbnail","mime_type","duration_seconds","width","height","photo","edited",
  #                 "poll.question","poll.closed","poll.total_voters","poll.answers","performer","sticker_emoji","inviter","location_information.longitude",
  #                 "location_information.latitude","contact_vcard","contact_information.phone_number","contact_information.last_name",
  #                 "contact_information.first_name","saved_from","members","author","via_bot")]
  
  print("Replacement of special characters started")
  start_time_replace_special_chars <- Sys.time()
  all_msg$channel <- gsub("[^[:alnum:] ]", "", all_msg$channel)
  all_msg$forwarded_from <- gsub("[^[:alnum:] ]", "", all_msg$forwarded_from)
  all_msg$user <- gsub("[^[:alnum:] ]", "", all_msg$user)
  print(paste("Time used to replace special characters:", Sys.time()-start_time_replace_special_chars))
  
  # Distinct channel id 
  all_msg[, channel_id := .GRP , by=list(channel)]
  
  # Distinct Message Id
  all_msg$own_id <- 1:nrow(all_msg)
  
  #Save as RDS to directly load data instead of doing manipulation every time
  
  saveRDS(all_msg, paste(out_path, "all_messages_prepared.rds", sep = ""))
  
  rm(all_msg)
}





extract_links <- function(in_file = "output/all_messages_prepared.rds", out_path = "output/", domains_file = "utils/top-level-domain-names.csv"){
  
  #'This function reads out the links from the telegram messages
  #'
  #'@description Saves an RDS-file "links.rds" including relevant information on the links posted in the different chats.
  #'Adds the number of links shared per message to the "all_messages_prepared.rds" file.
  #'
  #'@param in_file character. path to the RDS file containing the prepared telegram chat messages from all chats
  #'@param out_path character. path of the output folder for the new RDS file
  #'@param domains_file data.frame. path to a data frame of common domain names. Top-level domains need to be accessible in the column called "Domain"
  
  library(data.table)
  
  all_msg_prep <- readRDS(in_file)
  
  library(data.table)
  library(stringr)
  
  links <- data.table(own_id=numeric(),channel=character(),link=character())
  start_time_loop <- Sys.time()
  for (i in 1:nrow(all_msg_prep)){
    
    if(i %% 50000 == 0){print(paste("Links extracted for",round((i/nrow(all_msg_prep))*100,1),"% of all messages. Time used: ", round(Sys.time() - start_time_loop, 2)))}
    if (is.data.frame(all_msg_prep$text[[i]])){
      if(any(all_msg_prep$text[[i]][c("type")] == "link")){
        links <- rbind(links,
                       cbind(own_id = all_msg_prep$own_id[i],
                             channel = all_msg_prep$channel[i],
                             link = all_msg_prep$text[[i]][all_msg_prep$text[[i]]$type == "link", c("text")]),
                       fill = T)
      }
    }
    
    else if (is.list(all_msg_prep$text[[i]])){
      for (j in 1:length(all_msg_prep$text[[i]])){
        if (is.list(all_msg_prep$text[[i]][[j]])){
          if (all_msg_prep$text[[i]][[j]]$type == "link"){
            links <- rbind(links,
                           cbind(own_id= all_msg_prep$own_id[i],
                                 channel=all_msg_prep$channel[i],
                                 link=all_msg_prep$text[[i]][[j]]$text),
                           fill=T)
          }
        }
      }
    }
    
  }
  
  links$link<-tolower(links$link)
  links$link<-gsub("https://|http://|www.","",links$link)
  
  #Split at the end of this strings, otherwise use whole link
  domains <- read.csv(file = domains_file, header = T)$Domain
  list_to_split<-c(paste(domains, collapse = "/|"))
  links$end <- str_locate(pattern = list_to_split, links$link)[,2]-1
  links$end <- ifelse(is.na(links$end), nchar(links$link), links$end)
  links$link_short<-substr(links$link,1,links$end)
  links$end<-NULL
  
  # Adapt telegram internal links to chat
  links$telegram_chat<-ifelse(links$link_short=="t.me",gsub("t.me/","",links$link),NA) 
  links$telegram_chat<-gsub("^s/","",links$telegram_chat)
  links$end<-str_locate(links$telegram_chat,"/")[,1]-1
  links$telegram_chat<-ifelse(!is.na(links$end),substr(links$telegram_chat,1,links$end),links$telegram_chat)
  links$end<-NULL
  
  #youtubes ersetzen
  links$link_short<-gsub("youtu.be|m.youtube.com","youtube.com",links$link_short)
  
  #ensure all ids are saved as integers for compatibility
  links$own_id <- as.integer(links$own_id)
  all_msg_prep$own_id <- as.integer(all_msg_prep$own_id)
  
  all_msg_prep <- merge(all_msg_prep, links[, .("Count_of_Links" = .N), by = "own_id"], by = "own_id",all.x = T)
  
  links <- merge(links, all_msg_prep[, .(date, forwarded_from, own_id, user, user_id, channel_id, channel_type)], by = "own_id", all.x = T)
  
  print("Replacement of special characters started")
  start_time_replace_special_chars <- Sys.time()
  links$channel <- gsub("[^[:alnum:] ]", "", links$channel)
  links$forwarded_from <- gsub("[^[:alnum:] ]", "", links$forwarded_from)
  links$user <- gsub("[^[:alnum:] ]", "", links$user)
  print(paste("Time used to replace special characters:", Sys.time()-start_time_replace_special_chars))
  
  saveRDS(links, paste(out_path, "links.rds", sep = ""))
  saveRDS(all_msg_prep, paste(out_path, "all_messages_prepared.rds", sep = ""))
  
  rm(links)
  rm(all_msg_prep)
  print(paste("Total time used to extract links:", round(Sys.time() - start_time_loop, 2)))
}

perm_test_links <- function(links_file, reps = 10000){
  
  #'This function conducts a permutation test to test the Null hypothesis that links from domains connected to alternative media are not forwarded more that those connected to traditional media
  #'
  #'@description Returns a list with the plot of the permutation test, the p_value, the vector of average forward ratio by category and the observed average forward ratio by category
  #'
  #'@param links_file data.frame. containing the links, categories "alternative_media or "traditional_media", an the forward_ratio
  #'@param reps int. optional. default = 10000. number of permutations to be performed

  library(data.table)
  library(ggplot2)
  library(dplyr)
  
  #mean by category
  mu <- links_file[, .(mu=mean(ratio_forwarded)), by = category]
  
  #conduct a permutation test to retrieve a p-value for the Null that he mean forward-ratio of alternative media links is the same as for traditional media links
  n_alt <- sum(links_file$category == "alternative_media")
  n_trad <- sum(links_file$category == "traditional_media") 
  n <- nrow(links_file)
  
  mu_ratios_perm <-  setDT(data.frame(matrix(0,nrow = reps, ncol = 1)))
  colnames(mu_ratios_perm) = "mu_ratios_perm"
  
  for(i in 1:reps){
    samp <- sample(1:n, n_alt)
    mu_ratios_perm[i,"mu_ratios_perm"] <- mean(links_file$ratio_forwarded[samp])
  }
  
  #return p-value
  p_value <-  sum(mu_ratios_perm$mu_ratios_perm > mu$mu[mu$category == "alternative_media"])/reps
  
  #plot the distribution of the mean forward ratio under the Null and the actual observed mean forward-ratio
  p <- mu_ratios_perm %>% 
    ggplot(aes(x = mu_ratios_perm)) +
    geom_density() +
    geom_vline(aes(xintercept = mu$mu[mu$category == "alternative_media"],
                   color = "observed_mean_forward_ratio"),
               linetype="dashed"
              ) +
    scale_color_manual(name = "Legend:", values = c(observed_mean_forward_ratio = "red")) +
    theme(legend.position = "bottom") +
    ggtitle(paste("Permutation Test with", reps, "permutations: p-value =", p_value)) +
    xlab("Average forward-ratio of alternative media links") +
    ylab("Density")
  
  result <- list(p, p_value, mu_ratios_perm, mu)
  names(result) <- c("plot", "p_value", "average_forward_ratio_permutations", "average_forward_ratio_observed")
  return(result)
  
}
