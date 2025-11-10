library(tidyr)

minCountFilter <- function(x, col_names, minCounts) {
  if(nrow(x) != 0) {
    return(x %>% mutate(across(col_names, ~ if_else(.x < minCounts, NA, .x))))
  }
}

get_counts <- function(cdm, tbl_name) {
  tryCatch(
    {
      cdm[[tbl_name]] %>%
        summarise(
          n_rec = n(),
          n_sub = n_distinct("person_id")
        ) %>%
        mutate(table = tbl_name) %>%
        collect()
    },
    error = function(e) {
      message("Table not found or empty: ", tbl_name)
      tibble(n_rec = 0, n_sub = 0, table = tbl_name)
    }
  )
}


summarise_cols_na <- function(cdm, table_name){
  cols <- colnames(cdm[[table_name]])
  results <- lapply(cols, function(col) {
    cdm[[table_name]] %>%
      summarise(
        n_na = sum(as.integer(is.na(.data[[col]]))),
        n_total  = n()
      ) %>%
      collect() %>%
      mutate(
        variable = col,
        pct_na = n_na / n_total * 100
      )
  })
  result_tbl <- bind_rows(results)
}

getChecksData <- function(server_dbi, port, host, user, password, cdm_schema, write_schema, output_folder =c("Results")) {
  
  # mother_table_name <- "pregnancy_episode"
  # minimum_counts <- 5
  table_stem <- "omop_checks_"
  if(!exists("minimum_counts")){
    minimum_counts = 5
  }
  # output folder
  if (!dir.exists(output_folder)) {
    dir.create(output_folder)
  }
  #CDM Connection
  db <- DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = server_dbi,
    port = port,
    host = host,
    user = user,
    password = password
  )
  cdm <- CDMConnector::cdmFromCon(
    con = db, 
    cdmSchema = cdm_schema, 
    writeSchema = c("schema" = write_schema, "prefix" = tolower(table_stem))
  )
  #CDM add mother table
  if(!exists("mother_table_name")){
    mother_table = NULL
    print("Mother table name was not provided.")
  }else if(is.null(mother_table_name)){
    mother_table = NULL
    print("Mother table name was not provided.")
  }else{
    mother_table <- tryCatch(
      {
        # readSourceTable(cdm, mother_table_name)
        dplyr::tbl(db, DBI::Id(schema = cdm_schema, mother_table_name))
      },
      error = function(e){
        message("Table was not found: ", mother_table_name)
        return(NULL)
      }
    )
  }
  if(!is.null(mother_table)){
    cdm$mother_table <- mother_table %>%
      compute(name="mother_table", overwrite=TRUE)
  }

  # Snapshot
  CDMConnector::snapshot(cdm) %>%
    write_csv(paste0(output_folder, "/cdm_snapshot.csv"))

  # Person counts
  person_db <- cdm$person %>%
    select(person_id, year_of_birth) %>%
    PatientProfiles::addSex() %>%
    left_join(
      cdm$observation_period %>%
        mutate(
          observation_period_start_date = lubridate::floor_date(observation_period_start_date, "month"),
          observation_period_end_date = lubridate::floor_date(observation_period_end_date, "month")
        ) %>%
        select(person_id, observation_period_start_date, observation_period_end_date)
    ) %>%
    compute()

  # births per year and sex
  person_db %>%
    group_by(year_of_birth, sex) %>%
    tally() %>%
    collect()%>%
    minCountFilter("n", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/birth.csv"))

  # observation start and end per month
  person_db %>%
    group_by(observation_period_start_date) %>%
    tally(name = "n_start") %>%
    mutate(date = observation_period_start_date) %>%
    select(date, n_start) %>%
    left_join(
      person_db %>%
        group_by(observation_period_end_date) %>%
        tally(name = "n_end") %>%
        mutate(date = observation_period_end_date) %>%
        select(date, n_end)
    ) %>%
    collect() %>%
    minCountFilter(c("n_start", "n_end"), minimum_counts) %>% 
    write_csv(paste0(output_folder, "/observation_period.csv"))

  # death
  cdm$death %>%
    group_by(year(death_date)) %>%
    tally() %>%
    rename("death_date" = "year(death_date)") %>%
    collect() %>%
    minCountFilter("n", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/death.csv"))

  #Counts per table
  bind_rows(
    get_counts(cdm, "condition_occurrence"),
    get_counts(cdm, "drug_exposure"),
    get_counts(cdm, "visit_occurrence"),
    get_counts(cdm, "observation"),
    get_counts(cdm, "measurement"),
    get_counts(cdm, "procedure_occurrence"),
    get_counts(cdm, "device_exposure")
  ) %>%
  write_csv(paste0(output_folder, "/table_counts.csv"))

  # # NAs en las tablas
  # table_names <- c("condition_occurrence", "drug_exposure", "visit_occurrence", "observation",
  #                  "measurement", "procedure_occurrence", "device_exposure")
  # summary_nas <- lapply(table_names, function(tbl) {
  #   if(!omopgenerics::isTableEmpty(cdm[[tbl]])){
  #     summarise_cols_na(cdm, tbl) %>%
  #       mutate(table = tbl)
  #   }
  # })
  # bind_rows(summary_nas) %>%
  #   mutate(pct_na = round(pct_na, 3)) %>%
  #   write_csv(paste0(output_folder, "/table_cols_na.csv"))

  # #Conditions source value
  # cdm$condition_occurrence %>%
  #   group_by(condition_source_concept_id) %>%
  #   tally() %>%
  #   left_join(
  #     cdm$concept %>%
  #       select(condition_source_concept_id = concept_id, concept_name, vocabulary_id)
  #   ) %>%
  #   group_by(vocabulary_id) %>%
  #   summarise(n_total_source_voc = sum(n)) %>%
  #   collect() %>%
  #   write_csv(paste0(output_folder, "/conditions_source_vocabularies.csv"))

  # Fact_relationship
  cdm$fact_relationship %>%
    group_by(relationship_concept_id) %>%
    tally() %>%
    left_join(cdm$concept %>% select(relationship_concept_id = concept_id , concept_name)) %>%
    select(relationship_concept_id, concept_name, n) %>%
    collect() %>%
    minCountFilter("n", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/fact_relationship.csv"))

  # Pregnancies
  if(!is.null(mother_table)){
    cdm$mother_table %>%
      mutate(date = lubridate::floor_date(pregnancy_start_date, "month")) %>%
      group_by(date) %>%
      tally(name = "n_start") %>%
      left_join(
        cdm$mother_table %>%
          mutate(date = lubridate::floor_date(pregnancy_end_date, "month")) %>%
          group_by(date) %>%
          tally(name = "n_end")
      ) %>%
      collect() %>%
      minCountFilter(c("n_start", "n_end"), minimum_counts) %>% 
      write_csv(paste0(output_folder, "/pregnancies.csv"))

  }

  #Source to concept map
  cdm$source_to_concept_map %>%
    collect() %>%
    write_csv(paste0(output_folder, "/source_to_concept.csv"))

  #Procedures: source vocabularies records
  cdm$procedure_occurrence %>%
    group_by(procedure_concept_id) %>%
    tally() %>%
    left_join(
      cdm$concept %>%
        select(procedure_concept_id = concept_id, concept_name, vocabulary_id)
    ) %>%
    group_by(vocabulary_id) %>%
    summarise(n_total_voc = sum(n)) %>%
    collect() %>%
    minCountFilter("n_total_voc", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/procedures.csv"))

  # Observations: source codes and mapping
  cdm$observation %>%
    group_by(observation_source_value, obs_concept_id = observation_concept_id) %>%
    tally() %>%
    left_join(
      cdm$concept %>%
        select(source_concept_id = concept_id, source_concept_name=concept_name, concept_code),
      by=c("observation_source_value"= "concept_code")
    )%>%
    left_join(
      cdm$concept_relationship %>%
        filter(relationship_id == "Maps to") %>%
        select(source_concept_id = concept_id_1, concept_relationship.target_concept_id = concept_id_2),
      by = "source_concept_id"
    ) %>%
    left_join(
      cdm$source_to_concept_map %>%
        filter(source_code == "Maps to") %>%
        select(source_code, source_to_concept_map.target_concept_id = target_concept_id),
      by=c("observation_source_value"= "source_code")
    ) %>%
    collect() %>%
    minCountFilter("n", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/observation_source_values.csv"))

  # Observations: nationality
  cdm$person %>%
    select(person_id) %>%
    left_join(
      cdm$observation %>%
        filter(observation_concept_id == 4087925) %>%  #Ethnicity concept_id
        select(person_id, nationality = value_as_string),
      by = "person_id"
    ) %>%
    group_by(nationality) %>%
    tally() %>%
    collect() %>%
    minCountFilter("n", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/obs_nationality.csv"))


  # measurements
  cdm$measurement %>%
    group_by(measurement_concept_id) %>%
    summarise(n_rec=n(), n_sub=n_distinct(person_id)) %>%
    left_join(
      cdm$concept %>%
        select(measurement_concept_id=concept_id, concept_name)
      ) %>%
    select(measurement_concept_id, concept_name, n_rec, n_sub) %>%
    collect() %>%
    minCountFilter(c("n_rec", "n_sub"), minimum_counts) %>% 
    write_csv(paste0(output_folder, "/measurements_concepts.csv"))


  #visits
  cdm$visit_occurrence %>%
    mutate(date_visit = year(visit_start_date)) %>%
    group_by(visit_concept_id, date_visit) %>%
    tally() %>%
    left_join(
      cdm$concept %>%
        select(visit_concept_id = concept_id, type_visit=concept_name),
      by="visit_concept_id"
    ) %>%
    collect() %>%
    minCountFilter("n", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/visits.csv"))

  #Drug exposure: quantity
  cdm$drug_exposure %>%
    mutate(
      quantity_val = ifelse(
        is.na(quantity),
        NA_character_,
        ifelse(quantity==0, "zero value", "non-zero value")
      )
    ) %>%
    group_by(drug_type_concept_id, quantity_val) %>%
    tally() %>%
    left_join(cdm$concept %>% select(drug_type_concept_id = concept_id, concept_name)) %>%
    select(drug_type_concept_id, concept_name, quantity_val, n) %>%
    collect() %>%
    minCountFilter("n", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/drug_quantity.csv"))

  #Drug exposure: days_supply
  cdm$drug_exposure %>%
    mutate(
      days_supply_val = ifelse(
        is.na(days_supply),
        NA_character_,
        ifelse(days_supply==0, "zero value", "non-zero value")
      )
    ) %>%
    group_by(drug_type_concept_id, days_supply_val) %>%
    tally() %>%
    left_join(cdm$concept %>% select(drug_type_concept_id = concept_id, concept_name)) %>%
    select(drug_type_concept_id, concept_name, days_supply_val, n) %>%
    collect() %>%
    minCountFilter("n", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/drug_days_supply.csv"))


  cdm$drug_exposure %>%
    distinct(drug_source_value, .keep_all = TRUE) %>%
    summarise(n = n_distinct(drug_source_value), n_id0= sum(as.integer(drug_concept_id == 0))) %>%
    collect() %>%
    write_csv(paste0(output_folder, "/drug_source_values.csv"))


  cdm$drug_exposure %>%
    filter(drug_concept_id == 0) %>%
    group_by(drug_source_value, drug_type_concept_id) %>%
    tally() %>%
    collect() %>%
    arrange(desc(n)) %>%
    minCountFilter("n", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/drug_source_values_id0_n.csv"))

  cdm$drug_exposure %>%
    filter(drug_concept_id != 0) %>%
    group_by(drug_source_value, drug_type_concept_id) %>%
    tally() %>%
    collect() %>%
    arrange(desc(n)) %>%
    group_by(drug_type_concept_id) %>%
    summarise(n_rec = sum(n)) %>%
    minCountFilter("n_rec", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/drugs_id_n.csv"))

  # Cancer sex -- check female/male
  cancer_concepts <- CDMConnector::readCohortSet(path = here::here("cancerSexCohorts"))

  cdm <- CDMConnector::generateCohortSet(
    cdm,
    cohortSet = cancer_concepts,
    name = "cancer_sex",
    overwrite = TRUE
  )

  cdm$cancer_sex %>%
    PatientProfiles::addSex() %>%
    group_by(cohort_definition_id, sex) %>%
    tally() %>%
    collect() %>%
    left_join(cdm$cancer_sex %>% omopgenerics::settings()) %>%
    select(cohort_name, sex, n ) %>%
    minCountFilter("n", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/cancer_sex.csv"))
  
  
  # Conditions ----
  if (! paste0(table_stem, "conditions_medications") %in% CDMConnector::listTables(db, write_schema)) {
    load(here::here("codeLists", "atc3rdLevel.RData"))
    load(here::here("codeLists", "icdSubChap.RData"))
    load(here::here("codeLists", "icdChap.RData"))
    
    chapters_approved <- c("\\[VI\\]", "\\[XV\\]", "\\[XVI\\]")
    subChapters_approved <- c("\\[I20-I25\\]", "\\[I26-I28\\]", "\\[I30-I52\\]", "\\[I60-I69\\]", 
                              "\\[I70-I79\\]", "\\[I80-I89\\]", "\\[F30-F38\\]", "\\[N17-N19\\]")
    ATC3rd_approved <- c("HYPNOTICS AND SEDATIVES", "ANTIDEPRESSANTS", "ANXIOLYTICS")
    
    code_list <- c(
      icdChap[grepl(paste(chapters_approved, collapse = "|"), names(icdChap))],
      icdSubChap[grepl(paste(subChapters_approved, collapse = "|"), names(icdSubChap))],
      atc3rdLevel[grepl(paste(ATC3rd_approved, collapse = "|"), names(atc3rdLevel))]
    )
    
    cdm <- CDMConnector::generateConceptCohortSet(
      cdm = cdm,
      name = "conditions_medications",
      conceptSet = code_list
    )
    
  } else {
    cdm <- cdmFromCon(
      con = db, 
      cdmSchema = cdm_schema, 
      writeSchema = c("schema" = write_schema, "prefix" = tolower(table_stem)),
      cohortTables = "conditions_medications"
    ) 
  }
  
  cdm$conditions_medications %>% 
    mutate(cohort_start_date = year(cohort_start_date)) %>%
    PatientProfiles::addCohortName() %>%
    select(cohort_name, cohort_start_date, subject_id) %>%
    group_by(cohort_name, cohort_start_date) %>%
    tally() %>% 
    collect() %>%
    minCountFilter("n", minimum_counts) %>% 
    write_csv(paste0(output_folder, "/conditions_medications.csv"))
  
  CDMConnector::cdmDisconnect(cdm)
  

}








